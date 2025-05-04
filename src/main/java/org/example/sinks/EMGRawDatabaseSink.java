package org.example.sinks;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.example.config.DBConfig; // Import DB configuration
import org.example.models.EMGSensorReading; // Import your POJO
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Flink Sink to write raw EMGSensorReading data into appropriate
 * TimescaleDB tables (emg_raw_data_01, 02, or 03) based on
 * likely source inferred from populated fields.
 */
public class EMGRawDatabaseSink implements Sink<EMGSensorReading> {
    private static final Logger logger = LoggerFactory.getLogger(EMGRawDatabaseSink.class);

    private final String jdbcUrlBase;
    private final String dbName;
    private final String username;
    private final String password;

    public EMGRawDatabaseSink(String jdbcUrlBase, String dbName, String username, String password) {
        this.jdbcUrlBase = jdbcUrlBase;
        this.dbName = dbName; // e.g., DBConfig.EMG_DB_NAME
        this.username = username;
        this.password = password;
    }

    @Override
    public SinkWriter<EMGSensorReading> createWriter(WriterInitContext context) throws IOException {
        String fullJdbcUrl = jdbcUrlBase + dbName;
        return new EMGRawDatabaseSinkWriter(fullJdbcUrl, username, password);
    }

    // Keep for potential backward compatibility if needed
    @Override
    @Deprecated
    public SinkWriter<EMGSensorReading> createWriter(InitContext context) throws IOException {
        logger.warn("EMGRawDatabaseSink: Using deprecated createWriter(Sink.InitContext).");
        String fullJdbcUrl = jdbcUrlBase + dbName;
        return new EMGRawDatabaseSinkWriter(fullJdbcUrl, username, password);
    }

    // --- Inner Writer Class ---
    private static class EMGRawDatabaseSinkWriter implements SinkWriter<EMGSensorReading>, Serializable {
        private static final long serialVersionUID = 1L;

        // --- SQL Statements for each table ---
        // Table 01 (Left Arm)
        private static final String INSERT_SQL_01 =
            "INSERT INTO " + DBConfig.EMG_DB_TABLE_01 + " (" +
            "time, thingid, timestamp_str, " +
            "deltoids_left, triceps_left, biceps_left, " +
            "wrist_extensors_left, wrist_flexor_left" +
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        // Table 02 (Right Arm)
        private static final String INSERT_SQL_02 =
            "INSERT INTO " + DBConfig.EMG_DB_TABLE_02 + " (" +
            "time, thingid, timestamp_str, " +
            "deltoids_right, triceps_right, biceps_right, " +
            "wrist_extensors_right, wrist_flexor_right" +
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        // Table 03 (Trunk/Chest)
        private static final String INSERT_SQL_03 =
             "INSERT INTO " + DBConfig.EMG_DB_TABLE_03 + " (" +
             "time, thingid, timestamp_str, " +
             "trapezius_left, trapezius_right, pectoralis_right, " +
             "pectoralis_left, latissimus_left, latissimus_right" +
             ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        private final String jdbcUrl;
        private final String username;
        private final String password;

        private transient Connection connection;
        private transient PreparedStatement statement01; // Statement for table 01
        private transient PreparedStatement statement02; // Statement for table 02
        private transient PreparedStatement statement03; // Statement for table 03

        public EMGRawDatabaseSinkWriter(String jdbcUrl, String username, String password) throws IOException {
             this.jdbcUrl = jdbcUrl;
             this.username = username;
             this.password = password;
             initializeJdbc(); // Initialize connection and statements
        }

        // Initialize JDBC connection and prepare all statements
        private void initializeJdbc() throws IOException {
             try {
                this.connection = DriverManager.getConnection(jdbcUrl, username, password);
                this.statement01 = connection.prepareStatement(INSERT_SQL_01);
                this.statement02 = connection.prepareStatement(INSERT_SQL_02);
                this.statement03 = connection.prepareStatement(INSERT_SQL_03);
                logger.info("EMGRaw Sink: Successfully connected/reconnected to the database and prepared statements.");
            } catch (SQLException e) {
                logger.error("EMGRaw Sink: Failed to establish JDBC connection or prepare statements", e);
                throw new IOException("Failed to establish JDBC connection", e);
            }
        }

        @Override
        public void write(EMGSensorReading record, Context context) throws IOException {
            if (record == null || record.getThingid() == null || record.getTimestamp() == null) {
                logger.warn("EMGRaw Sink: Received null or incomplete record. Skipping.");
                return;
            }
             checkConnection(); // Ensure connection is valid

            // --- Infer Target Table and Prepare Statement ---
            PreparedStatement targetStatement = null;
            int tableTarget = 0; // 1, 2, or 3

            // Inference logic: Check which group of fields seems populated.
            // This assumes non-relevant fields might be 0.0 or default.
            // A more robust approach might involve adding a 'source_topic' field earlier.
            boolean hasLeftArmData = record.getDeltoids_left() != 0 || record.getTriceps_left() != 0 || record.getBiceps_left() != 0 || record.getWrist_extensors_left() != 0 || record.getWrist_flexor_left() != 0;
            boolean hasRightArmData = record.getDeltoids_right() != 0 || record.getTriceps_right() != 0 || record.getBiceps_right() != 0 || record.getWrist_extensors_right() != 0 || record.getWrist_flexor_right() != 0;
            boolean hasTrunkData = record.getTrapezius_left() != 0 || record.getTrapezius_right() != 0 || record.getPectoralis_left() != 0 || record.getPectoralis_right() != 0 || record.getLatissimus_left() != 0 || record.getLatissimus_right() != 0;

            // Prioritize based on which group has data. Handle potential overlaps if necessary.
            if (hasLeftArmData && !hasRightArmData && !hasTrunkData) {
                targetStatement = statement01;
                tableTarget = 1;
            } else if (hasRightArmData && !hasLeftArmData && !hasTrunkData) {
                targetStatement = statement02;
                tableTarget = 2;
            } else if (hasTrunkData && !hasLeftArmData && !hasRightArmData) {
                targetStatement = statement03;
                tableTarget = 3;
            } else {
                // Ambiguous or empty record? Log and potentially skip or default.
                logger.warn("EMGRaw Sink: Could not clearly determine target table for record {}. Skipping.", record.getThingid());
                // Or maybe try inserting into a default table or logging table?
                return;
            }

            if (targetStatement == null) { // Should not happen if logic above is sound
                 logger.error("EMGRaw Sink: Target statement is null after inference for record {}. Skipping.", record.getThingid());
                 return;
            }

            // --- Set Parameters and Execute ---
            try {
                // 1. Parse Timestamp
                Timestamp sqlTimestamp = null;
                try {
                     LocalDateTime ldt = LocalDateTime.parse(record.getTimestamp(), TIMESTAMP_FORMATTER);
                     sqlTimestamp = Timestamp.from(ldt.toInstant(ZoneOffset.UTC)); // Assuming UTC
                } catch (DateTimeParseException | NullPointerException e) {
                    logger.warn("EMGRaw Sink: Failed to parse timestamp string '{}' for {}. Storing NULL for time.", record.getTimestamp(), record.getThingid());
                    // Insert NULL for the primary time column - check if your hypertable allows this!
                }

                // 2. Set Common Parameters
                 if (sqlTimestamp != null) {
                    targetStatement.setTimestamp(1, sqlTimestamp); // time (hypertable column)
                 } else {
                     // Setting the hypertable time column to NULL might fail depending on table definition
                     targetStatement.setNull(1, Types.TIMESTAMP_WITH_TIMEZONE);
                     logger.error("EMGRaw Sink: Inserting NULL for hypertable time column for {}. This might fail.", record.getThingid());
                 }
                targetStatement.setString(2, record.getThingid());
                targetStatement.setString(3, record.getTimestamp()); // timestamp_str

                // 3. Set Table-Specific Parameters
                switch (tableTarget) {
                    case 1: // Left Arm
                        targetStatement.setDouble(4, record.getDeltoids_left());
                        targetStatement.setDouble(5, record.getTriceps_left());
                        targetStatement.setDouble(6, record.getBiceps_left());
                        targetStatement.setDouble(7, record.getWrist_extensors_left());
                        targetStatement.setDouble(8, record.getWrist_flexor_left());
                        break;
                    case 2: // Right Arm
                        targetStatement.setDouble(4, record.getDeltoids_right());
                        targetStatement.setDouble(5, record.getTriceps_right());
                        targetStatement.setDouble(6, record.getBiceps_right());
                        targetStatement.setDouble(7, record.getWrist_extensors_right());
                        targetStatement.setDouble(8, record.getWrist_flexor_right());
                        break;
                    case 3: // Trunk
                        targetStatement.setDouble(4, record.getTrapezius_left());
                        targetStatement.setDouble(5, record.getTrapezius_right());
                        targetStatement.setDouble(6, record.getPectoralis_right());
                        targetStatement.setDouble(7, record.getPectoralis_left());
                        targetStatement.setDouble(8, record.getLatissimus_left());
                        targetStatement.setDouble(9, record.getLatissimus_right());
                        break;
                }

                // 4. Execute Update
                targetStatement.executeUpdate();
                // logger.debug("EMGRaw Sink: Inserted raw EMG record into table {} for {}", tableTarget, record.getThingid());

            } catch (SQLException e) {
                logger.error("EMGRaw Sink: Error inserting raw EMG data into table {} for {}: {}", tableTarget, record.getThingid(), e.getMessage());
                // Consider re-throwing IOException if needed
            } catch (Exception e) {
                logger.error("EMGRaw Sink: Unexpected error writing raw EMG record {} for {}: {}", tableTarget, record.getThingid(), e.getMessage());
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException {
            // No-op for non-batching writes
        }

        @Override
        public void close() throws IOException {
            closeSilently(); // Use helper
            logger.info("EMGRaw Sink: Database connection closed.");
        }

        // Helper to check connection and potentially reconnect
        private void checkConnection() throws IOException {
             if (connection == null) { initializeJdbc(); return; }
            try {
                if (!connection.isValid(1)) {
                    logger.warn("EMGRaw Sink: JDBC connection is not valid. Reconnecting...");
                    closeSilently();
                    initializeJdbc();
                }
            } catch (SQLException e) {
                 logger.error("EMGRaw Sink: Error checking/restoring connection.", e);
                 closeSilently();
                 initializeJdbc();
            }
         }

         // Helper to close resources quietly
         private void closeSilently() {
             try { if (statement01 != null) statement01.close(); } catch (SQLException ignored) {}
             try { if (statement02 != null) statement02.close(); } catch (SQLException ignored) {}
             try { if (statement03 != null) statement03.close(); } catch (SQLException ignored) {}
             try { if (connection != null) connection.close(); } catch (SQLException ignored) {}
             statement01 = null; statement02 = null; statement03 = null;
             connection = null;
         }
    }
}