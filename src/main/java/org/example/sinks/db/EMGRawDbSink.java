
// File: Flink-CEP/src/main/java/org/example/sinks/db/EMGRawDbSink.java
package org.example.sinks.db; // Updated package

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.example.config.DBConfig;
import org.example.models.EMGReading; // Updated model import
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

// Renamed from EMGRawDatabaseSink
public class EMGRawDbSink implements Sink<EMGReading> { // Implement Serializable
    private static final Logger logger = LoggerFactory.getLogger(EMGRawDbSink.class);
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final String jdbcUrlBase;
    private final String dbName;
    private final String username;
    private final String password;

    public EMGRawDbSink(String jdbcUrlBase, String dbName, String username, String password) {
        this.jdbcUrlBase = jdbcUrlBase;
        this.dbName = dbName;
        this.username = username;
        this.password = password;
    }

    @Override
    public SinkWriter<EMGReading> createWriter(WriterInitContext context) throws IOException {
        String fullJdbcUrl = jdbcUrlBase + dbName;
        return new EMGRawDbSinkWriter(fullJdbcUrl, username, password);
    }

    // Keep for potential backward compatibility
    @Override
    @Deprecated
    public SinkWriter<EMGReading> createWriter(InitContext context) throws IOException {
        logger.warn("EMGRawDbSink: Using deprecated createWriter(Sink.InitContext).");
        String fullJdbcUrl = jdbcUrlBase + dbName;
        return new EMGRawDbSinkWriter(fullJdbcUrl, username, password);
    }

    private static class EMGRawDbSinkWriter implements SinkWriter<EMGReading>, Serializable {
        private static final long serialVersionUID = 504L; // Unique ID

        // SQL Statements (Keep as is)
        private static final String INSERT_SQL_01 = "INSERT INTO " + DBConfig.EMG_DB_TABLE_01 + " (time, thingid, timestamp_str, deltoids_left, triceps_left, biceps_left, wrist_extensors_left, wrist_flexor_left) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        private static final String INSERT_SQL_02 = "INSERT INTO " + DBConfig.EMG_DB_TABLE_02 + " (time, thingid, timestamp_str, deltoids_right, triceps_right, biceps_right, wrist_extensors_right, wrist_flexor_right) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        private static final String INSERT_SQL_03 = "INSERT INTO " + DBConfig.EMG_DB_TABLE_03 + " (time, thingid, timestamp_str, trapezius_left, trapezius_right, pectoralis_right, pectoralis_left, latissimus_left, latissimus_right) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        private final String jdbcUrl;
        private final String username;
        private final String password;

        private transient Connection connection;
        private transient PreparedStatement statement01;
        private transient PreparedStatement statement02;
        private transient PreparedStatement statement03;

        public EMGRawDbSinkWriter(String jdbcUrl, String username, String password) throws IOException {
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
            initializeJdbc();
        }

         private void initializeJdbc() throws IOException {
             try {
                this.connection = DriverManager.getConnection(jdbcUrl, username, password);
                this.statement01 = connection.prepareStatement(INSERT_SQL_01);
                this.statement02 = connection.prepareStatement(INSERT_SQL_02);
                this.statement03 = connection.prepareStatement(INSERT_SQL_03);
                logger.info("EMGRaw Sink: Successfully connected/reconnected to the database.");
            } catch (SQLException e) {
                logger.error("EMGRaw Sink: Failed to establish JDBC connection", e);
                throw new IOException("Failed to establish JDBC connection", e);
            }
        }

        @Override
        public void write(EMGReading record, Context context) throws IOException {
            // (Keep the existing write logic from EMGRawDatabaseSinkWriter)
            if (record == null || record.getThingid() == null || record.getTimestamp() == null) { return; }
             checkConnection();
            PreparedStatement targetStatement = null; int tableTarget = 0;
            boolean hasLeftArmData = record.getDeltoids_left() != 0 || record.getTriceps_left() != 0 || record.getBiceps_left() != 0 || record.getWrist_extensors_left() != 0 || record.getWrist_flexor_left() != 0;
            boolean hasRightArmData = record.getDeltoids_right() != 0 || record.getTriceps_right() != 0 || record.getBiceps_right() != 0 || record.getWrist_extensors_right() != 0 || record.getWrist_flexor_right() != 0;
            boolean hasTrunkData = record.getTrapezius_left() != 0 || record.getTrapezius_right() != 0 || record.getPectoralis_left() != 0 || record.getPectoralis_right() != 0 || record.getLatissimus_left() != 0 || record.getLatissimus_right() != 0;

            if (hasLeftArmData && !hasRightArmData && !hasTrunkData) { targetStatement = statement01; tableTarget = 1;
            } else if (hasRightArmData && !hasLeftArmData && !hasTrunkData) { targetStatement = statement02; tableTarget = 2;
            } else if (hasTrunkData && !hasLeftArmData && !hasRightArmData) { targetStatement = statement03; tableTarget = 3;
            } else { logger.warn("EMGRaw Sink: Ambiguous target table for {}. Skipping.", record.getThingid()); return; }

            if (targetStatement == null) { logger.error("EMGRaw Sink: Target statement null for {}. Skipping.", record.getThingid()); return; }

            try {
                Timestamp sqlTimestamp = null;
                try {
                     LocalDateTime ldt = LocalDateTime.parse(record.getTimestamp(), TIMESTAMP_FORMATTER);
                     sqlTimestamp = Timestamp.from(ldt.toInstant(ZoneOffset.UTC));
                } catch (DateTimeParseException | NullPointerException e) {
                    logger.warn("EMGRaw Sink: Failed parse timestamp '{}' for {}. Storing NULL.", record.getTimestamp(), record.getThingid());
                }
                 if (sqlTimestamp != null) targetStatement.setTimestamp(1, sqlTimestamp); else targetStatement.setNull(1, Types.TIMESTAMP_WITH_TIMEZONE);
                targetStatement.setString(2, record.getThingid());
                targetStatement.setString(3, record.getTimestamp());

                switch (tableTarget) {
                    case 1: // Left Arm
                        targetStatement.setDouble(4, record.getDeltoids_left()); targetStatement.setDouble(5, record.getTriceps_left()); targetStatement.setDouble(6, record.getBiceps_left()); targetStatement.setDouble(7, record.getWrist_extensors_left()); targetStatement.setDouble(8, record.getWrist_flexor_left()); break;
                    case 2: // Right Arm
                        targetStatement.setDouble(4, record.getDeltoids_right()); targetStatement.setDouble(5, record.getTriceps_right()); targetStatement.setDouble(6, record.getBiceps_right()); targetStatement.setDouble(7, record.getWrist_extensors_right()); targetStatement.setDouble(8, record.getWrist_flexor_right()); break;
                    case 3: // Trunk
                        targetStatement.setDouble(4, record.getTrapezius_left()); targetStatement.setDouble(5, record.getTrapezius_right()); targetStatement.setDouble(6, record.getPectoralis_right()); targetStatement.setDouble(7, record.getPectoralis_left()); targetStatement.setDouble(8, record.getLatissimus_left()); targetStatement.setDouble(9, record.getLatissimus_right()); break;
                }
                targetStatement.executeUpdate();
            } catch (SQLException e) { logger.error("EMGRaw Sink: Error inserting raw EMG into table {} for {}: {}", tableTarget, record.getThingid(), e.getMessage());
            } catch (Exception e) { logger.error("EMGRaw Sink: Unexpected error writing raw EMG record {} for {}: {}", tableTarget, record.getThingid(), e.getMessage()); }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException {
            // No-op
        }

        @Override
        public void close() throws IOException {
            // (Keep the existing close logic from EMGRawDatabaseSinkWriter)
            closeSilently();
            logger.info("EMGRaw Sink: Database connection closed.");
        }

         // --- checkConnection and closeSilently helpers (same as in MoCapRawDbSink) ---
         private void checkConnection() throws IOException { /* ... */
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
         private void closeSilently() { /* ... */
             try { if (statement01 != null) statement01.close(); } catch (SQLException ignored) {}
             try { if (statement02 != null) statement02.close(); } catch (SQLException ignored) {}
             try { if (statement03 != null) statement03.close(); } catch (SQLException ignored) {}
             try { if (connection != null) connection.close(); } catch (SQLException ignored) {}
             statement01 = null; statement02 = null; statement03 = null;
             connection = null;
         }
    }
}
