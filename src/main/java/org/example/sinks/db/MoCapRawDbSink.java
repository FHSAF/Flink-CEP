// File: Flink-CEP/src/main/java/org/example/sinks/db/MoCapRawDbSink.java
package org.example.sinks.db; // Updated package

import java.sql.Timestamp;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.example.models.MoCapReading; // Updated model import
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable; // Import Serializable
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

// Renamed from MoCapRawDatabaseSink
public class MoCapRawDbSink implements Sink<MoCapReading> { // Implement Serializable
    private static final Logger logger = LoggerFactory.getLogger(MoCapRawDbSink.class);
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String tableName; // Make table name configurable

    public MoCapRawDbSink(String jdbcUrl, String tableName, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
    }

    @Override
    public SinkWriter<MoCapReading> createWriter(WriterInitContext context) throws IOException {
        return new MoCapRawDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    // Keep for potential backward compatibility
    @Override
    @Deprecated
    public SinkWriter<MoCapReading> createWriter(InitContext context) throws IOException {
        logger.warn("MoCapRawDbSink: Using deprecated createWriter(Sink.InitContext).");
        return new MoCapRawDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    private static class MoCapRawDbSinkWriter implements SinkWriter<MoCapReading>, Serializable {
        private static final long serialVersionUID = 501L; // Unique ID

        // Dynamic SQL based on table name
        private final String insertSql;

        private final String jdbcUrl;
        private final String username;
        private final String password;

        private transient Connection connection;
        private transient PreparedStatement statement;

        public MoCapRawDbSinkWriter(String jdbcUrl, String tableName, String username, String password) throws IOException {
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
            // Construct SQL dynamically - CAUTION: Ensure tableName is safe if user-provided elsewhere
            this.insertSql = "INSERT INTO " + tableName + " (" +
                "time, thingid, timestamp_str, " + // Added time and timestamp_str
                "elbow_flex_ext_left, elbow_flex_ext_right, " +
                "shoulder_flex_ext_left, shoulder_flex_ext_right, " +
                "shoulder_abd_add_left, shoulder_abd_add_right, " +
                "lowerarm_pron_sup_left, lowerarm_pron_sup_right, " +
                "upperarm_rotation_left, upperarm_rotation_right, " +
                "hand_flex_ext_left, hand_flex_ext_right, " +
                "hand_radial_ulnar_left, hand_radial_ulnar_right, " +
                "neck_flex_ext, neck_torsion, head_tilt, torso_tilt, " +
                "torso_side_tilt, back_curve, back_torsion, " +
                "knee_flex_ext_left, knee_flex_ext_right " +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"; // 25 placeholders
            initializeJdbc();
        }

        private void initializeJdbc() throws IOException {
             try {
                this.connection = DriverManager.getConnection(jdbcUrl, username, password);
                this.statement = connection.prepareStatement(this.insertSql);
                logger.info("MoCapRaw Sink: Successfully connected/reconnected to the database.");
            } catch (SQLException e) {
                logger.error("MoCapRaw Sink: Failed to establish JDBC connection", e);
                throw new IOException("Failed to establish JDBC connection", e);
            }
        }

        @Override
        public void write(MoCapReading record, Context context) throws IOException {
             if (record == null || record.getThingid() == null || record.getTimestamp() == null) {
                logger.warn("MoCapRaw Sink: Received null or incomplete record. Skipping.");
                return;
            }
             checkConnection(); // Ensure connection is valid

            try {
                // 1. Parse Timestamp
                Timestamp sqlTimestamp = null;
                try {
                     LocalDateTime ldt = LocalDateTime.parse(record.getTimestamp(), TIMESTAMP_FORMATTER);
                     sqlTimestamp = Timestamp.from(ldt.toInstant(ZoneOffset.UTC)); // Assuming UTC
                } catch (DateTimeParseException | NullPointerException e) {
                    logger.warn("MoCapRaw Sink: Failed to parse timestamp string '{}' for {}. Storing NULL for time.", record.getTimestamp(), record.getThingid());
                }

                // 2. Set Parameters
                if (sqlTimestamp != null) {
                    statement.setTimestamp(1, sqlTimestamp); // time (hypertable column)
                 } else {
                     statement.setNull(1, Types.TIMESTAMP_WITH_TIMEZONE);
                     logger.error("MoCapRaw Sink: Inserting NULL for hypertable time column for {}. This might fail.", record.getThingid());
                 }
                statement.setString(2, record.getThingid());
                statement.setString(3, record.getTimestamp()); // timestamp_str

                statement.setDouble(4, record.getElbowFlexExtLeft());
                statement.setDouble(5, record.getElbowFlexExtRight());
                statement.setDouble(6, record.getShoulderFlexExtLeft());
                statement.setDouble(7, record.getShoulderFlexExtRight());
                statement.setDouble(8, record.getShoulderAbdAddLeft());
                statement.setDouble(9, record.getShoulderAbdAddRight());
                statement.setDouble(10, record.getLowerarmPronSupLeft());
                statement.setDouble(11, record.getLowerarmPronSupRight());
                statement.setDouble(12, record.getUpperarmRotationLeft());
                statement.setDouble(13, record.getUpperarmRotationRight());
                statement.setDouble(14, record.getHandFlexExtLeft());
                statement.setDouble(15, record.getHandFlexExtRight());
                statement.setDouble(16, record.getHandRadialUlnarLeft());
                statement.setDouble(17, record.getHandRadialUlnarRight());
                statement.setDouble(18, record.getNeckFlexExt());
                statement.setDouble(19, record.getNeckTorsion());
                statement.setDouble(20, record.getHeadTilt());
                statement.setDouble(21, record.getTorsoTilt());
                statement.setDouble(22, record.getTorsoSideTilt());
                statement.setDouble(23, record.getBackCurve());
                statement.setDouble(24, record.getBackTorsion());
                statement.setDouble(25, record.getKneeFlexExtLeft());
                statement.setDouble(26, record.getKneeFlexExtRight()); // Index corrected to 26

                // 3. Execute Update
                statement.executeUpdate();
                // logger.debug("MoCapRaw Sink: Inserted raw MoCap record for {}", record.getThingid());

            } catch (SQLException e) {
                logger.error("MoCapRaw Sink: Error inserting raw MoCap data for {}: {}", record.getThingid(), e.getMessage());
            } catch (Exception e) {
                logger.error("MoCapRaw Sink: Unexpected error writing raw MoCap record for {}: {}", record.getThingid(), e.getMessage());
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException {
            // No-op
        }

        @Override
        public void close() throws IOException {
            closeSilently();
            logger.info("MoCapRaw Sink: Database connection closed.");
        }

        // --- checkConnection and closeSilently helpers (same as in EMGRawDbSink) ---
         private void checkConnection() throws IOException { /* ... */
              if (connection == null) { initializeJdbc(); return; }
            try {
                if (!connection.isValid(1)) {
                    logger.warn("MoCapRaw Sink: JDBC connection is not valid. Reconnecting...");
                    closeSilently();
                    initializeJdbc();
                }
            } catch (SQLException e) {
                 logger.error("MoCapRaw Sink: Error checking/restoring connection.", e);
                 closeSilently();
                 initializeJdbc();
            }
         }
         private void closeSilently() { /* ... */
              try { if (statement != null) statement.close(); } catch (SQLException ignored) {}
             try { if (connection != null) connection.close(); } catch (SQLException ignored) {}
             statement = null;
             connection = null;
         }
    }
}