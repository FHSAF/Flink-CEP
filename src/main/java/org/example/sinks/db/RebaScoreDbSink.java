// File: Flink-CEP/src/main/java/org/example/sinks/db/RebaScoreDbSink.java
package org.example.sinks.db; // Updated package

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.example.models.RebaScore; // Correct model import
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable; // Import Serializable
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

// Renamed from MoCapRebaScoreDatabaseSink
public class RebaScoreDbSink implements Sink<String> { // Implement Serializable
    private static final Logger logger = LoggerFactory.getLogger(RebaScoreDbSink.class);
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String tableName; // Make table name configurable

    public RebaScoreDbSink(String jdbcUrl, String tableName, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
    }

    @Override
    public SinkWriter<String> createWriter(WriterInitContext context) throws IOException {
        return new RebaScoreDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    // Keep for potential backward compatibility
    @Override
    @Deprecated
    public SinkWriter<String> createWriter(InitContext context) throws IOException {
        logger.warn("RebaScoreDbSink: Using deprecated createWriter(Sink.InitContext).");
        return new RebaScoreDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    private static class RebaScoreDbSinkWriter implements SinkWriter<String>, Serializable {
        private static final long serialVersionUID = 502L; // Unique ID

        private final String insertSql; // SQL constructed dynamically
        private final String jdbcUrl;
        private final String username;
        private final String password;

        private transient Connection connection;
        private transient PreparedStatement statement;
        private transient Gson gson;

        public RebaScoreDbSinkWriter(String jdbcUrl, String tableName, String username, String password) throws IOException {
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
            this.gson = new Gson();
            // Construct SQL dynamically
            this.insertSql = "INSERT INTO " + tableName + " (" +
                "time, thingid, timestamp_str, " +
                "neck_score, trunk_score, leg_score, " +
                "upper_arm_left_score, upper_arm_right_score, " +
                "lower_arm_left_score, lower_arm_right_score, " +
                "wrist_left_score, wrist_right_score, " +
                "group_a_score, group_b_score, table_c_score, " +
                "load_force_score, coupling_score, activity_score, " +
                "final_reba_score, risk_level" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"; // 20 placeholders
            initializeJdbc();
        }

         private void initializeJdbc() throws IOException {
             try {
                if (this.gson == null) { this.gson = new Gson(); } // Ensure Gson is initialized on recovery
                this.connection = DriverManager.getConnection(jdbcUrl, username, password);
                this.statement = connection.prepareStatement(this.insertSql);
                logger.info("RebaScore Sink: Successfully connected/reconnected to the database.");
            } catch (SQLException e) {
                logger.error("RebaScore Sink: Failed to establish JDBC connection", e);
                throw new IOException("Failed to establish JDBC connection", e);
            }
        }

        @Override
        public void write(String jsonRecord, Context context) throws IOException {
            // (Keep the existing write logic from MoCapRebaScoreDatabaseSinkWriter)
             if (jsonRecord == null || jsonRecord.isEmpty()) { return; }
             checkConnection();
             try {
                RebaScore record = gson.fromJson(jsonRecord, RebaScore.class);
                if (record == null || record.thingId == null) { logger.warn("RebaScore Sink: Failed parse/missing thingId: {}", jsonRecord); return; }
                Timestamp sqlTimestamp = null;
                try {
                     LocalDateTime ldt = LocalDateTime.parse(record.timestamp, TIMESTAMP_FORMATTER);
                     sqlTimestamp = Timestamp.from(ldt.toInstant(ZoneOffset.UTC));
                } catch (DateTimeParseException | NullPointerException e) {
                    logger.warn("RebaScore Sink: Failed parse timestamp '{}' for {}. Storing NULL.", record.timestamp, record.thingId);
                }
                if (sqlTimestamp != null) statement.setTimestamp(1, sqlTimestamp); else statement.setNull(1, Types.TIMESTAMP_WITH_TIMEZONE);
                statement.setString(2, record.thingId);
                statement.setString(3, record.timestamp);
                statement.setInt(4, record.neckScore);
                statement.setInt(5, record.trunkScore);
                statement.setInt(6, record.legScore);
                statement.setInt(7, record.upperArmLeftScore);
                statement.setInt(8, record.upperArmRightScore);
                statement.setInt(9, record.lowerArmLeftScore);
                statement.setInt(10, record.lowerArmRightScore);
                statement.setInt(11, record.wristLeftScore);
                statement.setInt(12, record.wristRightScore);
                statement.setInt(13, record.groupAScore);
                statement.setInt(14, record.groupBScore);
                statement.setInt(15, record.tableCScore);
                statement.setInt(16, record.loadForceScore);
                statement.setInt(17, record.couplingScore);
                statement.setInt(18, record.activityScore);
                statement.setInt(19, record.finalRebaScore);
                statement.setString(20, record.riskLevel);
                statement.executeUpdate();
            } catch (JsonSyntaxException e) { logger.error("RebaScore Sink: Error parsing JSON: {}", jsonRecord, e);
            } catch (SQLException e) { logger.error("RebaScore Sink: Error inserting data: {}", jsonRecord, e);
            } catch (Exception e) { logger.error("RebaScore Sink: Unexpected error writing: {}", jsonRecord, e); }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException {
            // No-op
        }

        @Override
        public void close() throws IOException {
            // (Keep the existing close logic from MoCapRebaScoreDatabaseSinkWriter)
            closeSilently();
            logger.info("RebaScore Sink: Database connection closed.");
        }

         // --- checkConnection and closeSilently helpers (same as in MoCapRawDbSink) ---
         private void checkConnection() throws IOException { /* ... */
              if (connection == null) { initializeJdbc(); return; }
            try {
                if (!connection.isValid(1)) {
                    logger.warn("RebaScore Sink: JDBC connection is not valid. Reconnecting...");
                    closeSilently();
                    initializeJdbc();
                }
            } catch (SQLException e) {
                 logger.error("RebaScore Sink: Error checking/restoring connection.", e);
                 closeSilently();
                 initializeJdbc();
            }
         }
         private void closeSilently() { /* ... */
             try { if (statement != null) statement.close(); } catch (SQLException ignored) {}
             try { if (connection != null) connection.close(); } catch (SQLException ignored) {}
             statement = null;
             connection = null;
             gson = null; // Ensure Gson is re-initialized if needed
         }
    }
}
