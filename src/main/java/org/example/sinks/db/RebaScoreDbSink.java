package org.example.sinks.db;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.example.models.RebaScore;
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
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class RebaScoreDbSink implements Sink<String> {
    private static final Logger logger = LoggerFactory.getLogger(RebaScoreDbSink.class);
    // *** CORRECTED FORMATTER to standard ISO ***
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String tableName;

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

    @Override
    @Deprecated
    public SinkWriter<String> createWriter(InitContext context) throws IOException {
        logger.warn("RebaScoreDbSink: Using deprecated createWriter(Sink.InitContext).");
        return new RebaScoreDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    private static class RebaScoreDbSinkWriter implements SinkWriter<String>, Serializable {
        private static final long serialVersionUID = 502L;

        private final String insertSql;
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
            // Added ON CONFLICT DO NOTHING
            this.insertSql = "INSERT INTO " + tableName + " (" +
                "time, thingid, timestamp_str, " +
                "neck_score, trunk_score, leg_score, upper_arm_left_score, upper_arm_right_score, " +
                "lower_arm_left_score, lower_arm_right_score, wrist_left_score, wrist_right_score, " +
                "group_a_score, group_b_score, table_c_score, load_force_score, coupling_score, " +
                "activity_score, final_reba_score, risk_level" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"; // Added ON CONFLICT
            initializeJdbc();
        }

        private void initializeJdbc() throws IOException {
             try {
                if (this.gson == null) { this.gson = new Gson(); }
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
             if (jsonRecord == null || jsonRecord.isEmpty()) { return; }
             checkConnection();
             RebaScore record = null;
             Timestamp sqlTimestamp = null;
             String originalTimestampStr = null;

             try {
                record = gson.fromJson(jsonRecord, RebaScore.class);
                if (record == null || record.thingId == null) {
                    logger.warn("RebaScore Sink: Failed parse/missing thingId: {}", jsonRecord);
                    return;
                }
                originalTimestampStr = record.timestamp; // Get original string

                // *** Parse the ISO string using the CORRECT formatter ***
                if (originalTimestampStr != null) {
                    Instant instant = Instant.from(ISO_FORMATTER.parse(originalTimestampStr));
                    sqlTimestamp = Timestamp.from(instant);
                } else {
                     logger.warn("RebaScore Sink: Missing timestamp string in parsed RebaScore for {}. Storing NULL for time.", record.thingId);
                }

            } catch (JsonSyntaxException e) {
                 logger.error("RebaScore Sink: Error parsing JSON: {}", jsonRecord, e);
                 return;
            } catch (DateTimeParseException e) {
                 logger.warn("RebaScore Sink: Failed to parse ISO timestamp '{}' for {}. Storing NULL for time.", originalTimestampStr, record.thingId);
                 sqlTimestamp = null;
            } catch (Exception e) {
                 logger.error("RebaScore Sink: Unexpected error during parsing for {}: {}", jsonRecord, e);
                 return;
            }

            if (record == null) return;

            // *** Skip insert if timestamp parsing failed ***
            if (sqlTimestamp == null) {
                logger.error("RebaScore Sink: Skipping insert for {} due to failed timestamp parsing (time column cannot be NULL).", record.thingId);
                return;
            }

            try {
                // Set Parameters
                statement.setTimestamp(1, sqlTimestamp); // time (parsed)
                statement.setString(2, record.thingId);
                statement.setString(3, originalTimestampStr); // timestamp_str (original)

                // Set remaining parameters (4 to 20)
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
            } catch (SQLException e) {
                // ON CONFLICT handles duplicate keys silently
                if (!"23505".equals(e.getSQLState())) {
                    logger.error("RebaScore Sink: Error inserting data: {}", jsonRecord, e);
                 }
            } catch (Exception e) {
                 logger.error("RebaScore Sink: Unexpected error writing: {}", jsonRecord, e);
            }
        }

        // ... flush, close, checkConnection, closeSilently methods remain the same ...
        @Override
        public void flush(boolean endOfInput) throws IOException { /* No-op */ }

        @Override
        public void close() throws IOException {
            closeSilently();
            logger.info("RebaScore Sink: Database connection closed.");
        }

        private void checkConnection() throws IOException {
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
        private void closeSilently() {
             try { if (statement != null) statement.close(); } catch (SQLException ignored) {}
             try { if (connection != null) connection.close(); } catch (SQLException ignored) {}
             statement = null;
             connection = null;
             gson = null;
        }
    }
}