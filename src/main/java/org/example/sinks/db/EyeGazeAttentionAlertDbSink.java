
// File: Flink-CEP/src/main/java/org/example/sinks/db/EyeGazeAttentionAlertDbSink.java
package org.example.sinks.db;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import java.io.IOException;
import java.io.Serializable;
import java.sql.*;

// NEW Sink - Handles String JSON alerts from EyeGazeAttentionProcessor
public class EyeGazeAttentionAlertDbSink implements Sink<String> {
    private static final Logger logger = LoggerFactory.getLogger(EyeGazeAttentionAlertDbSink.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String tableName;

    public EyeGazeAttentionAlertDbSink(String jdbcUrl, String tableName, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
    }

    @Override
    public SinkWriter<String> createWriter(WriterInitContext context) throws IOException {
        return new EyeGazeAttentionAlertDbSinkWriter(jdbcUrl, tableName, username, password);
    }

     // Keep for potential backward compatibility
    @Override
    @Deprecated
    public SinkWriter<String> createWriter(InitContext context) throws IOException {
        logger.warn("EyeGazeAttentionAlertDbSink: Using deprecated createWriter(Sink.InitContext).");
        return new EyeGazeAttentionAlertDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    private static class EyeGazeAttentionAlertDbSinkWriter implements SinkWriter<String>, Serializable {
        private static final long serialVersionUID = 509L; // Unique ID

        private final String insertSql;
        private final String jdbcUrl;
        private final String username;
        private final String password;

        private transient Connection connection;
        private transient PreparedStatement statement;

        public EyeGazeAttentionAlertDbSinkWriter(String jdbcUrl, String tableName, String username, String password) throws IOException {
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
            // TODO: Define your table schema and corresponding INSERT SQL
            // Example assumes columns: time, thingid, alert_timestamp, feedback_type, severity, reason, details_json
            this.insertSql = "INSERT INTO " + tableName + " (time, thingid, alert_timestamp, feedback_type, severity, reason, details_json) VALUES (?, ?, ?, ?, ?, ?, ?::JSONB)";
            initializeJdbc();
        }

         private void initializeJdbc() throws IOException {
             try {
                this.connection = DriverManager.getConnection(jdbcUrl, username, password);
                this.statement = connection.prepareStatement(this.insertSql);
                logger.info("EyeGazeAlert Sink: Successfully connected/reconnected.");
            } catch (SQLException e) {
                logger.error("EyeGazeAlert Sink: Failed JDBC connection", e);
                throw new IOException("Failed JDBC connection", e);
            }
        }

        @Override
        public void write(String jsonRecord, Context context) throws IOException {
            if (jsonRecord == null || jsonRecord.isEmpty()) { return; }
             checkConnection();

            try {
                // 1. Parse JSON
                JsonObject alertJson = JsonParser.parseString(jsonRecord).getAsJsonObject();

                // 2. Extract fields (handle potential missing fields)
                String thingId = alertJson.has("thingId") ? alertJson.get("thingId").getAsString() : "unknown";
                Long alertTimestampMillis = alertJson.has("alertTimestamp") ? alertJson.get("alertTimestamp").getAsLong() : // Check duration alert field
                                            (alertJson.has("windowEndTimestamp") ? alertJson.get("windowEndTimestamp").getAsLong() : null); // Check average alert field
                String feedbackType = alertJson.has("feedbackType") ? alertJson.get("feedbackType").getAsString() : "unknown_gaze_alert";
                String severity = alertJson.has("severity") ? alertJson.get("severity").getAsString() : "UNKNOWN";
                String reason = alertJson.has("reason") ? alertJson.get("reason").getAsString() : "N/A";

                if (alertTimestampMillis == null) {
                    logger.warn("EyeGazeAlert Sink: Missing alert timestamp in JSON for {}. Skipping.", thingId);
                    return;
                }
                Timestamp alertTs = new Timestamp(alertTimestampMillis);

                // 3. Set Parameters
                statement.setTimestamp(1, alertTs); // time (hypertable)
                statement.setString(2, thingId);
                statement.setTimestamp(3, alertTs); // alert_timestamp
                statement.setString(4, feedbackType);
                statement.setString(5, severity);
                statement.setString(6, reason);
                statement.setString(7, jsonRecord); // Store full JSON in details_json

                // 4. Execute
                statement.executeUpdate();

            } catch (JsonSyntaxException e) {
                 logger.error("EyeGazeAlert Sink: Error parsing JSON: {}", jsonRecord, e);
            } catch (SQLException e) {
                logger.error("EyeGazeAlert Sink: Error inserting data: {}", jsonRecord, e);
            } catch (Exception e) {
                 logger.error("EyeGazeAlert Sink: Unexpected error writing: {}", jsonRecord, e);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException { /* No-op */ }

        @Override
        public void close() throws IOException {
            closeSilently();
            logger.info("EyeGazeAlert Sink: Database connection closed.");
        }

        // --- checkConnection and closeSilently helpers ---
         private void checkConnection() throws IOException { /* ... */
              if (connection == null) { initializeJdbc(); return; }
            try {
                if (!connection.isValid(1)) {
                    logger.warn("EyeGazeAlert Sink: JDBC connection is not valid. Reconnecting...");
                    closeSilently();
                    initializeJdbc();
                }
            } catch (SQLException e) {
                 logger.error("EyeGazeAlert Sink: Error checking/restoring connection.", e);
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
