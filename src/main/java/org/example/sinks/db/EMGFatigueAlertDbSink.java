
// File: Flink-CEP/src/main/java/org/example/sinks/db/EMGFatigueAlertDbSink.java
package org.example.sinks.db; // Updated package

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
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

// Renamed from EMGDatabaseSink
public class EMGFatigueAlertDbSink implements Sink<String> { // Implement Serializable
    private static final Logger logger = LoggerFactory.getLogger(EMGFatigueAlertDbSink.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String tableName; // Make table name configurable

    public EMGFatigueAlertDbSink(String jdbcUrl, String tableName, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
    }

    @Override
    public SinkWriter<String> createWriter(WriterInitContext context) throws IOException {
        return new EMGFatigueAlertDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    // Keep for potential backward compatibility
    @Override
    @Deprecated
    public SinkWriter<String> createWriter(InitContext context) throws IOException {
        logger.warn("EMGFatigueAlertDbSink: Using deprecated createWriter(Sink.InitContext).");
        return new EMGFatigueAlertDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    private static class EMGFatigueAlertDbSinkWriter implements SinkWriter<String>, Serializable {
        private static final long serialVersionUID = 505L; // Unique ID

        private final String insertSql; // SQL constructed dynamically
        private final String jdbcUrl;
        private final String username;
        private final String password;

        private transient Connection connection;
        private transient PreparedStatement statement;

        public EMGFatigueAlertDbSinkWriter(String jdbcUrl, String tableName, String username, String password) throws IOException {
             this.jdbcUrl = jdbcUrl;
             this.username = username;
             this.password = password;
             // Construct SQL dynamically
             this.insertSql = "INSERT INTO " + tableName + " (" +
                "time, thingid, alert_timestamp, muscle, severity, reason, " +
                "average_rms, check_window_seconds, rms_threshold, full_alert_json" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::JSONB)"; // Assuming JSONB
            initializeJdbc();
        }

         private void initializeJdbc() throws IOException {
             try {
                this.connection = DriverManager.getConnection(jdbcUrl, username, password);
                this.statement = connection.prepareStatement(this.insertSql);
                logger.info("EMGFatigueAlert Sink: Successfully connected/reconnected.");
            } catch (SQLException e) {
                logger.error("EMGFatigueAlert Sink: Failed JDBC connection", e);
                throw new IOException("Failed JDBC connection", e);
            }
        }

        @Override
        public void write(String jsonRecord, Context context) throws IOException {
            // (Keep the existing write logic from EMGDatabaseSinkWriter)
            if (jsonRecord == null || jsonRecord.isEmpty()) { return; }
             checkConnection();
            try {
                JsonObject alertJson = JsonParser.parseString(jsonRecord).getAsJsonObject();
                String thingId = alertJson.has("thingId") ? alertJson.get("thingId").getAsString() : "unknown";
                Long alertTimestampMillis = alertJson.has("timestamp") ? alertJson.get("timestamp").getAsLong() : null;
                String muscle = alertJson.has("muscle") ? alertJson.get("muscle").getAsString() : "unknown";
                String severity = alertJson.has("severity") ? alertJson.get("severity").getAsString() : "UNKNOWN";
                String reason = alertJson.has("reason") ? alertJson.get("reason").getAsString() : "N/A";
                Double averageRms = alertJson.has("averageRMS") && !alertJson.get("averageRMS").isJsonNull() ? alertJson.get("averageRMS").getAsDouble() : null;
                Integer checkWindow = alertJson.has("checkWindowSeconds") ? alertJson.get("checkWindowSeconds").getAsInt() : null;
                Double rmsThreshold = alertJson.has("rmsThreshold") && !alertJson.get("rmsThreshold").isJsonNull() ? alertJson.get("rmsThreshold").getAsDouble() : null;

                if (alertTimestampMillis == null) { logger.warn("EMGFatigueAlert Sink: Missing timestamp in JSON for {}. Skipping.", thingId); return; }
                Timestamp alertTs = new Timestamp(alertTimestampMillis);

                statement.setTimestamp(1, alertTs); // time
                statement.setString(2, thingId);
                statement.setTimestamp(3, alertTs); // alert_timestamp
                statement.setString(4, muscle);
                statement.setString(5, severity);
                statement.setString(6, reason);
                if (averageRms != null) statement.setDouble(7, averageRms); else statement.setNull(7, Types.DOUBLE);
                if (checkWindow != null) statement.setInt(8, checkWindow); else statement.setNull(8, Types.INTEGER);
                if (rmsThreshold != null) statement.setDouble(9, rmsThreshold); else statement.setNull(9, Types.DOUBLE);
                statement.setString(10, jsonRecord); // full_alert_json
                statement.executeUpdate();
            } catch (JsonSyntaxException e) { logger.error("EMGFatigueAlert Sink: Error parsing JSON: {}", jsonRecord, e);
            } catch (SQLException e) { logger.error("EMGFatigueAlert Sink: Error inserting data: {}", jsonRecord, e);
            } catch (Exception e) { logger.error("EMGFatigueAlert Sink: Unexpected error writing: {}", jsonRecord, e); }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException {
            // No-op
        }

        @Override
        public void close() throws IOException {
            // (Keep the existing close logic from EMGDatabaseSinkWriter)
            closeSilently();
            logger.info("EMGFatigueAlert Sink: Database connection closed.");
        }

         // --- checkConnection and closeSilently helpers (same as in MoCapRawDbSink) ---
         private void checkConnection() throws IOException { /* ... */
              if (connection == null) { initializeJdbc(); return; }
            try {
                if (!connection.isValid(1)) {
                    logger.warn("EMGFatigueAlert Sink: JDBC connection is not valid. Reconnecting...");
                    closeSilently();
                    initializeJdbc();
                }
            } catch (SQLException e) {
                 logger.error("EMGFatigueAlert Sink: Error checking/restoring connection.", e);
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
