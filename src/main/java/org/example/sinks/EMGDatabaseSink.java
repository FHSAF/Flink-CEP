package org.example.sinks;

// Keep Gson imports needed for JsonParser and JsonObject
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext; // Use this one
// Sink.InitContext is no longer needed
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

/**
 * Flink Sink to write EMG Fatigue Alert JSON strings to a TimescaleDB/PostgreSQL database.
 * Uses Sink API v2 (WriterInitContext). Parses JSON to extract fields for insertion.
 */
public class EMGDatabaseSink implements Sink<String> {
    private static final Logger logger = LoggerFactory.getLogger(EMGDatabaseSink.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;

    public EMGDatabaseSink(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    // --- Use WriterInitContext for Flink 1.15+ ---
    @Override
    public SinkWriter<String> createWriter(WriterInitContext context) throws IOException {
        return new EMGDatabaseSinkWriter(jdbcUrl, username, password);
    }

    @Override
    @Deprecated
    public SinkWriter<String> createWriter(InitContext context) throws IOException {
         logger.warn("EMGDatabaseSink: Using deprecated createWriter(Sink.InitContext).");
         // Delegate to the same writer implementation
         return new EMGDatabaseSinkWriter(jdbcUrl, username, password);
    }
    // --- Inner Writer Class ---
    private static class EMGDatabaseSinkWriter implements SinkWriter<String>, Serializable {
        private static final long serialVersionUID = 1L;

        // !!! Adjust SQL query to match your table schema !!!
        private static final String INSERT_SQL =
            "INSERT INTO emg_fatigue_alerts (" +
            "time, thingid, alert_timestamp, muscle, severity, reason, " +
            "average_rms, check_window_seconds, rms_threshold, full_alert_json" +
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::JSONB)"; // Example

        private final String jdbcUrl;
        private final String username;
        private final String password;

        private transient Connection connection;
        private transient PreparedStatement statement;
        // private transient Gson gson; // REMOVED - Unused field

        public EMGDatabaseSinkWriter(String jdbcUrl, String username, String password) throws IOException {
             this.jdbcUrl = jdbcUrl;
             this.username = username;
             this.password = password;
             // this.gson = new Gson(); // REMOVED - Unused field initialization
             initializeJdbc();
        }

        private void initializeJdbc() throws IOException {
             try {
                // this.gson = new Gson(); // REMOVED - Unused field initialization
                this.connection = DriverManager.getConnection(jdbcUrl, username, password);
                this.statement = connection.prepareStatement(INSERT_SQL);
                logger.info("EMGDB Sink: Successfully connected/reconnected to the database.");
            } catch (SQLException e) {
                logger.error("EMGDB Sink: Failed to establish JDBC connection", e);
                throw new IOException("Failed to establish JDBC connection", e);
            }
        }

        @Override
        public void write(String jsonRecord, Context context) throws IOException {
            if (jsonRecord == null || jsonRecord.isEmpty()) {
                // logger.warn("EMGDB Sink: Received null or empty record."); // Reduce log noise maybe
                return;
            }
             checkConnection(); // Check/reconnect if needed

            try {
                // 1. Parse JSON using JsonParser
                JsonObject alertJson = JsonParser.parseString(jsonRecord).getAsJsonObject();

                // 2. Extract fields
                String thingId = alertJson.has("thingId") ? alertJson.get("thingId").getAsString() : "unknown";
                Long alertTimestampMillis = alertJson.has("timestamp") ? alertJson.get("timestamp").getAsLong() : null;
                String muscle = alertJson.has("muscle") ? alertJson.get("muscle").getAsString() : "unknown";
                String severity = alertJson.has("severity") ? alertJson.get("severity").getAsString() : "UNKNOWN";
                String reason = alertJson.has("reason") ? alertJson.get("reason").getAsString() : "N/A";
                Double averageRms = alertJson.has("averageRMS") && !alertJson.get("averageRMS").isJsonNull() ? alertJson.get("averageRMS").getAsDouble() : null;
                Integer checkWindow = alertJson.has("checkWindowSeconds") ? alertJson.get("checkWindowSeconds").getAsInt() : null;
                Double rmsThreshold = alertJson.has("rmsThreshold") && !alertJson.get("rmsThreshold").isJsonNull() ? alertJson.get("rmsThreshold").getAsDouble() : null;

                if (alertTimestampMillis == null) {
                    logger.warn("EMGDB Sink: Missing alert timestamp in JSON for {}. Skipping insert.", thingId);
                    return;
                }

                // 3. Convert timestamp
                Timestamp alertTs = new Timestamp(alertTimestampMillis);

                // 4. Set PreparedStatement parameters
                statement.setTimestamp(1, alertTs); // time (hypertable column)
                statement.setString(2, thingId);
                statement.setTimestamp(3, alertTs); // alert_timestamp column
                statement.setString(4, muscle);
                statement.setString(5, severity);
                statement.setString(6, reason);
                if (averageRms != null) statement.setDouble(7, averageRms); else statement.setNull(7, Types.DOUBLE);
                if (checkWindow != null) statement.setInt(8, checkWindow); else statement.setNull(8, Types.INTEGER);
                if (rmsThreshold != null) statement.setDouble(9, rmsThreshold); else statement.setNull(9, Types.DOUBLE);
                statement.setString(10, jsonRecord); // full_alert_json (or setObject for JSONB)

                // 5. Execute
                statement.executeUpdate();

            } catch (JsonSyntaxException e) {
                 logger.error("EMGDB Sink: Error parsing EMG Alert JSON record: {}", jsonRecord, e);
            } catch (SQLException e) {
                logger.error("EMGDB Sink: Error inserting EMG Alert data into database: {}", jsonRecord, e);
                 // throw new IOException("Error inserting EMG Alert data", e); // Optional: Fail task on DB error
            } catch (Exception e) {
                logger.error("EMGDB Sink: Unexpected error writing EMG Alert record: {}", jsonRecord, e);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException {
            // No-op for non-batching writes
        }

        @Override
        public void close() throws IOException {
            // ... same close logic as before ...
            closeSilently(); // Use helper to ensure cleanup
            logger.info("EMGDB Sink: Database connection closed.");
        }

        // --- checkConnection and closeSilently helpers ---
        private void checkConnection() throws IOException { /* ... same as before ... */
             if (connection == null) { initializeJdbc(); return; }
            try {
                if (!connection.isValid(1)) {
                    logger.warn("EMGDB Sink: JDBC connection is not valid. Reconnecting...");
                    closeSilently();
                    initializeJdbc();
                }
            } catch (SQLException e) {
                 logger.error("EMGDB Sink: Error checking/restoring connection.", e);
                 closeSilently();
                 initializeJdbc();
            }
         }
         private void closeSilently() { /* ... same as before ... */
              try { if (statement != null) statement.close(); } catch (SQLException ignored) {}
             try { if (connection != null) connection.close(); } catch (SQLException ignored) {}
             statement = null;
             connection = null;
         }
    }
}