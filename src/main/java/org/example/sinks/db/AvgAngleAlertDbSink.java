
// File: Flink-CEP/src/main/java/org/example/sinks/db/AvgAngleAlertDbSink.java
package org.example.sinks.db; // Updated package

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable; // Import Serializable
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types; // Import Types

// Renamed from MoCapAverageAngleAlertDatabaseSink
public class AvgAngleAlertDbSink implements Sink<String> { // Implement Serializable
    private static final Logger logger = LoggerFactory.getLogger(AvgAngleAlertDbSink.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String tableName; // Make table name configurable

    public AvgAngleAlertDbSink(String jdbcUrl, String tableName, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
    }

    @Override
    public SinkWriter<String> createWriter(WriterInitContext context) throws IOException {
        return new AvgAngleAlertDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    // Keep for potential backward compatibility
    @Override
    @Deprecated
    public SinkWriter<String> createWriter(InitContext context) throws IOException {
        logger.warn("AvgAngleAlertDbSink: Using deprecated createWriter(Sink.InitContext).");
        return new AvgAngleAlertDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    private static class AvgAngleAlertDbSinkWriter implements SinkWriter<String>, Serializable {
        private static final long serialVersionUID = 503L; // Unique ID

        private final String insertSql; // SQL constructed dynamically
        private final String jdbcUrl;
        private final String username;
        private final String password;

        private transient Connection connection;
        private transient PreparedStatement statement;
        // Removed unused Gson field

        public AvgAngleAlertDbSinkWriter(String jdbcUrl, String tableName, String username, String password) throws IOException {
             this.jdbcUrl = jdbcUrl;
             this.username = username;
             this.password = password;
             // Construct SQL dynamically
             this.insertSql = "INSERT INTO " + tableName + " (" +
                "time, thingid, window_end_ts, feedback_type, alert_details" +
                ") VALUES (?, ?, ?, ?, ?::JSONB)"; // Assuming JSONB, adjust if TEXT
            initializeJdbc();
        }

         private void initializeJdbc() throws IOException {
             try {
                this.connection = DriverManager.getConnection(jdbcUrl, username, password);
                this.statement = connection.prepareStatement(this.insertSql);
                logger.info("AvgAngleAlert Sink: Successfully connected/reconnected to the database.");
            } catch (SQLException e) {
                logger.error("AvgAngleAlert Sink: Failed to establish JDBC connection", e);
                throw new IOException("Failed to establish JDBC connection", e);
            }
        }

        @Override
        public void write(String jsonRecord, Context context) throws IOException {
            // (Keep the existing write logic from MoCapAverageAngleAlertDatabaseSinkWriter)
             if (jsonRecord == null || jsonRecord.isEmpty()) { return; }
             checkConnection();
             try {
                JsonObject jsonObject = JsonParser.parseString(jsonRecord).getAsJsonObject();
                String thingId = jsonObject.has("thingId") ? jsonObject.get("thingId").getAsString() : null;
                Long windowEndMillis = jsonObject.has("windowEndTimestamp") ? jsonObject.get("windowEndTimestamp").getAsLong() : null;
                String feedbackType = jsonObject.has("feedbackType") ? jsonObject.get("feedbackType").getAsString() : "unknown";

                if (thingId == null || windowEndMillis == null) { logger.warn("AvgAngleAlert Sink: Missing fields in JSON: {}", jsonRecord); return; }

                Timestamp windowEndTs = new Timestamp(windowEndMillis);
                Timestamp hypertableTime = windowEndTs; // Use window end as primary time

                statement.setTimestamp(1, hypertableTime);
                statement.setString(2, thingId);
                statement.setTimestamp(3, windowEndTs);
                statement.setString(4, feedbackType);
                statement.setString(5, jsonRecord); // Store full JSON
                statement.executeUpdate();

            } catch (JsonSyntaxException e) { logger.error("AvgAngleAlert Sink: Error parsing JSON: {}", jsonRecord, e);
            } catch (SQLException e) { logger.error("AvgAngleAlert Sink: Error inserting data: {}", jsonRecord, e);
            } catch (Exception e) { logger.error("AvgAngleAlert Sink: Unexpected error writing: {}", jsonRecord, e); }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException {
            // No-op
        }

        @Override
        public void close() throws IOException {
            // (Keep the existing close logic from MoCapAverageAngleAlertDatabaseSinkWriter)
            closeSilently();
            logger.info("AvgAngleAlert Sink: Database connection closed.");
        }

         // --- checkConnection and closeSilently helpers (same as in MoCapRawDbSink) ---
         private void checkConnection() throws IOException { /* ... */
              if (connection == null) { initializeJdbc(); return; }
            try {
                if (!connection.isValid(1)) {
                    logger.warn("AvgAngleAlert Sink: JDBC connection is not valid. Reconnecting...");
                    closeSilently();
                    initializeJdbc();
                }
            } catch (SQLException e) {
                 logger.error("AvgAngleAlert Sink: Error checking/restoring connection.", e);
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
