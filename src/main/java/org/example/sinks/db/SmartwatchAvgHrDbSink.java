
// File: Flink-CEP/src/main/java/org/example/sinks/db/SmartwatchAvgHrDbSink.java
package org.example.sinks.db;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.*;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class SmartwatchAvgHrDbSink implements Sink<String> {
    private static final Logger logger = LoggerFactory.getLogger(SmartwatchAvgHrDbSink.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String tableName;

    public SmartwatchAvgHrDbSink(String jdbcUrl, String tableName, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
    }

    @Override
    public SinkWriter<String> createWriter(WriterInitContext context) throws IOException {
        return new SmartwatchAvgHrDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    @Override
    @Deprecated
    public SinkWriter<String> createWriter(InitContext context) throws IOException {
        logger.warn("SmartwatchAvgHrDbSink: Using deprecated createWriter(Sink.InitContext).");
        return new SmartwatchAvgHrDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    private static class SmartwatchAvgHrDbSinkWriter implements SinkWriter<String>, Serializable {
        private static final long serialVersionUID = 507L;

        private final String insertSql;
        private final String jdbcUrl;
        private final String username;
        private final String password;
        private transient Connection connection;
        private transient PreparedStatement statement;

        public SmartwatchAvgHrDbSinkWriter(String jdbcUrl, String tableName, String username, String password) throws IOException {
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
            // Added ON CONFLICT DO NOTHING/UPDATE (choose one)
            this.insertSql = "INSERT INTO " + tableName + " (time, thingid, window_end_ts, avg_heart_rate, is_alert) VALUES (?, ?, ?, ?, ?) ON CONFLICT (time, thingid) DO NOTHING";
            // Or: "ON CONFLICT (time, thingid) DO UPDATE SET avg_heart_rate = EXCLUDED.avg_heart_rate, is_alert = EXCLUDED.is_alert";
            initializeJdbc();
        }

        private void initializeJdbc() throws IOException { /* ... keep existing logic ... */
             try {
                this.connection = DriverManager.getConnection(jdbcUrl, username, password);
                this.statement = connection.prepareStatement(this.insertSql);
                logger.info("SmartwatchAvgHr Sink: Successfully connected/reconnected.");
            } catch (SQLException e) {
                logger.error("SmartwatchAvgHr Sink: Failed JDBC connection", e);
                throw new IOException("Failed JDBC connection", e);
            }
        }

        @Override
        public void write(String recordString, Context context) throws IOException {
            if (recordString == null || recordString.isEmpty()) { return; }
             checkConnection();

            String thingId = "unknown";
            Long windowEndMillis = null;
            Double avgHr = null;
            Boolean isAlert = null;
            Timestamp windowEndTs = null; // Initialize

            try {
                // Assuming processor outputs JSON: {"thingId":"..", "windowEndTimestamp":123..., "averageHeartRate":75.5, "isAlert":false}
                JsonObject jsonObject = JsonParser.parseString(recordString).getAsJsonObject();
                thingId = jsonObject.has("thingId") ? jsonObject.get("thingId").getAsString() : "unknown";
                windowEndMillis = jsonObject.has("windowEndTimestamp") ? jsonObject.get("windowEndTimestamp").getAsLong() : null;
                avgHr = jsonObject.has("averageHeartRate") && !jsonObject.get("averageHeartRate").isJsonNull() ? jsonObject.get("averageHeartRate").getAsDouble() : null;
                isAlert = jsonObject.has("isAlert") ? jsonObject.get("isAlert").getAsBoolean() : false;

                if (windowEndMillis == null) {
                    logger.warn("SmartwatchAvgHr Sink: Missing windowEndTimestamp in JSON: {}. Using current time.", recordString);
                    windowEndMillis = System.currentTimeMillis(); // Fallback
                }
                windowEndTs = new Timestamp(windowEndMillis);

            } catch (JsonSyntaxException | IllegalStateException | NullPointerException e) {
                logger.error("SmartwatchAvgHr Sink: Error parsing record string as JSON: {}", recordString, e);
                return; // Skip if parsing fails
            }

            try {
                // Set parameters
                statement.setTimestamp(1, windowEndTs); // time (using window end)
                statement.setString(2, thingId);
                statement.setTimestamp(3, windowEndTs); // window_end_ts
                if (avgHr != null) statement.setDouble(4, avgHr); else statement.setNull(4, Types.DOUBLE);
                if (isAlert != null) statement.setBoolean(5, isAlert); else statement.setNull(5, Types.BOOLEAN);

                statement.executeUpdate();

            } catch (SQLException e) {
                 if (!"23505".equals(e.getSQLState())) { // Don't log duplicate key warnings if using ON CONFLICT
                    logger.error("SmartwatchAvgHr Sink: Error inserting data: {}", recordString, e);
                 }
            } catch (Exception e) {
                 logger.error("SmartwatchAvgHr Sink: Unexpected error writing: {}", recordString, e);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException { /* No-op */ }

        @Override
        public void close() throws IOException { /* ... keep existing logic ... */
            closeSilently();
            logger.info("SmartwatchAvgHr Sink: Database connection closed.");
        }

        private void checkConnection() throws IOException { /* ... keep existing logic ... */
            if (connection == null) { initializeJdbc(); return; }
            try {
                if (!connection.isValid(1)) {
                    logger.warn("SmartwatchAvgHr Sink: JDBC connection is not valid. Reconnecting...");
                    closeSilently();
                    initializeJdbc();
                }
            } catch (SQLException e) {
                 logger.error("SmartwatchAvgHr Sink: Error checking/restoring connection.", e);
                 closeSilently();
                 initializeJdbc();
            }
        }
        private void closeSilently() { /* ... keep existing logic ... */
             try { if (statement != null) statement.close(); } catch (SQLException ignored) {}
             try { if (connection != null) connection.close(); } catch (SQLException ignored) {}
             statement = null;
             connection = null;
        }
    }
}
