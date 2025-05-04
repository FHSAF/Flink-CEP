
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
import com.google.gson.JsonObject; // Using Gson for parsing the string input
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;


// NEW Sink - Handles String output from SmartwatchAvgHrProcessor
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

     // Keep for potential backward compatibility
    @Override
    @Deprecated
    public SinkWriter<String> createWriter(InitContext context) throws IOException {
        logger.warn("SmartwatchAvgHrDbSink: Using deprecated createWriter(Sink.InitContext).");
        return new SmartwatchAvgHrDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    private static class SmartwatchAvgHrDbSinkWriter implements SinkWriter<String>, Serializable {
        private static final long serialVersionUID = 507L; // Unique ID

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
            // TODO: Define your table schema and corresponding INSERT SQL
            // Example assumes columns: time, thingid, window_end_ts, avg_heart_rate, is_alert
            this.insertSql = "INSERT INTO " + tableName + " (time, thingid, window_end_ts, avg_heart_rate, is_alert) VALUES (?, ?, ?, ?, ?)";
            initializeJdbc();
        }

        private void initializeJdbc() throws IOException {
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

            // TODO: Implement parsing logic for the String record
            // This depends on the EXACT format produced by SmartwatchAvgHrProcessor.
            // Assuming it's a simple string like "Smartwatch Avg HR [thingId] [start - end]: avg bpm [🚨 ALERT]"
            // A more robust way would be to output JSON from the processor.
            // --- Example Parsing (Needs refinement based on actual string format) ---
            String thingId = "unknown";
            long windowEndMillis = System.currentTimeMillis(); // Default if parsing fails
            double avgHr = Double.NaN;
            boolean isAlert = false;

            try {
                 // Basic parsing - VERY FRAGILE, assumes specific format
                 if (recordString.contains("ALERT:")) {
                     isAlert = true;
                     recordString = recordString.replace(" 🚨 ALERT: High average heart rate!", "").trim();
                 }
                 String[] parts = recordString.split("]:");
                 if (parts.length > 1) {
                     String keyPart = parts[0]; // "Smartwatch Avg HR [thingId] [start - end"
                     String valuePart = parts[1].trim().replace(" bpm", ""); // "avg"

                     // Extract thingId (assuming format like "[thingId]")
                     int idStart = keyPart.indexOf('[');
                     int idEnd = keyPart.indexOf(']');
                     if (idStart != -1 && idEnd != -1 && idEnd > idStart) {
                         thingId = keyPart.substring(idStart + 1, idEnd);
                     }

                     // Extract window end (more complex, might need regex or better format)
                     // For simplicity, we'll just use current time or a fixed value if needed
                     // Timestamp windowEndTs = new Timestamp(windowEndMillis);

                     // Extract avgHr
                     avgHr = Double.parseDouble(valuePart);
                 } else {
                     logger.warn("SmartwatchAvgHr Sink: Could not parse record string format: {}", recordString);
                 }

            } catch (Exception e) {
                logger.error("SmartwatchAvgHr Sink: Error parsing record string: {}", recordString, e);
                // Decide how to handle - skip, insert defaults?
                return; // Skip for now
            }
            // --- End Example Parsing ---


            try {
                Timestamp windowEndTs = new Timestamp(windowEndMillis); // Use parsed/defaulted end time

                statement.setTimestamp(1, windowEndTs); // time (hypertable column)
                statement.setString(2, thingId);
                statement.setTimestamp(3, windowEndTs); // window_end_ts
                if (!Double.isNaN(avgHr)) statement.setDouble(4, avgHr); else statement.setNull(4, Types.DOUBLE);
                statement.setBoolean(5, isAlert);

                statement.executeUpdate();

            } catch (SQLException e) {
                logger.error("SmartwatchAvgHr Sink: Error inserting data: {}", recordString, e);
            } catch (Exception e) {
                 logger.error("SmartwatchAvgHr Sink: Unexpected error writing: {}", recordString, e);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException { /* No-op */ }

        @Override
        public void close() throws IOException {
            closeSilently();
            logger.info("SmartwatchAvgHr Sink: Database connection closed.");
        }

        // --- checkConnection and closeSilently helpers ---
         private void checkConnection() throws IOException { /* ... */
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
         private void closeSilently() { /* ... */
             try { if (statement != null) statement.close(); } catch (SQLException ignored) {}
             try { if (connection != null) connection.close(); } catch (SQLException ignored) {}
             statement = null;
             connection = null;
         }
    }
}
