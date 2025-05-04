package org.example.sinks; // Or your preferred package

import com.google.gson.Gson;
import com.google.gson.JsonObject; // Import Gson JsonObject
import com.google.gson.JsonParser; // Import Gson JsonParser
import com.google.gson.JsonSyntaxException;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;


// Sink accepts the JSON String output from ErgonomicsProcessor
public class MoCapAverageAngleAlertDatabaseSink implements Sink<String> {
    private static final Logger logger = LoggerFactory.getLogger(MoCapAverageAngleAlertDatabaseSink.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;

    public MoCapAverageAngleAlertDatabaseSink(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        // Optional: Load driver explicitly if needed
        // try { Class.forName("org.postgresql.Driver"); } catch (ClassNotFoundException e) { logger.error("PostgreSQL driver not found", e); }
    }

    @Override
    public SinkWriter<String> createWriter(WriterInitContext context) throws IOException {
        return new MoCapAverageAngleAlertDatabaseSinkWriter(jdbcUrl, username, password);
    }

    // Keep for potential backward compatibility if needed by specific Flink runner/version
    @Override
    @Deprecated
    public SinkWriter<String> createWriter(InitContext context) throws IOException {
         logger.warn("Using deprecated InitContext in createWriter for MoCapAverageAngleAlertDatabaseSink.");
        return new MoCapAverageAngleAlertDatabaseSinkWriter(jdbcUrl, username, password);
    }

    // --- Inner Writer Class ---
    private static class MoCapAverageAngleAlertDatabaseSinkWriter implements SinkWriter<String> {

        // !!! IMPORTANT: Adjust SQL query to match your actual table schema !!!
        // Example assumes storing full JSON in alert_details (JSONB or TEXT)
        private static final String INSERT_SQL =
            "INSERT INTO average_angle_alerts (" +
            "time, thingid, window_end_ts, feedback_type, alert_details" +
            ") VALUES (?, ?, ?, ?, ?::JSONB)"; // Use ?::JSONB if column type is JSONB, otherwise just ? for TEXT

        private final Connection connection;
        private final PreparedStatement statement;
        private final Gson gson; // For parsing JSON string

        public MoCapAverageAngleAlertDatabaseSinkWriter(String jdbcUrl, String username, String password) throws IOException {
             this.gson = new Gson();
            try {
                connection = DriverManager.getConnection(jdbcUrl, username, password);
                // Optional: Set properties for JSONB handling if needed by driver version
                // Properties props = new Properties();
                // props.setProperty("user", username);
                // props.setProperty("password", password);
                // props.setProperty("stringtype", "unspecified"); // May help some drivers treat String as unknown for JSONB
                // connection = DriverManager.getConnection(jdbcUrl, props);

                statement = connection.prepareStatement(INSERT_SQL);
                logger.info("AvgAngleAlert Sink: Successfully connected to the database.");
            } catch (SQLException e) {
                logger.error("AvgAngleAlert Sink: Failed to establish JDBC connection", e);
                throw new IOException("Failed to establish JDBC connection", e);
            }
        }

        @Override
        public void write(String jsonRecord, Context context) throws IOException {
            if (jsonRecord == null || jsonRecord.isEmpty()) {
                logger.warn("AvgAngleAlert Sink: Received null or empty record.");
                return;
            }

            try {
                // 1. Parse JSON to extract key fields using Gson's JsonParser
                JsonObject jsonObject = JsonParser.parseString(jsonRecord).getAsJsonObject();

                String thingId = jsonObject.has("thingId") ? jsonObject.get("thingId").getAsString() : null;
                Long windowEndMillis = jsonObject.has("windowEndTimestamp") ? jsonObject.get("windowEndTimestamp").getAsLong() : null;
                String feedbackType = jsonObject.has("feedbackType") ? jsonObject.get("feedbackType").getAsString() : "unknown"; // Default if missing

                if (thingId == null || windowEndMillis == null) {
                     logger.warn("AvgAngleAlert Sink: Missing thingId or windowEndTimestamp in JSON: {}", jsonRecord);
                     return;
                }

                // 2. Convert epoch milliseconds to SQL Timestamps
                Timestamp windowEndTs = new Timestamp(windowEndMillis);
                // Using windowEndTs also for the primary 'time' column hypertable partitioning
                Timestamp hypertableTime = windowEndTs;

                // 3. Set parameters for the PreparedStatement
                statement.setTimestamp(1, hypertableTime);
                statement.setString(2, thingId);
                statement.setTimestamp(3, windowEndTs); // Store the specific window end
                statement.setString(4, feedbackType);

                // 4. Store the original JSON string in the alert_details column
                // If column type is JSONB use setObject with Types.OTHER or rely on ?::JSONB cast in SQL
                statement.setString(5, jsonRecord); // Assumes column is TEXT or driver handles conversion with ?::JSONB
                // Alternate for native JSONB type with some drivers/configs:
                // statement.setObject(5, jsonRecord, Types.OTHER);

                // 5. Execute the insert
                statement.executeUpdate();
                // logger.debug("AvgAngleAlert Sink: Inserted alert record for thingId: {}", thingId);

            } catch (JsonSyntaxException e) {
                 logger.error("AvgAngleAlert Sink: Error parsing JSON record: {}", jsonRecord, e);
            } catch (SQLException e) {
                logger.error("AvgAngleAlert Sink: Error inserting alert data into database: {}", jsonRecord, e);
                 // Decide on error handling (e.g., throw IOException to fail job?)
                 // throw new IOException("Error inserting alert data", e);
            } catch (Exception e) {
                // Catch other potential exceptions
                logger.error("AvgAngleAlert Sink: Unexpected error writing alert record: {}", jsonRecord, e);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException {
            // No-op usually fine for non-batching JDBC writes
             // logger.debug("AvgAngleAlert Sink: Flush called (endOfInput={})", endOfInput);
        }

        @Override
        public void close() throws IOException {
            try {
                if (statement != null) statement.close();
                if (connection != null) connection.close();
                logger.info("AvgAngleAlert Sink: Database connection closed.");
            } catch (SQLException e) {
                logger.error("AvgAngleAlert Sink: Error closing JDBC connection", e);
                 throw new IOException("Error closing JDBC connection", e);
            }
        }
    }
}