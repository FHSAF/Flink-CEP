package org.example.sinks; // Or your preferred package

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext; // Use this for Flink 1.15+
import org.example.models.RebaScore; // Import your RebaScore POJO
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset; // Or your preferred ZoneId
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;


// Sink accepts the JSON String output from RebaMapper
public class MoCapRebaScoreDatabaseSink implements Sink<String> {
    private static final Logger logger = LoggerFactory.getLogger(MoCapRebaScoreDatabaseSink.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;

    public MoCapRebaScoreDatabaseSink(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        // Ensure JDBC driver is loaded (optional, modern drivers often auto-register)
        // try { Class.forName("org.postgresql.Driver"); } catch (ClassNotFoundException e) { logger.error("PostgreSQL JDBC driver not found", e); }
    }

    // --- Use WriterInitContext for Flink 1.15+ ---
    @Override
    public SinkWriter<String> createWriter(WriterInitContext context) throws IOException {
        return new MoCapRebaScoreDatabaseSinkWriter(jdbcUrl, username, password);
    }

    // --- Keep createWriter with InitContext for potential backward compatibility ---
    // This might cause warnings depending on your Flink version / Sink API usage pattern
    @Override
    @Deprecated
    public SinkWriter<String> createWriter(InitContext context) throws IOException {
         logger.warn("Using deprecated InitContext in createWriter. Consider updating Sink API usage if possible.");
        return new MoCapRebaScoreDatabaseSinkWriter(jdbcUrl, username, password);
    }


    // --- Inner Writer Class ---
    private static class MoCapRebaScoreDatabaseSinkWriter implements SinkWriter<String> {

        // !!! IMPORTANT: Adjust SQL query to match your table name and columns !!!
        private static final String INSERT_SQL =
            "INSERT INTO reba_scores (" +
            "time, thingid, timestamp_str, " + // using 'time' for hypertable, 'timestamp_str' for original string
            "neck_score, trunk_score, leg_score, " +
            "upper_arm_left_score, upper_arm_right_score, " +
            "lower_arm_left_score, lower_arm_right_score, " +
            "wrist_left_score, wrist_right_score, " +
            "group_a_score, group_b_score, table_c_score, " +
            "load_force_score, coupling_score, activity_score, " +
            "final_reba_score, risk_level" +
            // Add columns for calculationDetails if storing them
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"; // Count the ? placeholders! (20 in this example)

        private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        private final Connection connection;
        private final PreparedStatement statement;
        private final Gson gson; // For parsing JSON string back to object

        public MoCapRebaScoreDatabaseSinkWriter(String jdbcUrl, String username, String password) throws IOException {
             this.gson = new Gson(); // Initialize Gson parser
            try {
                connection = DriverManager.getConnection(jdbcUrl, username, password);
                statement = connection.prepareStatement(INSERT_SQL);
                logger.info("RebaScore Sink: Successfully connected to the database.");
            } catch (SQLException e) {
                logger.error("RebaScore Sink: Failed to establish JDBC connection", e);
                throw new IOException("Failed to establish JDBC connection", e);
            }
        }

        @Override
        public void write(String jsonRecord, Context context) throws IOException {
            if (jsonRecord == null || jsonRecord.isEmpty()) {
                logger.warn("RebaScore Sink: Received null or empty record to write.");
                return;
            }

            try {
                // 1. Parse the JSON string back into a RebaScore object
                RebaScore record = gson.fromJson(jsonRecord, RebaScore.class);

                if (record == null || record.thingId == null) {
                     logger.warn("RebaScore Sink: Failed to parse JSON or missing thingId: {}", jsonRecord);
                     return;
                }

                // 2. Parse the timestamp string from the RebaScore object for DB insertion
                Timestamp sqlTimestamp = null;
                try {
                     LocalDateTime ldt = LocalDateTime.parse(record.timestamp, TIMESTAMP_FORMATTER);
                     // Convert to SQL Timestamp (using UTC, adjust if needed)
                     sqlTimestamp = Timestamp.from(ldt.toInstant(ZoneOffset.UTC));
                } catch (DateTimeParseException | NullPointerException e) {
                    logger.warn("RebaScore Sink: Failed to parse timestamp string '{}' for thingId {}. Storing NULL for time.", record.timestamp, record.thingId, e);
                    // Decide how to handle timestamp errors - store NULL, use processing time, etc.
                }


                // 3. Set parameters for the PreparedStatement
                // Ensure the index numbers match the '?' placeholders in INSERT_SQL
                if (sqlTimestamp != null) {
                    statement.setTimestamp(1, sqlTimestamp); // time (hypertable column)
                } else {
                    statement.setNull(1, java.sql.Types.TIMESTAMP_WITH_TIMEZONE);
                }
                statement.setString(2, record.thingId);
                statement.setString(3, record.timestamp); // timestamp_str (original string)

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

                // Add parameters for calculationDetails if storing them


                // 4. Execute the insert
                statement.executeUpdate();
                // logger.debug("RebaScore Sink: Inserted record into database for thingId: {}", record.thingId); // Debug level might be better

            } catch (JsonSyntaxException e) {
                 logger.error("RebaScore Sink: Error parsing JSON record: {}", jsonRecord, e);
            } catch (SQLException e) {
                logger.error("RebaScore Sink: Error inserting data into database: {}", jsonRecord, e);
                // Optional: throw IOException to signal failure to Flink, depending on desired fault tolerance
                // throw new IOException("Error inserting data into database", e);
            } catch (Exception e) {
                // Catch other potential exceptions during parsing or setting parameters
                logger.error("RebaScore Sink: Unexpected error writing record: {}", jsonRecord, e);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException {
            // The PreparedStatement might batch updates depending on the driver/config,
            // but executeUpdate() typically sends it immediately.
            // If batching were enabled, you'd call statement.executeBatch() here.
            // For this simple case, likely no-op is fine.
             logger.debug("RebaScore Sink: Flush called (endOfInput={})", endOfInput);
        }

        @Override
        public void close() throws IOException {
            try {
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
                logger.info("RebaScore Sink: Database connection closed successfully.");
            } catch (SQLException e) {
                logger.error("RebaScore Sink: Error closing JDBC connection", e);
                 // Still attempt to close underlying resources if possible
                 throw new IOException("Error closing JDBC connection", e); // Propagate to Flink
            }
        }
    }
}