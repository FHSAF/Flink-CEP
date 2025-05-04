
// File: Flink-CEP/src/main/java/org/example/sinks/db/EyeGazeRawDbSink.java
package org.example.sinks.db;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.example.models.EyeGazeReading; // Import the correct model
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.*;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

// NEW Sink - Handles EyeGazeReading for raw storage
public class EyeGazeRawDbSink implements Sink<EyeGazeReading> {
    private static final Logger logger = LoggerFactory.getLogger(EyeGazeRawDbSink.class);
    // ISO 8601 format expected from C# DateTime.UtcNow.ToString("o")
    private static final DateTimeFormatter ISO_TIMESTAMP_FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;


    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String tableName;

    public EyeGazeRawDbSink(String jdbcUrl, String tableName, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
    }

    @Override
    public SinkWriter<EyeGazeReading> createWriter(WriterInitContext context) throws IOException {
        return new EyeGazeRawDbSinkWriter(jdbcUrl, tableName, username, password);
    }

     // Keep for potential backward compatibility
    @Override
    @Deprecated
    public SinkWriter<EyeGazeReading> createWriter(InitContext context) throws IOException {
        logger.warn("EyeGazeRawDbSink: Using deprecated createWriter(Sink.InitContext).");
        return new EyeGazeRawDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    private static class EyeGazeRawDbSinkWriter implements SinkWriter<EyeGazeReading>, Serializable {
        private static final long serialVersionUID = 508L; // Unique ID

        private final String insertSql;
        private final String jdbcUrl;
        private final String username;
        private final String password;

        private transient Connection connection;
        private transient PreparedStatement statement;

        public EyeGazeRawDbSinkWriter(String jdbcUrl, String tableName, String username, String password) throws IOException {
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
            // TODO: Define your table schema and corresponding INSERT SQL
            // Example assumes columns: time, thingid, timestamp_str, attention_state
            this.insertSql = "INSERT INTO " + tableName + " (time, thingid, timestamp_str, attention_state) VALUES (?, ?, ?, ?)";
            initializeJdbc();
        }

         private void initializeJdbc() throws IOException {
             try {
                this.connection = DriverManager.getConnection(jdbcUrl, username, password);
                this.statement = connection.prepareStatement(this.insertSql);
                logger.info("EyeGazeRaw Sink: Successfully connected/reconnected.");
            } catch (SQLException e) {
                logger.error("EyeGazeRaw Sink: Failed JDBC connection", e);
                throw new IOException("Failed JDBC connection", e);
            }
        }

        @Override
        public void write(EyeGazeReading record, Context context) throws IOException {
            if (record == null || record.getThingid() == null || record.getTimestamp() == null) { return; }
             checkConnection();

            try {
                 // 1. Parse ISO Timestamp
                 Timestamp sqlTimestamp = null;
                 try {
                     Instant instant = Instant.from(ISO_TIMESTAMP_FORMATTER.parse(record.getTimestamp()));
                     sqlTimestamp = Timestamp.from(instant);
                 } catch (DateTimeParseException | NullPointerException e) {
                     logger.warn("EyeGazeRaw Sink: Failed parse ISO timestamp '{}' for {}. Storing NULL.", record.getTimestamp(), record.getThingid());
                 }

                // 2. Set Parameters
                if (sqlTimestamp != null) statement.setTimestamp(1, sqlTimestamp); else statement.setNull(1, Types.TIMESTAMP_WITH_TIMEZONE);
                statement.setString(2, record.getThingid());
                statement.setString(3, record.getTimestamp()); // Store original string
                statement.setBoolean(4, record.isAttention()); // attention_state

                // 3. Execute Update
                statement.executeUpdate();

            } catch (SQLException e) {
                logger.error("EyeGazeRaw Sink: Error inserting data: {}", record, e);
            } catch (Exception e) {
                 logger.error("EyeGazeRaw Sink: Unexpected error writing: {}", record, e);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException { /* No-op */ }

        @Override
        public void close() throws IOException {
            closeSilently();
            logger.info("EyeGazeRaw Sink: Database connection closed.");
        }

        // --- checkConnection and closeSilently helpers ---
         private void checkConnection() throws IOException { /* ... */
              if (connection == null) { initializeJdbc(); return; }
            try {
                if (!connection.isValid(1)) {
                    logger.warn("EyeGazeRaw Sink: JDBC connection is not valid. Reconnecting...");
                    closeSilently();
                    initializeJdbc();
                }
            } catch (SQLException e) {
                 logger.error("EyeGazeRaw Sink: Error checking/restoring connection.", e);
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
