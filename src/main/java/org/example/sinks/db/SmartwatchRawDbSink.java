
// File: Flink-CEP/src/main/java/org/example/sinks/db/SmartwatchRawDbSink.java
package org.example.sinks.db; // Updated package

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.example.models.SmartwatchReading; // Updated model import
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable; // Import Serializable
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

// Renamed from SmartWatchDatabaseSink
public class SmartwatchRawDbSink implements Sink<SmartwatchReading> { // Implement Serializable
    private static final Logger logger = LoggerFactory.getLogger(SmartwatchRawDbSink.class);
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String tableName; // Make table name configurable

    public SmartwatchRawDbSink(String jdbcUrl, String tableName, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
    }

    @Override
    public SinkWriter<SmartwatchReading> createWriter(WriterInitContext context) throws IOException {
        return new SmartwatchRawDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    // Keep for potential backward compatibility
    @Override
    @Deprecated
    public SinkWriter<SmartwatchReading> createWriter(InitContext context) throws IOException {
        logger.warn("SmartwatchRawDbSink: Using deprecated createWriter(Sink.InitContext).");
        return new SmartwatchRawDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    private static class SmartwatchRawDbSinkWriter implements SinkWriter<SmartwatchReading>, Serializable {
        private static final long serialVersionUID = 506L; // Unique ID

        private final String insertSql; // SQL constructed dynamically
        private final String jdbcUrl;
        private final String username;
        private final String password;

        private transient Connection connection;
        private transient PreparedStatement statement;

        public SmartwatchRawDbSinkWriter(String jdbcUrl, String tableName, String username, String password) throws IOException {
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
            // Construct SQL dynamically
            this.insertSql = "INSERT INTO " + tableName + " (time, thingid, heartrate, timestamp_str) VALUES (?, ?, ?, ?)"; // Added time, timestamp_str
            initializeJdbc();
        }

         private void initializeJdbc() throws IOException {
             try {
                this.connection = DriverManager.getConnection(jdbcUrl, username, password);
                this.statement = connection.prepareStatement(this.insertSql);
                logger.info("SmartwatchRaw Sink: Successfully connected/reconnected.");
            } catch (SQLException e) {
                logger.error("SmartwatchRaw Sink: Failed JDBC connection", e);
                throw new IOException("Failed JDBC connection", e);
            }
        }

        @Override
        public void write(SmartwatchReading record, Context context) throws IOException {
            // (Keep the existing write logic from SmartwatchSinkWriter, adapt for new SQL)
             if (record == null || record.getThingid() == null || record.getTimestamp() == null) { return; }
             checkConnection();
            try {
                 Timestamp sqlTimestamp = null;
                try {
                     LocalDateTime ldt = LocalDateTime.parse(record.getTimestamp(), TIMESTAMP_FORMATTER);
                     sqlTimestamp = Timestamp.from(ldt.toInstant(ZoneOffset.UTC));
                } catch (DateTimeParseException | NullPointerException e) {
                    logger.warn("SmartwatchRaw Sink: Failed parse timestamp '{}' for {}. Storing NULL.", record.getTimestamp(), record.getThingid());
                }

                if (sqlTimestamp != null) statement.setTimestamp(1, sqlTimestamp); else statement.setNull(1, Types.TIMESTAMP_WITH_TIMEZONE);
                statement.setString(2, record.getThingid());
                statement.setInt(3, record.getHeartrate());
                statement.setString(4, record.getTimestamp()); // timestamp_str

                statement.executeUpdate();
                // logger.debug("Inserted Smartwatch record: {}", record);
            } catch (SQLException e) {
                logger.error("Error inserting smartwatch data: {}", record, e);
            } catch (Exception e) {
                 logger.error("SmartwatchRaw Sink: Unexpected error writing: {}", record, e);
            }
        }

        @Override
        public void flush(boolean endOfInput) {}

        @Override
        public void close() throws IOException {
            // (Keep the existing close logic from SmartwatchSinkWriter)
            closeSilently();
            logger.info("SmartwatchRaw Sink: Database connection closed.");
        }

         // --- checkConnection and closeSilently helpers (same as in MoCapRawDbSink) ---
         private void checkConnection() throws IOException { /* ... */
              if (connection == null) { initializeJdbc(); return; }
            try {
                if (!connection.isValid(1)) {
                    logger.warn("SmartwatchRaw Sink: JDBC connection is not valid. Reconnecting...");
                    closeSilently();
                    initializeJdbc();
                }
            } catch (SQLException e) {
                 logger.error("SmartwatchRaw Sink: Error checking/restoring connection.", e);
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
