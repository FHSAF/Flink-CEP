
// File: Flink-CEP/src/main/java/org/example/sinks/db/SmartwatchRawDbSink.java
package org.example.sinks.db;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.example.models.SmartwatchReading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.*;
import java.time.Instant; // Use Instant for ISO parsing
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class SmartwatchRawDbSink implements Sink<SmartwatchReading> {
    private static final Logger logger = LoggerFactory.getLogger(SmartwatchRawDbSink.class);
    // Standard ISO formatter
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String tableName;

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

    @Override
    @Deprecated
    public SinkWriter<SmartwatchReading> createWriter(InitContext context) throws IOException {
        logger.warn("SmartwatchRawDbSink: Using deprecated createWriter(Sink.InitContext).");
        return new SmartwatchRawDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    private static class SmartwatchRawDbSinkWriter implements SinkWriter<SmartwatchReading>, Serializable {
        private static final long serialVersionUID = 506L;

        private final String insertSql;
        private final String jdbcUrl;
        private final String username;
        private final String password;
        private transient Connection connection;
        private transient PreparedStatement statement;

        public SmartwatchRawDbSinkWriter(String jdbcUrl, String tableName, String username, String password) throws IOException {
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
            // Added ON CONFLICT DO NOTHING
            this.insertSql = "INSERT INTO " + tableName + " (time, thingid, heartrate, timestamp_str) VALUES (?, ?, ?, ?) ON CONFLICT (time, thingid) DO NOTHING";
            initializeJdbc();
        }

         private void initializeJdbc() throws IOException { /* ... keep existing logic ... */
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
             if (record == null || record.getThingid() == null || record.getTimestamp() == null) { return; }
             checkConnection();

            Timestamp sqlTimestamp = null;
            String originalTimestampStr = record.getTimestamp();
            try {
                // *** Parse the ISO string to get the Instant for the 'time' column ***
                Instant instant = Instant.from(ISO_FORMATTER.parse(originalTimestampStr));
                sqlTimestamp = Timestamp.from(instant);
            } catch (DateTimeParseException | NullPointerException e) {
                logger.warn("SmartwatchRaw Sink: Failed to parse ISO timestamp '{}' for {}. Storing NULL for time.", originalTimestampStr, record.getThingid());
                sqlTimestamp = null;
            } catch (Exception e) {
                 logger.error("SmartwatchRaw Sink: Unexpected error parsing timestamp '{}' for {}.", originalTimestampStr, record.getThingid(), e);
                 sqlTimestamp = null;
            }

            try {
                // Set parameters
                if (sqlTimestamp != null) statement.setTimestamp(1, sqlTimestamp); else statement.setNull(1, Types.TIMESTAMP_WITH_TIMEZONE);
                statement.setString(2, record.getThingid());
                statement.setInt(3, record.getHeartrate());
                statement.setString(4, originalTimestampStr); // Store original string

                statement.executeUpdate();

            } catch (SQLException e) {
                 // ON CONFLICT handles duplicate keys, only log other errors
                 if (!"23505".equals(e.getSQLState())) {
                    logger.error("Error inserting smartwatch data: {}", record, e);
                 }
            } catch (Exception e) {
                 logger.error("SmartwatchRaw Sink: Unexpected error writing: {}", record, e);
            }
        }

        @Override
        public void flush(boolean endOfInput) {}

        @Override
        public void close() throws IOException { /* ... keep existing logic ... */
            closeSilently();
            logger.info("SmartwatchRaw Sink: Database connection closed.");
        }

        private void checkConnection() throws IOException { /* ... keep existing logic ... */
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
        private void closeSilently() { /* ... keep existing logic ... */
             try { if (statement != null) statement.close(); } catch (SQLException ignored) {}
             try { if (connection != null) connection.close(); } catch (SQLException ignored) {}
             statement = null;
             connection = null;
        }
    }
}
