package org.example.sinks;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.example.models.SmartwatchSensorReading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;

public class SmartWatchDatabaseSink implements Sink<SmartwatchSensorReading> {
    private static final Logger logger = LoggerFactory.getLogger(SmartWatchDatabaseSink.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;

    public SmartWatchDatabaseSink(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    @Override
    public SinkWriter<SmartwatchSensorReading> createWriter(WriterInitContext context) throws IOException {
        return new SmartwatchSinkWriter(jdbcUrl, username, password);
    }
    // Old method signature (for backward compatibility) do not remove it (for backward compatibility)
    @Override
    @Deprecated
    public SinkWriter<SmartwatchSensorReading> createWriter(InitContext context) throws IOException {
        return new SmartwatchSinkWriter(jdbcUrl, username, password);
    }

    private static class SmartwatchSinkWriter implements SinkWriter<SmartwatchSensorReading> {
        private static final String INSERT_SQL =
            "INSERT INTO heartrate_data (thingid, heartrate, timestamp) VALUES (?, ?, ?)";

        private final Connection connection;
        private final PreparedStatement statement;

        public SmartwatchSinkWriter(String jdbcUrl, String username, String password) throws IOException {
            try {
                connection = DriverManager.getConnection(jdbcUrl, username, password);
                statement = connection.prepareStatement(INSERT_SQL);
                logger.info("Connected to database for Smartwatch sink.");
            } catch (SQLException e) {
                logger.error("Failed to connect to database for Smartwatch sink.", e);
                throw new IOException("JDBC connection failed", e);
            }
        }

        @Override
        public void write(SmartwatchSensorReading record, Context context) throws IOException {
            try {
                statement.setString(1, record.getThingid());
                statement.setInt(2, record.getHeartrate());
                Timestamp timestamp = Timestamp.valueOf(record.getTimestamp());
                statement.setTimestamp(3, timestamp);

                statement.executeUpdate();
                logger.debug("Inserted Smartwatch record: {}", record);
            } catch (SQLException e) {
                logger.error("Error inserting smartwatch data: {}", record, e);
            }
        }

        @Override
        public void flush(boolean endOfInput) {}

        @Override
        public void close() throws IOException {
            try {
                if (statement != null) statement.close();
                if (connection != null) connection.close();
                logger.info("Closed Smartwatch DB connection.");
            } catch (SQLException e) {
                logger.error("Error closing Smartwatch DB connection", e);
            }
        }
    }
}
