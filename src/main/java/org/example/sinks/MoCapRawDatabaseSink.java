package org.example.sinks;
import java.sql.Timestamp;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.example.models.SensorReading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MoCapRawDatabaseSink implements Sink<SensorReading> {
    private static final Logger logger = LoggerFactory.getLogger(MoCapRawDatabaseSink.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;

    public MoCapRawDatabaseSink(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    @Override
    public SinkWriter<SensorReading> createWriter(WriterInitContext context) throws IOException {
        return new MoCapRawDatabaseSinkWriter(jdbcUrl, username, password);
    }

    // Old method signature (for backward compatibility) do not remove it (for backward compatibility)
    @Override
    @Deprecated
    public SinkWriter<SensorReading> createWriter(InitContext context) throws IOException {
        return new MoCapRawDatabaseSinkWriter(jdbcUrl, username, password);
    }

    private static class MoCapRawDatabaseSinkWriter implements SinkWriter<SensorReading> {
        private static final String INSERT_SQL =
        "INSERT INTO rokoko_joint_angles (" +
        "thingid, elbow_flex_ext_left, elbow_flex_ext_right, " +
        "shoulder_flex_ext_left, shoulder_flex_ext_right, " +
        "shoulder_abd_add_left, shoulder_abd_add_right, " +
        "lowerarm_pron_sup_left, lowerarm_pron_sup_right, " +
        "upperarm_rotation_left, upperarm_rotation_right, " +
        "hand_flex_ext_left, hand_flex_ext_right, " +
        "hand_radial_ulnar_left, hand_radial_ulnar_right, " +
        "neck_flex_ext, neck_torsion, head_tilt, torso_tilt, " +
        "torso_side_tilt, back_curve, back_torsion, " +
        "knee_flex_ext_left, knee_flex_ext_right, " +
        "timestamp" +
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    

        private final Connection connection;
        private final PreparedStatement statement;

        public MoCapRawDatabaseSinkWriter(String jdbcUrl, String username, String password) throws IOException {
            try {
                connection = DriverManager.getConnection(jdbcUrl, username, password);
                statement = connection.prepareStatement(INSERT_SQL);
                logger.info("#######################################################################################");
                logger.info("####################### Successfully connected to the database. #######################");
                logger.info("#######################################################################################");
            } catch (SQLException e) {
                logger.error("Failed to establish JDBC connection", e);
                throw new IOException("Failed to establish JDBC connection", e);
            }
        }

        @Override
        public void write(SensorReading record, Context context) throws IOException {
            try {
                statement.setString(1, record.getThingid());
                statement.setDouble(2, record.getElbowFlexExtLeft());
                statement.setDouble(3, record.getElbowFlexExtRight());
                statement.setDouble(4, record.getShoulderFlexExtLeft());
                statement.setDouble(5, record.getShoulderFlexExtRight());
                statement.setDouble(6, record.getShoulderAbdAddLeft());
                statement.setDouble(7, record.getShoulderAbdAddRight());
                statement.setDouble(8, record.getLowerarmPronSupLeft());
                statement.setDouble(9, record.getLowerarmPronSupRight());
                statement.setDouble(10, record.getUpperarmRotationLeft());
                statement.setDouble(11, record.getUpperarmRotationRight());
                statement.setDouble(12, record.getHandFlexExtLeft());
                statement.setDouble(13, record.getHandFlexExtRight());
                statement.setDouble(14, record.getHandRadialUlnarLeft());
                statement.setDouble(15, record.getHandRadialUlnarRight());
                statement.setDouble(16, record.getNeckFlexExt());
                statement.setDouble(17, record.getNeckTorsion());
                statement.setDouble(18, record.getHeadTilt());
                statement.setDouble(19, record.getTorsoTilt());
                statement.setDouble(20, record.getTorsoSideTilt());
                statement.setDouble(21, record.getBackCurve());
                statement.setDouble(22, record.getBackTorsion());
                statement.setDouble(23, record.getKneeFlexExtLeft());
                statement.setDouble(24, record.getKneeFlexExtRight());

                Timestamp sqlTimestamp = Timestamp.valueOf(record.getTimestamp());
                statement.setTimestamp(25, sqlTimestamp);

                statement.executeUpdate();
                logger.info("#######################################################################################");
                logger.debug("Inserted record into database: {}", record);
                logger.info("#######################################################################################");
            } catch (SQLException e) {
                logger.error("Error inserting data into database: {}", record, e);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException {
            // No-op, as each record is inserted immediately
        }

        @Override
        public void close() throws IOException {
            try {
                if (statement != null) statement.close();
                if (connection != null) connection.close();
                logger.info("Database connection closed successfully.");
            } catch (SQLException e) {
                logger.error("Error closing JDBC connection", e);
            }
        }
    }
}

