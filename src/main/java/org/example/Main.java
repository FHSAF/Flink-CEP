package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource; // Import if needed by Source Providers
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.time.Duration;

// Import Models
import org.example.models.EMGSensorReading;
import org.example.models.EyeGazeSensorReading;
import org.example.models.SensorReading;
import org.example.models.SmartwatchSensorReading;

// Import Sources
import org.example.sources.*;
import org.example.processing.DataParser;
// Import Processors
import org.example.processing.EMGProcessing;
import org.example.processing.ErgonomicsProcessor;
import org.example.processing.EyeGazeProcessing;
import org.example.processing.RebaMapper;
import org.example.processing.SmartwatchCEPProcessor;
import org.example.processing.SmartwatchSensorReadingParser;
import org.example.processing.SmartwatchSlidingWindowProcessor;

// Import Sinks
import org.example.sinks.*;

// Import DBConfig
import org.example.config.DBConfig;
import org.example.config.KafkaConfig;
// Import KafkaConfig IF your source/sink providers require constants from it
// import org.example.config.KafkaConfig;

// Other imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Set;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    // --- Configuration Variables ---
    // Flink Cluster
    private static final String FLINK_MASTER_HOST = "192.168.50.106";
    private static final int FLINK_MASTER_PORT = 8081;
    private static final String JAR_PATH = "/home/rootr2/development/new/cep_pipeline_java/target/HRC-CEP-1.0-SNAPSHOT.jar";

    // Kafka Brokers - Needed for Source AND Sink Providers
    private static final String KAFKA_BROKERS = KafkaConfig.BOOTSTRAP_SERVERS; 

    // Kafka Topics (Defined locally - or use KafkaConfig if preferred AND providers support it)
    // !!! REPLACE with your actual topic names !!!

    // EMG Topics are handled inside EMGSourceProvider
    private static final String AVG_ANGLE_ALERT_TOPIC = "km_average_angle_alerts";
    private static final String REBA_SCORE_TOPIC = "km_reba_scores_json";
    private static final String EMG_FATIGUE_ALERT_TOPIC = "km_emg_fatigue_alerts";
    private static final String ECG_ALERT_TOPIC = "km_ecg_cep_alerts";
    private static final String ECG_AVG_ALERT_TOPIC = "km_ecg_avg_alerts";

    // Consumer Groups
    private static final String EMG_GROUP_ID = "flink-emg-processor-group";

    // Processing Params
    private static final Set<String> MUSCLES_TO_MONITOR = Set.of(
            "trapezius_left", "trapezius_right", "deltoids_left", "deltoids_right",
            "wrist_extensors_right", "wrist_flexor_right"
    );
    private static final double REBA_TASK_LOAD_KG = 1.0; // VERIFY
    private static final int REBA_COUPLING_SCORE = 0; // VERIFY (0-3)
    private static final int REBA_ACTIVITY_SCORE = 1; // VERIFY (0 or 1)
    // --- End Configuration Variables ---


    public static void main(String[] args) {

        logger.info("#############################################");
        logger.info("Setting up Flink Remote Environment...");
        // ... logging ...
        logger.info("#############################################");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
                FLINK_MASTER_HOST, FLINK_MASTER_PORT, JAR_PATH);

        // env.getCheckpointConfig().disableCheckpointing();

        try {
            logger.info("#############################################");
            logger.info("Building Flink Data Pipeline...");
            logger.info("#############################################");

            KafkaSource<String> rokokoStringSource = MoCapSourceProvider.getKafkaSource(); // Gets configured KafkaSource<String>
            DataStream<String> rawRokokoStringStream = env.fromSource(rokokoStringSource, WatermarkStrategy.noWatermarks(), "RokokoKafkaSource");
            // Parse AFTER adding source
            DataStream<SensorReading> rawSensorStream = rawRokokoStringStream
                .map(DataParser::parseSensorReading) // Assuming DataParser exists
                .filter(value -> value != null); // Filter nulls from parser
            // Apply final filter
            DataStream<SensorReading> sensorStream = rawSensorStream.filter(sensor -> sensor.getThingid() != null && !sensor.getThingid().isEmpty());


            // --- Source: Smartwatch Data ---
            KafkaSource<String> smartwatchStringSource = SmartwatchSourceProvider.getSmartwatchSource(); // Gets configured KafkaSource<String>
            DataStream<String> rawSmartwatchStringStream = env.fromSource(smartwatchStringSource, WatermarkStrategy.noWatermarks(), "SmartwatchKafkaSource");
            // Parse AFTER adding source
            DataStream<SmartwatchSensorReading> smartwatchRawSensorStream = rawSmartwatchStringStream
                .map(SmartwatchSensorReadingParser::parse) // Assuming parser exists
                .filter(value -> value != null); // Filter nulls from parser
            // Apply final filter
            DataStream<SmartwatchSensorReading> smartwatchSensorStream = smartwatchRawSensorStream.filter(sensor -> sensor.getThingid() != null && !sensor.getThingid().isEmpty());

            // // --- Source: EMG Data ---
            // // CORRECTED: Call the existing method with arguments
            KafkaSource<EMGSensorReading> emgKafkaSource = EMGSourceProvider.getEMGKafkaSource(KAFKA_BROKERS, EMG_GROUP_ID);
            DataStream<EMGSensorReading> rawEmgStream = env.fromSource(emgKafkaSource, WatermarkStrategy.noWatermarks(), "EMGKafkaSource");
            DataStream<EMGSensorReading> emgSensorStream = rawEmgStream.filter(sensor -> sensor != null && sensor.getThingid() != null && !sensor.getThingid().isEmpty());


            // --- Processing ---
            logger.info("Configuring Processing Steps...");
            DataStream<String> averageAngleAlerts = ErgonomicsProcessor.processAverageAnglesForFeedback(sensorStream);
            DataStream<String> rebaScoreJsonStream = sensorStream
                .map(new RebaMapper(REBA_TASK_LOAD_KG, REBA_COUPLING_SCORE, REBA_ACTIVITY_SCORE))
                .filter(json -> json != null);
            DataStream<String> emgFatigueAlerts = EMGProcessing.processEMGFatigue(emgSensorStream, MUSCLES_TO_MONITOR);
            DataStream<String> smartwatchAlerts = SmartwatchCEPProcessor.applyCEP(smartwatchSensorStream);
            DataStream<String> smartwatchWindowAlerts = SmartwatchSlidingWindowProcessor.applySlidingWindowAlerts(smartwatchSensorStream);

            // --- Sinks ---
            logger.info("Configuring Sinks...");

            // Define DB connection details using DBConfig
            String dbUrlBase = DBConfig.DB_URL;
            String dbUser = DBConfig.DB_USER;
            String dbPassword = DBConfig.DB_PASSWORD;

            // Sink Average Angle Alerts (Kafka + DB using DBConfig)
            averageAngleAlerts.print("AVG_ANGLE_ALERT");
            // CORRECTED: Pass arguments required by sink provider
            averageAngleAlerts.sinkTo(MoCapKafkaSink.getKafkaSink(KAFKA_BROKERS, AVG_ANGLE_ALERT_TOPIC));
            averageAngleAlerts.sinkTo(new MoCapAverageAngleAlertDatabaseSink(dbUrlBase + DBConfig.ROKOKO_AVERAGE_DB_NAME, dbUser, dbPassword));

            // Sink REBA Scores (Kafka + DB using DBConfig)
            rebaScoreJsonStream.print("REBA_SCORE_JSON");
            // CORRECTED: Pass arguments required by sink provider
            rebaScoreJsonStream.sinkTo(MoCapKafkaSink.getKafkaSink(KAFKA_BROKERS, REBA_SCORE_TOPIC));
            rebaScoreJsonStream.sinkTo(new MoCapRebaScoreDatabaseSink(dbUrlBase + DBConfig.ROKOKO_REBA_SCORE_DB_NAME, dbUser, dbPassword));

            // Sink Smartwatch Alerts (to Kafka)
            smartwatchAlerts.print("ECG_ALERT");
            // CORRECTED: Pass arguments required by sink provider
            smartwatchAlerts.sinkTo(SmartWatchAlertSink.getKafkaSink(KAFKA_BROKERS, ECG_ALERT_TOPIC));
            smartwatchWindowAlerts.print("ECG_AVG_ALERT");
            // CORRECTED: Pass arguments required by sink provider
            smartwatchWindowAlerts.sinkTo(SlidingWindowAlertSinkECG.getKafkaSink(KAFKA_BROKERS, ECG_AVG_ALERT_TOPIC));

            // Sink Filtered Raw Data to DB (Using DBConfig)
            logger.info("Configuring Raw Data Sinks...");
            sensorStream.sinkTo(new MoCapRawDatabaseSink(dbUrlBase + DBConfig.ROKOKO_DB_NAME, dbUser, dbPassword));
            smartwatchSensorStream.sinkTo(new SmartWatchDatabaseSink(dbUrlBase + DBConfig.SMARTWATCH_DB_NAME, dbUser, dbPassword));

            logger.info("Configuring Raw EMG Data Sink...");
            emgFatigueAlerts.print("EMG_FATIGUE_ALERT");
            emgFatigueAlerts.sinkTo(EMGKafkaSink.getKafkaSink(KAFKA_BROKERS, EMG_FATIGUE_ALERT_TOPIC));
            emgSensorStream.sinkTo(new EMGRawDatabaseSink( // Use the new Sink
                                  DBConfig.DB_URL,        // Base URL
                                  DBConfig.EMG_DB_NAME,   // Database Name
                                  DBConfig.DB_USER,
                                  DBConfig.DB_PASSWORD))
                           .name("RawEMGDbSink");
            // EMG Alerts to Kafka

            // --- Source: Eye Gaze Attention ---
            String gazeAttentionTopic = KafkaConfig.EYE_GAZE_TOPIC;
            String gazeGroupId = "flink-gaze-processor-group";
            KafkaSource<EyeGazeSensorReading> gazeSource = EyeGazeSourceProvider.getEyeGazeKafkaSource(KAFKA_BROKERS, gazeAttentionTopic, gazeGroupId);
            DataStream<EyeGazeSensorReading> rawGazeStream = env.fromSource(gazeSource, WatermarkStrategy.noWatermarks(), "GazeAttentionKafkaSource");
            DataStream<EyeGazeSensorReading> gazeStream = rawGazeStream.filter(g -> g != null && g.getThingid() != null && g.getTimestamp() != null);

            // --- Processing: Eye Gaze Inattention (Both Types) ---
            Duration durationThreshold = Duration.ofSeconds(5); // Threshold for continuous inattention
            // The average threshold (e.g., 50%) is configured inside EyeGazeProcessing for now
            DataStream<String> gazeAlerts = EyeGazeProcessing.processGazeAttention(gazeStream, durationThreshold);

            // --- Sink: Eye Gaze Alerts ---
            gazeAlerts.print("GAZE_ALERT"); // Will print both types of alerts
            String gazeAlertTopic = "gaze_alerts"; // Single topic for both alert types, or use separate sinks
            gazeAlerts.sinkTo(EyeGazeKafkaSink.getKafkaSink(KAFKA_BROKERS, gazeAlertTopic))
                      .name("GazeAlertKafkaSink");



            logger.info("#############################################");
            logger.info("Flink pipeline built successfully.");
            logger.info("#############################################");

            // Execute Flink job
            logger.info("Starting Flink job execution...");
            env.execute("HRC Real-time Ergonomics & Fatigue Pipeline");
            logger.info("#############################################");
            logger.info("Flink job submitted successfully (check Flink UI for status)");
            logger.info("#############################################");

        } catch (Exception e) {
            logger.error("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
            logger.error("An error occurred while building or executing the Flink job: ", e);
            logger.error("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
             System.exit(1);
        }
    }
}