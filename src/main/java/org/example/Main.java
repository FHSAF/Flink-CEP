package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.time.Duration;

// Import Refactored Models
import org.example.models.EMGReading;
import org.example.models.EyeGazeReading;
import org.example.models.MoCapReading;
import org.example.models.SmartwatchReading;
// RebaScore model remains in models package
import org.example.models.RebaScore;

// Import Refactored Sources (Providers only needed here)
import org.example.sources.provider.MoCapKafkaSourceProvider;
import org.example.sources.provider.EMGKafkaSourceProvider;
import org.example.sources.provider.SmartwatchKafkaSourceProvider;
import org.example.sources.provider.EyeGazeKafkaSourceProvider;

// Import Refactored Processors (from sub-packages)
import org.example.processing.mocap.MoCapErgonomicsProcessor;
import org.example.processing.mocap.MoCapRebaProcessor; // Contains the MapFunction now
import org.example.processing.emg.EMGFatigueProcessor;
import org.example.processing.eyegaze.EyeGazeAttentionProcessor;
import org.example.processing.smartwatch.SmartwatchAvgHrProcessor;
import org.example.processing.smartwatch.SmartwatchHrvProcessor; // Planned

// Import Refactored Sinks (from sub-packages)
import org.example.sinks.db.MoCapRawDbSink;
import org.example.sinks.db.RebaScoreDbSink;
import org.example.sinks.db.AvgAngleAlertDbSink;
import org.example.sinks.db.EMGRawDbSink;
import org.example.sinks.db.EMGFatigueAlertDbSink;
import org.example.sinks.db.SmartwatchRawDbSink;
import org.example.sinks.db.SmartwatchAvgHrDbSink; // New
import org.example.sinks.db.EyeGazeRawDbSink; // New
import org.example.sinks.db.EyeGazeAttentionAlertDbSink; // New

import org.example.sinks.kafka.MoCapErgonomicsAlertKafkaSink;
import org.example.sinks.kafka.EMGFatigueAlertKafkaSink;
import org.example.sinks.kafka.SmartwatchAvgHrAlertKafkaSink;
import org.example.sinks.kafka.SmartwatchHrvAlertKafkaSink; // Planned
import org.example.sinks.kafka.EyeGazeAlertKafkaSink;


// Import Configs
import org.example.config.DBConfig;
import org.example.config.KafkaConfig;

// Other imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Set;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    // --- Configuration Variables (Keep as defined before) ---
    private static final String FLINK_MASTER_HOST = "192.168.50.106";
    private static final int FLINK_MASTER_PORT = 8081;
    private static final String JAR_PATH = "/home/rootr2/development/new/cep_pipeline_java/target/HRC-CEP-1.0-SNAPSHOT.jar";
    private static final String KAFKA_BROKERS = KafkaConfig.BOOTSTRAP_SERVERS;

    // Kafka Topics (Defined locally or use KafkaConfig)
    // Ergonomics/REBA Alerts
    private static final String MOCAP_AVG_ANGLE_ALERT_TOPIC = "km_average_angle_alerts"; // Example name
    private static final String MOCAP_REBA_SCORE_TOPIC = "km_reba_scores_json"; // Example name
    // Fatigue/Stress Alerts
    private static final String EMG_FATIGUE_ALERT_TOPIC = "km_emg_fatigue_alerts";
    private static final String SMARTWATCH_AVG_HR_ALERT_TOPIC = "km_ecg_avg_alerts"; // For Avg HR
    private static final String SMARTWATCH_HRV_ALERT_TOPIC = "km_hrv_stress_alerts"; // Planned for HRV
    // Attention Alerts
    private static final String EYE_GAZE_ALERT_TOPIC = "km_gaze_alerts"; // Single topic for both gaze alert types

    // Consumer Groups
    private static final String MOCAP_GROUP_ID = KafkaConfig.GROUP_ID; // Use base group ID or specific
    private static final String EMG_GROUP_ID = "flink-emg-processor-group";
    private static final String SMARTWATCH_GROUP_ID = KafkaConfig.SMARTWATCH_GROUP_ID != null ? KafkaConfig.SMARTWATCH_GROUP_ID : KafkaConfig.GROUP_ID + "-smartwatch";
    private static final String EYE_GAZE_GROUP_ID = "flink-gaze-processor-group";


    // Processing Params (Keep as defined before)
    private static final Set<String> MUSCLES_TO_MONITOR = Set.of(
            "trapezius_left", "trapezius_right", "deltoids_left", "deltoids_right",
            "wrist_extensors_right", "wrist_flexor_right"
            // Add other muscles from EMGReading model if needed
    );
    private static final double REBA_TASK_LOAD_KG = 1.0;
    private static final int REBA_COUPLING_SCORE = 0;
    private static final int REBA_ACTIVITY_SCORE = 1;
    private static final Duration EYE_GAZE_DURATION_THRESHOLD = Duration.ofSeconds(15); // Example: Alert if inattentive for 15s


    public static void main(String[] args) {

        logger.info("#############################################");
        logger.info("Setting up Flink Remote Environment...");
        logger.info("  Master: {}:{}", FLINK_MASTER_HOST, FLINK_MASTER_PORT);
        logger.info("  JAR Path: {}", JAR_PATH);
        logger.info("#############################################");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
                FLINK_MASTER_HOST, FLINK_MASTER_PORT, JAR_PATH);

        // Optional: Disable checkpointing for simpler local testing if needed
        // env.getCheckpointConfig().disableCheckpointing();

        try {
            logger.info("#############################################");
            logger.info("Building Flink Data Pipeline...");
            logger.info("#############################################");

            // --- Source: MoCap Data ---
            KafkaSource<MoCapReading> moCapSource = MoCapKafkaSourceProvider.getKafkaSource();
            DataStream<MoCapReading> moCapStream = env.fromSource(moCapSource, WatermarkStrategy.noWatermarks(), "MoCapKafkaSource")
                                                     .filter(value -> value != null && value.getThingid() != null && !value.getThingid().isEmpty())
                                                     .name("FilterValidMoCap");

            // --- Source: Smartwatch Data ---
            KafkaSource<SmartwatchReading> smartwatchSource = SmartwatchKafkaSourceProvider.getSmartwatchSource();
            DataStream<SmartwatchReading> smartwatchStream = env.fromSource(smartwatchSource, WatermarkStrategy.noWatermarks(), "SmartwatchKafkaSource")
                                                                 .filter(value -> value != null && value.getThingid() != null && !value.getThingid().isEmpty())
                                                                 .name("FilterValidSmartwatch");

            // --- Source: EMG Data ---
            KafkaSource<EMGReading> emgSource = EMGKafkaSourceProvider.getEMGKafkaSource(KAFKA_BROKERS, EMG_GROUP_ID);
            DataStream<EMGReading> emgStream = env.fromSource(emgSource, WatermarkStrategy.noWatermarks(), "EMGKafkaSource")
                                                  .filter(value -> value != null && value.getThingid() != null && !value.getThingid().isEmpty())
                                                  .name("FilterValidEMG");


            // --- Source: Eye Gaze Attention ---
            String gazeAttentionTopic = KafkaConfig.EYE_GAZE_TOPIC; // Get topic from config
            KafkaSource<EyeGazeReading> gazeSource = EyeGazeKafkaSourceProvider.getEyeGazeKafkaSource(KAFKA_BROKERS, gazeAttentionTopic, EYE_GAZE_GROUP_ID);
            DataStream<EyeGazeReading> gazeStream = env.fromSource(gazeSource, WatermarkStrategy.noWatermarks(), "GazeAttentionKafkaSource")
                                                       .filter(value -> value != null && value.getThingid() != null && !value.getThingid().isEmpty())
                                                       .name("FilterValidGaze");


            // --- Processing ---
            logger.info("Configuring Processing Steps...");

            // MoCap Processing
            DataStream<String> averageAngleAlerts = MoCapErgonomicsProcessor.processAverageAnglesForFeedback(moCapStream);
            DataStream<String> rebaScoreJsonStream = moCapStream
                .map(new MoCapRebaProcessor.RebaScoreMapFunction(REBA_TASK_LOAD_KG, REBA_COUPLING_SCORE, REBA_ACTIVITY_SCORE)) // Use MapFunction from Processor class
                .filter(json -> json != null)
                .name("CalculateREBAScore");

            // EMG Processing (RMS based fatigue)
            DataStream<String> emgFatigueAlerts = EMGFatigueProcessor.processEMGFatigueRMS(emgStream, MUSCLES_TO_MONITOR);

            // Smartwatch Processing
            DataStream<String> smartwatchAvgHrAlerts = SmartwatchAvgHrProcessor.applySlidingWindowAlerts(smartwatchStream);
            // DataStream<String> smartwatchHrvAlerts = SmartwatchHrvProcessor.processHrv(smartwatchStream); // Planned

            // Eye Gaze Processing
            DataStream<String> gazeAlerts = EyeGazeAttentionProcessor.processGazeAttention(gazeStream, EYE_GAZE_DURATION_THRESHOLD);


            // --- Sinks ---
            logger.info("Configuring Sinks...");

            // Define DB connection details using DBConfig
            String dbUrlBase = DBConfig.DB_URL; // e.g., "jdbc:postgresql://host:port/"
            String dbUser = DBConfig.DB_USER;
            String dbPassword = DBConfig.DB_PASSWORD;

            // --- Kafka Sinks ---
            averageAngleAlerts.print("AVG_ANGLE_ALERT");
            averageAngleAlerts.sinkTo(MoCapErgonomicsAlertKafkaSink.getKafkaSink(KAFKA_BROKERS, MOCAP_AVG_ANGLE_ALERT_TOPIC))
                              .name("AvgAngleAlertKafkaSink");

            rebaScoreJsonStream.print("REBA_SCORE_JSON");
            rebaScoreJsonStream.sinkTo(MoCapErgonomicsAlertKafkaSink.getKafkaSink(KAFKA_BROKERS, MOCAP_REBA_SCORE_TOPIC)) // Can reuse the same sink provider if target is just String
                               .name("RebaScoreKafkaSink");

            emgFatigueAlerts.print("EMG_FATIGUE_ALERT");
            emgFatigueAlerts.sinkTo(EMGFatigueAlertKafkaSink.getKafkaSink(KAFKA_BROKERS, EMG_FATIGUE_ALERT_TOPIC))
                            .name("EMGFatigueAlertKafkaSink");

            smartwatchAvgHrAlerts.print("SMARTWATCH_AVG_HR_ALERT");
            smartwatchAvgHrAlerts.sinkTo(SmartwatchAvgHrAlertKafkaSink.getKafkaSink(KAFKA_BROKERS, SMARTWATCH_AVG_HR_ALERT_TOPIC))
                                 .name("SmartwatchAvgHrAlertKafkaSink");

            // smartwatchHrvAlerts.print("SMARTWATCH_HRV_ALERT"); // Planned
            // smartwatchHrvAlerts.sinkTo(SmartwatchHrvAlertKafkaSink.getKafkaSink(KAFKA_BROKERS, SMARTWATCH_HRV_ALERT_TOPIC))
            //                    .name("SmartwatchHrvAlertKafkaSink"); // Planned

            gazeAlerts.print("GAZE_ALERT");
            gazeAlerts.sinkTo(EyeGazeAlertKafkaSink.getKafkaSink(KAFKA_BROKERS, EYE_GAZE_ALERT_TOPIC))
                      .name("GazeAlertKafkaSink");


            // --- Database Sinks ---
            logger.info("Configuring Database Sinks...");

            // Raw Data Sinks
            moCapStream.sinkTo(new MoCapRawDbSink(dbUrlBase + DBConfig.ROKOKO_DB_NAME, DBConfig.ROKOKO_DB_TABLE, dbUser, dbPassword))
                       .name("RawMoCapDbSink");
            smartwatchStream.sinkTo(new SmartwatchRawDbSink(dbUrlBase + DBConfig.SMARTWATCH_DB_NAME, DBConfig.SMARTWATCH_DB_TABLE, dbUser, dbPassword))
                            .name("RawSmartwatchDbSink");
            emgStream.sinkTo(new EMGRawDbSink(dbUrlBase, DBConfig.EMG_DB_NAME, dbUser, dbPassword)) // Passes base URL and DB name
                     .name("RawEMGDbSink");
            gazeStream.sinkTo(new EyeGazeRawDbSink(dbUrlBase + DBConfig.GAZE_STATE_DB_NAME, DBConfig.GAZE_STATE_TABLE, dbUser, dbPassword)) // Assumes config exists
                      .name("RawEyeGazeDbSink"); // Optional Raw Gaze Sink

            // Processed Data Sinks
            averageAngleAlerts.sinkTo(new AvgAngleAlertDbSink(dbUrlBase + DBConfig.ROKOKO_AVERAGE_DB_NAME, DBConfig.ROKOKO_AVERAGE_DB_TABLE, dbUser, dbPassword))
                              .name("AvgAngleAlertDbSink");
            rebaScoreJsonStream.sinkTo(new RebaScoreDbSink(dbUrlBase + DBConfig.ROKOKO_REBA_SCORE_DB_NAME, DBConfig.ROKOKO_REBA_SCORE_DB_TABLE, dbUser, dbPassword))
                               .name("RebaScoreDbSink");
            emgFatigueAlerts.sinkTo(new EMGFatigueAlertDbSink(dbUrlBase + DBConfig.EMG_AVERAGE_DB_NAME, DBConfig.EMG_AVERAGE_DB_TABLE, dbUser, dbPassword)) // Assuming table name from config
                            .name("EMGFatigueAlertDbSink");
            // smartwatchAvgHrAlerts.sinkTo(new SmartwatchAvgHrDbSink(dbUrlBase + DBConfig.SMARTWATCH_AVERAGE_DB_NAME, DBConfig.SMARTWATCH_AVERAGE_DB_TABLE, dbUser, dbPassword)) // Assumes config exists
            //                      .name("SmartwatchAvgHrDbSink"); // Optional Avg HR Sink
            gazeAlerts.sinkTo(new EyeGazeAttentionAlertDbSink(dbUrlBase + DBConfig.GAZE_STATE_DB_NAME, DBConfig.GAZE_STATE_ALERT_TABLE, dbUser, dbPassword)) // Assumes config exists
                      .name("EyeGazeAlertDbSink"); // Optional Gaze Alert Sink


            logger.info("#############################################");
            logger.info("Flink pipeline built successfully.");
            logger.info("#############################################");

            // Execute Flink job
            logger.info("Starting Flink job execution...");
            env.execute("HRC Real-time Monitoring Pipeline (Refactored)");
            logger.info("#############################################");
            logger.info("Flink job submitted successfully (check Flink UI for status)");
            logger.info("#############################################");

        } catch (Exception e) {
            logger.error("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
            logger.error("An error occurred while building or executing the Flink job: ", e);
            logger.error("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
             System.exit(1); // Exit if setup fails
        }
    }
}
