package org.example.sources;

// Import removed: org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.connector.kafka.source.KafkaSource; // Correct import
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.example.config.KafkaConfig; // Using config for topics/brokers
// Import removed: org.example.models.SmartwatchSensorReading;
// Import removed: org.example.processing.SmartwatchSensorReadingParser;
// Import removed: org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
// Import removed: org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SmartwatchSourceProvider {
    private static final Logger logger = LoggerFactory.getLogger(SmartwatchSourceProvider.class);

     /**
     * Creates and configures a KafkaSource for reading raw JSON strings
     * for Smartwatch sensor data. Assumes configuration is in KafkaConfig.
     *
     * @return Configured KafkaSource<String>
     * @throws IllegalStateException if required KafkaConfig values are missing.
     */
    public static KafkaSource<String> getSmartwatchSource() { // Removed env parameter
        try {
            logger.info("========= Initializing Smartwatch Kafka Source Configuration =========");
            logger.info("  Brokers: {}", KafkaConfig.BOOTSTRAP_SERVERS);
            logger.info("  Topic: {}", KafkaConfig.SMARTWATCH_TOPIC01_SOURCE); // Assuming this constant exists
            logger.info("  Group ID: {}", KafkaConfig.GROUP_ID + "-smartwatch");
            logger.info("======================================================================");


            if (KafkaConfig.BOOTSTRAP_SERVERS == null || KafkaConfig.SMARTWATCH_TOPIC01_SOURCE == null || KafkaConfig.GROUP_ID == null) {
                 throw new IllegalStateException("Kafka configuration (servers, smartwatch topic, group id) missing in KafkaConfig.");
            }

            // Consumer group ID specific to this source
            String groupId = KafkaConfig.GROUP_ID + "-smartwatch";

            KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                    .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVERS)
                    .setTopics(KafkaConfig.SMARTWATCH_TOPIC01_SOURCE) // Assuming single topic
                    .setGroupId(groupId)
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setValueOnlyDeserializer(new SimpleStringSchema()) // Reads raw JSON String
                    .build();

            logger.info("Smartwatch Kafka Source configured successfully.");
            return kafkaSource; // Return the configured source object

        } catch (Exception e) {
            logger.error("Error initializing Smartwatch Kafka Source", e);
             throw new RuntimeException("Error initializing Smartwatch Kafka Source", e);
        }
         // Removed: env.fromSource(...) and .map(...) calls
    }
}