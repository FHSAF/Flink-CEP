package org.example.sources;

// Import removed: org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.connector.kafka.source.KafkaSource; // Correct import
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.example.config.KafkaConfig; // Using config for topics/brokers
// Import removed: org.example.processing.DataParser;
// Import removed: org.example.models.SensorReading;
// Import removed: org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
// Import removed: org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MoCapSourceProvider {
    private static final Logger logger = LoggerFactory.getLogger(MoCapSourceProvider.class);

    /**
     * Creates and configures a KafkaSource for reading raw JSON strings
     * for Rokoko sensor data. Assumes configuration is in KafkaConfig.
     *
     * @return Configured KafkaSource<String>
     * @throws IllegalStateException if required KafkaConfig values are missing.
     */
    public static KafkaSource<String> getKafkaSource() { // Removed env parameter
        try {
            logger.info("#############################################");
            logger.info("Initializing Rokoko Kafka Source Configuration...");
            logger.info("  Brokers: {}", KafkaConfig.BOOTSTRAP_SERVERS);
            logger.info("  Topic: {}", KafkaConfig.ROKOKO_TOPIC_SOURCE);
            logger.info("  Group ID: {}", KafkaConfig.GROUP_ID);
            logger.info("#############################################");

            if (KafkaConfig.BOOTSTRAP_SERVERS == null || KafkaConfig.ROKOKO_TOPIC_SOURCE == null || KafkaConfig.GROUP_ID == null) {
                throw new IllegalStateException("Kafka configuration (servers, rokoko topic, group id) missing in KafkaConfig.");
            }

            KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                    .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVERS)
                    .setTopics(KafkaConfig.ROKOKO_TOPIC_SOURCE) // Assuming single topic for Rokoko
                    .setGroupId(KafkaConfig.GROUP_ID)          // Use base group ID or a specific one
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setValueOnlyDeserializer(new SimpleStringSchema()) // Reads raw JSON String
                    .build();

            logger.info("Rokoko Kafka Source configured successfully.");
            return kafkaSource; // Return the configured source object

        } catch (Exception e) {
            logger.error("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
            logger.error("Error initializing Rokoko Kafka Source", e);
            logger.error("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
            // Propagate exception to fail job startup if source cannot be configured
            throw new RuntimeException("Error initializing Rokoko Kafka Source", e);
        }
        // Removed: env.fromSource(...) and .map(...) calls
    }
}