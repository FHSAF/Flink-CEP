package org.example.sources;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
// Removed Collector and ConsumerRecord imports as they are now in the separate deserializer
import org.example.models.EMGSensorReading; // Import your POJO
import org.example.config.KafkaConfig; // Import your config class
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Removed Gson/JsonSyntaxException imports as they are now in the separate deserializer
// Removed Serializable import
// Removed StandardCharsets import
import java.util.Arrays;
import java.util.List;

// Import the separate deserializer class (adjust package if needed)
import org.example.sources.deserializers.EMGDeserializationSchema;

/**
 * Provides a configured Flink KafkaSource for consuming EMG sensor readings
 * from multiple specified topics (defined in KafkaConfig).
 * Uses an external DeserializationSchema.
 */
public class EMGSourceProvider {

    private static final Logger logger = LoggerFactory.getLogger(EMGSourceProvider.class);

    /**
     * Gets a configured KafkaSource for EMG data using topics from KafkaConfig.
     *
     * @param brokers Comma-separated list of Kafka broker addresses (e.g., "host1:9092,host2:9092").
     * @param groupId Kafka consumer group ID for this source instance.
     * @return A KafkaSource<EMGSensorReading>
     * @throws IllegalArgumentException if brokers or groupId are null or empty, or topics are not configured.
     * @throws IllegalStateException if required KafkaConfig values are missing.
     */
    public static KafkaSource<EMGSensorReading> getEMGKafkaSource(String brokers, String groupId) {
        if (brokers == null || brokers.isEmpty()) {
            throw new IllegalArgumentException("Kafka bootstrap servers cannot be null or empty.");
        }
        if (groupId == null || groupId.isEmpty()) {
            throw new IllegalArgumentException("Kafka consumer group ID cannot be null or empty.");
        }

        // --- Use topics from KafkaConfig ---
        // Perform null checks for required config values
        if (KafkaConfig.BOOTSTRAP_SERVERS == null || KafkaConfig.EMG_TOPIC_LEFT_ARM == null ||
            KafkaConfig.EMG_TOPIC_RIGHT_ARM == null || KafkaConfig.EMG_TOPIC_TRUNK == null) {
             throw new IllegalStateException("Required Kafka configuration (servers, EMG topics) missing in KafkaConfig class.");
        }
        List<String> topics = Arrays.asList(
                KafkaConfig.EMG_TOPIC_LEFT_ARM,
                KafkaConfig.EMG_TOPIC_RIGHT_ARM,
                KafkaConfig.EMG_TOPIC_TRUNK
        );

        logger.info("Configuring EMG Kafka Source:");
        logger.info("  Brokers: {}", brokers);
        logger.info("  Topics: {}", topics);
        logger.info("  Group ID: {}", groupId);

        try {
            KafkaSource<EMGSensorReading> source = KafkaSource.<EMGSensorReading>builder()
                    .setBootstrapServers(brokers) // Use brokers passed as argument
                    .setTopics(topics) // Use the list derived from KafkaConfig
                    .setGroupId(groupId) // Use argument
                    .setStartingOffsets(OffsetsInitializer.latest())
                    // Use the external, top-level deserializer class
                    .setDeserializer(new EMGDeserializationSchema())
                    .build();
            logger.info("EMG Kafka Source configured successfully.");
            return source;
        } catch (Exception e) {
            logger.error("Failed to build EMG Kafka Source", e);
            throw new RuntimeException("Failed to build EMG Kafka Source", e);
        }
    }

    // --- Inner Class EMGDeserializationSchema REMOVED ---
    // --- Its content is now in a separate file ---

}
