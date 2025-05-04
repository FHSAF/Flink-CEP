package org.example.sources;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.example.models.EyeGazeSensorReading; // Import the POJO
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Import the separate Deserialization Schema class
import org.example.sources.deserializers.EyeGazeDeserializationSchema; // Adjust package if necessary

/**
 * Provides a configured Flink KafkaSource for consuming eye gaze attention state
 * messages, using an external deserialization schema.
 */
public class EyeGazeSourceProvider {

    private static final Logger logger = LoggerFactory.getLogger(EyeGazeSourceProvider.class);

    /**
     * Gets a configured KafkaSource for Eye Gaze Attention data.
     *
     * @param brokers Comma-separated list of Kafka broker addresses (e.g., "host1:9092").
     * @param topic   The Kafka topic containing the gaze attention JSON messages.
     * @param groupId Kafka consumer group ID for this source instance.
     * @return A KafkaSource<EyeGazeSensorReading>
     * @throws IllegalArgumentException if brokers, topic, or groupId are null or empty.
     */
    public static KafkaSource<EyeGazeSensorReading> getEyeGazeKafkaSource(String brokers, String topic, String groupId) {
        // Validate input arguments
        if (brokers == null || brokers.isEmpty()) {
            throw new IllegalArgumentException("Kafka bootstrap servers cannot be null or empty for EyeGazeSourceProvider.");
        }
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("Kafka topic cannot be null or empty for EyeGazeSourceProvider.");
        }
        if (groupId == null || groupId.isEmpty()) {
            throw new IllegalArgumentException("Kafka consumer group ID cannot be null or empty for EyeGazeSourceProvider.");
        }

        logger.info("Configuring Eye Gaze Kafka Source:");
        logger.info("  Brokers: {}", brokers);
        logger.info("  Topic: {}", topic);
        logger.info("  Group ID: {}", groupId);

        try {
            // Build the KafkaSource using the provided arguments and the external deserializer
            KafkaSource<EyeGazeSensorReading> source = KafkaSource.<EyeGazeSensorReading>builder()
                    .setBootstrapServers(brokers)
                    .setTopics(topic) // Use the provided topic
                    .setGroupId(groupId) // Use the provided group ID
                    .setStartingOffsets(OffsetsInitializer.latest()) // Or .earliest() if needed
                    // Instantiate the external deserializer class
                    .setDeserializer(new EyeGazeDeserializationSchema())
                    .build();

            logger.info("Eye Gaze Kafka Source configured successfully for topic '{}'.", topic);
            return source;

        } catch (Exception e) {
            // Log and re-throw exceptions during source creation
            logger.error("Failed to build Eye Gaze Kafka Source for topic '{}'", topic, e);
            throw new RuntimeException("Failed to build Eye Gaze Kafka Source", e);
        }
    }
}
