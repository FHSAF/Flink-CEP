package org.example.sinks;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee; // Correct import
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
// Removed: import org.example.config.KafkaConfig; // No longer needed here
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Provides configuration for a Flink Kafka Sink specifically targeted
 * for publishing ECG Sliding Window Alert JSON strings to a specified topic.
 */
public class SlidingWindowAlertSinkECG {
    // Corrected logger name to match class name
    private static final Logger logger = LoggerFactory.getLogger(SlidingWindowAlertSinkECG.class);

    /**
     * Creates a configured KafkaSink for ECG Sliding Window alerts for the specified topic and brokers.
     *
     * @param brokers The comma-separated list of Kafka broker addresses.
     * @param topic   The target Kafka topic name for ECG sliding window alerts.
     * @return A configured KafkaSink<String>.
     * @throws IllegalArgumentException if brokers or topic are null or empty.
     */
    public static KafkaSink<String> getKafkaSink(String brokers, String topic) { // Accepts arguments
        if (brokers == null || brokers.isEmpty()) {
            throw new IllegalArgumentException("Kafka bootstrap servers cannot be null or empty.");
        }
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("Target Kafka topic cannot be null or empty.");
        }

        logger.info("Configuring ECG Sliding Window Alert Kafka Sink:");
        logger.info("  Brokers: {}", brokers);
        logger.info("  Topic: {}", topic);

        try {
            KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                    .setBootstrapServers(brokers) // Use argument
                    .setKafkaProducerConfig(getKafkaProperties()) // Apply common producer props
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic(topic) // Use argument
                            .setValueSerializationSchema(new SimpleStringSchema()) // Input is String
                            .build()
                    )
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE) // Set delivery guarantee
                    .build();

            logger.info("Successfully created ECG Sliding Window Alert Kafka Sink for topic '{}'", topic);
            return kafkaSink;

        } catch (Exception e) {
            logger.error("Error creating ECG Sliding Window Alert Kafka Sink for topic '{}': ", topic, e);
            // Propagate exception to fail job startup if sink cannot be configured
            throw new RuntimeException("Error creating ECG Sliding Window Alert Kafka Sink", e);
        }
    }

    /**
     * Configures basic Kafka Producer properties for reliability.
     * @return Properties object for Kafka producer.
     */
    private static Properties getKafkaProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        // Add other common properties if needed
        return properties;
    }

    // Removed addKafkaSink(DataStream<String> stream) method
}