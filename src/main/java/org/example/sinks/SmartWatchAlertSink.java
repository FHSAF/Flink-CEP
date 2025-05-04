package org.example.sinks;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
// Removed: import org.example.config.KafkaConfig; // No longer needed here
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Provides configuration for a Flink Kafka Sink specifically targeted
 * for publishing Smartwatch Alert (e.g., ECG CEP) JSON strings to a specified topic.
 */
public class SmartWatchAlertSink {
    private static final Logger logger = LoggerFactory.getLogger(SmartWatchAlertSink.class);

    /**
     * Creates a configured KafkaSink for Smartwatch alerts for the specified topic and brokers.
     *
     * @param brokers The comma-separated list of Kafka broker addresses.
     * @param topic   The target Kafka topic name for Smartwatch alerts.
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

        logger.info("Configuring Smartwatch Alert Kafka Sink:");
        logger.info("  Brokers: {}", brokers);
        logger.info("  Topic: {}", topic);

        try {
            KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                    .setBootstrapServers(brokers) // Use argument
                    .setKafkaProducerConfig(getKafkaProperties())
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic(topic) // Use argument
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build()
                    )
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();

            logger.info("Successfully created Smartwatch Alert Kafka Sink for topic '{}'", topic);
            return kafkaSink;

        } catch (Exception e) {
            logger.error("Error creating Smartwatch Alert Kafka Sink for topic '{}': ", topic, e);
            throw new RuntimeException("Error creating Smartwatch Alert Kafka Sink", e);
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
        return properties;
    }

    // Removed addKafkaSink(DataStream<String> stream) method
}