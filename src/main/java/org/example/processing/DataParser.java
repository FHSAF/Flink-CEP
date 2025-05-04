package org.example.processing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.models.SensorReading; // Ensure correct import
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime; // Import for current time formatting
import java.time.format.DateTimeFormatter; // Import for current time formatting

public class DataParser {
    // Use Flink's shaded Jackson if running within Flink operators,
    // or standard Jackson if this is run outside/before Flink.
    // Assuming standard Jackson here based on original code.
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(DataParser.class);
    // Formatter for defaulting timestamp
    private static final DateTimeFormatter CURRENT_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");


    /**
     * Parses a JSON string into a SensorReading object.
     * Validates essential fields (thingid, timestamp). Defaults timestamp if invalid.
     * Returns null if JSON is invalid or thingid is missing.
     *
     * @param json The JSON string to parse.
     * @return A SensorReading object or null if parsing/validation fails.
     */
    public static SensorReading parseSensorReading(String json) {
        if (json == null || json.trim().isEmpty()) {
            logger.warn("DataParser: Received null or empty JSON string. Skipping.");
            return null;
        }
        try {
            JsonNode node = objectMapper.readTree(json);

            // --- Validate and Extract Essential Fields FIRST ---
            String thingId = getText(node, "thingid", null); // Default to null if missing
            if (thingId == null || thingId.trim().isEmpty()) {
                logger.warn("DataParser: Missing or empty 'thingid' in JSON. Skipping record: {}", json);
                return null; // Cannot proceed without thingId
            }

            String timestampStr = getTextOrDefaultTimestamp(node, "timestamp"); // Get original or default formatted current time

            // --- Extract Numeric Fields (Defaulting to 0.0 on error) ---
            SensorReading reading = new SensorReading(
                    thingId, // Use validated thingId
                    getDouble(node, "elbow_flex_ext_left"),
                    getDouble(node, "elbow_flex_ext_right"),
                    getDouble(node, "shoulder_flex_ext_left"),
                    getDouble(node, "shoulder_flex_ext_right"),
                    getDouble(node, "shoulder_abd_add_left"),
                    getDouble(node, "shoulder_abd_add_right"),
                    getDouble(node, "lowerarm_pron_sup_left"),
                    getDouble(node, "lowerarm_pron_sup_right"),
                    getDouble(node, "upperarm_rotation_left"),
                    getDouble(node, "upperarm_rotation_right"),
                    getDouble(node, "hand_flex_ext_left"),
                    getDouble(node, "hand_flex_ext_right"),
                    getDouble(node, "hand_radial_ulnar_left"),
                    getDouble(node, "hand_radial_ulnar_right"),
                    getDouble(node, "neck_flex_ext"),
                    getDouble(node, "neck_torsion"),
                    getDouble(node, "head_tilt"),
                    getDouble(node, "torso_tilt"),
                    getDouble(node, "torso_side_tilt"),
                    getDouble(node, "back_curve"),
                    getDouble(node, "back_torsion"),
                    getDouble(node, "knee_flex_ext_left"),
                    getDouble(node, "knee_flex_ext_right"),
                    timestampStr // Use original or defaulted timestamp string
            );

            // Optional: Log successful parsing less frequently (e.g., DEBUG level)
            // logger.debug("Successfully parsed sensor reading: {}", reading);

            return reading;

        } catch (Exception e) {
            // Catch JSON parsing errors or other unexpected issues
            logger.error("Error parsing SensorReading JSON: {}", json, e);
            return null; // Return null on any parsing exception
        }
    }

    /**
     * Helper to get a double value from a JsonNode, defaulting to 0.0 on error/missing.
     */
    private static double getDouble(JsonNode node, String field) {
        JsonNode value = node.get(field);
        // Check if node exists, is not null, and is actually a number type
        if (value != null && !value.isNull() && value.isNumber()) {
            return value.asDouble();
        } else {
            if (value == null || value.isNull()) {
                 logger.warn("Numeric field '{}' is missing or null. Defaulting to 0.0", field);
            } else {
                 logger.warn("Numeric field '{}' has invalid type '{}'. Defaulting to 0.0", field, value.getNodeType());
            }
            return 0.0;
        }
    }

    /**
     * Helper to get a text value from a JsonNode, defaulting to a specified value (e.g., null or "").
     */
    private static String getText(JsonNode node, String field, String defaultValue) {
        JsonNode value = node.get(field);
        // Check if node exists, is not null, and is textual
        if (value != null && !value.isNull() && value.isTextual()) {
            String text = value.asText();
            // Optional: Check if the text is empty after extraction
            // if (text.trim().isEmpty()) {
            //     logger.warn("Text field '{}' is empty. Returning default.", field);
            //     return defaultValue;
            // }
            return text;
        } else {
             if (value == null || value.isNull()) {
                 logger.warn("Text field '{}' is missing or null. Returning default value.", field);
            } else {
                 logger.warn("Text field '{}' has invalid type '{}'. Returning default value.", field, value.getNodeType());
            }
            return defaultValue;
        }
    }

     /**
     * Helper specifically for timestamp: gets text value or defaults to current formatted time string.
     */
    private static String getTextOrDefaultTimestamp(JsonNode node, String field) {
        String timestampStr = getText(node, field, null); // Try to get text, default to null

        if (timestampStr == null || timestampStr.trim().isEmpty()) {
            // --- Default to current time formatted string ---
            String defaultTimestamp = LocalDateTime.now().format(CURRENT_TIME_FORMATTER);
            logger.warn("Missing, null, or empty text field '{}'. Defaulting to current formatted time: {}", field, defaultTimestamp);
            return defaultTimestamp;
        }
        // Optional: Add basic format validation here if desired, though full parsing happens later
        // else if (!isValidTimestampFormat(timestampStr)) { ... return defaultTimestamp; }
        else {
            return timestampStr.trim(); // Return trimmed original string
        }
    }

    // Optional: Basic format check (doesn't guarantee validity)
    // private static boolean isValidTimestampFormat(String timestampStr) {
    //    // Simple regex or length check - not a full parse validation
    //    return timestampStr != null && timestampStr.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}");
    // }
}
