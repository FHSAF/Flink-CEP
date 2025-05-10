// File: Flink-CEP/src/main/java/org/example/processing/mocap/MoCapErgonomicsProcessor.java
package org.example.processing.mocap; // New package

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.models.MoCapReading; // Updated model import
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.typeinfo.Types;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.HashMap;

// Renamed from ErgonomicsProcessor
public class MoCapErgonomicsProcessor {
    private static final Logger logger = LoggerFactory.getLogger(MoCapErgonomicsProcessor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // --- Thresholds Map (Keep as is) ---
    private static final Map<String, double[]> thresholds = Map.ofEntries(
            Map.entry("Elbow Flex Left", new double[]{5, 100, 10, 10}),
            Map.entry("Elbow Flex Right", new double[]{5, 100, 10, 10}),
            Map.entry("Shoulder Flex Left", new double[]{-20, 60, 10, 10}),
            Map.entry("Shoulder Flex Right", new double[]{-20, 60, 10, 10}),
            Map.entry("Shoulder Abduction Left", new double[]{-90, 90, 15, 15}),
            Map.entry("Shoulder Abduction Right", new double[]{-90, 90, 15, 15}),
            Map.entry("Lower Arm Pronation Left", new double[]{-45, 45, 10, 10}),
            Map.entry("Lower Arm Pronation Right", new double[]{-45, 45, 10, 10}),
            Map.entry("Upper Arm Rotation Left", new double[]{-30, 30, 5, 5}),
            Map.entry("Upper Arm Rotation Right", new double[]{-30, 30, 5, 5}),
            Map.entry("Wrist Flex Left", new double[]{-15, 15, 5, 5}),
            Map.entry("Wrist Flex Right", new double[]{-15, 15, 5, 5}),
            Map.entry("Wrist Radial/Ulnar Left", new double[]{-15, 15, 5, 5}),
            Map.entry("Wrist Radial/Ulnar Right", new double[]{-15, 15, 5, 5}),
            Map.entry("Neck Flex", new double[]{-10, 10, 5, 5}),
            Map.entry("Neck Torsion", new double[]{-20, 20, 5, 5}),
            Map.entry("Head Tilt", new double[]{-20, 20, 5, 5}),
            Map.entry("Torso Tilt", new double[]{-5, 20, 5, 5}),
            Map.entry("Torso Side Tilt", new double[]{-10, 10, 5, 5}),
            Map.entry("Back Curve", new double[]{-10, 20, 5, 5}),
            Map.entry("Back Torsion", new double[]{-15, 15, 5, 5}),
            Map.entry("Knee Flex Left", new double[]{-60, 60, 10, 10}),
            Map.entry("Knee Flex Right", new double[]{-60, 60, 10, 10})
    );

    // --- Timestamp Parsing (Moved to MoCapDeserializationSchema) ---
    // We assume the input stream now contains MoCapReading with correct timestamp strings

    // --- Wrapper Class for Timestamp Handling (Needed if parsing happens here) ---
    // If parsing is done in Deserializer, this can be simplified or removed if
    // the main processing function takes MoCapReading directly.
    // Let's assume parsing happens in the Deserializer and input is MoCapReading.
    // We still need a way to get the epoch ms timestamp for windowing.
    public static class TimestampedMoCapReading implements Serializable {
        private static final long serialVersionUID = 201L;
        public long timestampMillis;
        public MoCapReading reading;
        public TimestampedMoCapReading() {}
        public TimestampedMoCapReading(long ts, MoCapReading r) { this.timestampMillis = ts; this.reading = r; }
    }

    // --- Timestamp Parser MapFunction (If needed, adapt from ErgonomicsProcessor) ---
     public static class TimestampParser implements MapFunction<MoCapReading, TimestampedMoCapReading> {
        private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
        @Override
        public TimestampedMoCapReading map(MoCapReading value) throws Exception {
            if (value == null || value.getTimestamp() == null) return null;
            try {
                LocalDateTime ldt = LocalDateTime.parse(value.getTimestamp(), FORMATTER);
                long timestampMillis = ldt.toInstant(ZoneOffset.UTC).toEpochMilli(); // Assuming UTC
                return new TimestampedMoCapReading(timestampMillis, value);
            } catch (Exception e) {
                logger.warn("Failed to parse MoCap timestamp string: {}. Skipping record.", value.getTimestamp(), e);
                return null;
            }
        }
    }


    // --- Main Processing Function ---
    // Takes MoCapReading stream as input
    public static DataStream<String> processAverageAnglesForFeedback(DataStream<MoCapReading> sensorStream) {

        // 1. Parse timestamp string to millis and assign watermarks
        DataStream<TimestampedMoCapReading> sensorStreamWithTimestamps = sensorStream
                .map(new TimestampParser()) // Use the parser
                .filter(value -> value != null)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TimestampedMoCapReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.timestampMillis)
                                .withIdleness(Duration.ofMinutes(1))
                );


        // 2. Key the stream
        WindowedStream<TimestampedMoCapReading, String, TimeWindow> windowedStream = sensorStreamWithTimestamps
                .keyBy(tsr -> tsr.reading.getThingid()) // Key by thingid from the wrapped reading
                .window(SlidingEventTimeWindows.of(Duration.ofMinutes(1), Duration.ofSeconds(10)));

        // 3. Calculate Averages over the Window
        DataStream<AverageJointAngles> averageStream = windowedStream
                .aggregate(new AverageAngleAggregator(), new WindowAverageProcessor());

        // 4. Check Averages and Generate JSON Alerts
        DataStream<String> alertStream = averageStream
                .flatMap((AverageJointAngles avgAngles, Collector<String> out) -> {
                    // (Keep the existing flatMap logic from ErgonomicsProcessor)
                     try {
                        ObjectNode alertJson = objectMapper.createObjectNode();
                        alertJson.put("thingId", avgAngles.thingId);
                        alertJson.put("windowEndTimestamp", avgAngles.windowEnd);
                        alertJson.put("feedbackType", "averageAngleAlert"); // Keep type name consistent
                        ArrayNode jointAlerts = alertJson.putArray("alerts");

                        // --- Completed: Check all average angles against thresholds ---
                        checkAverageAndAddAlert(jointAlerts, "Elbow Flex Left", avgAngles.avgElbowFlexExtLeft);
                        checkAverageAndAddAlert(jointAlerts, "Elbow Flex Right", avgAngles.avgElbowFlexExtRight);
                        checkAverageAndAddAlert(jointAlerts, "Shoulder Flex Left", avgAngles.avgShoulderFlexExtLeft);
                        checkAverageAndAddAlert(jointAlerts, "Shoulder Flex Right", avgAngles.avgShoulderFlexExtRight);
                        checkAverageAndAddAlert(jointAlerts, "Shoulder Abduction Left", avgAngles.avgShoulderAbdAddLeft);
                        checkAverageAndAddAlert(jointAlerts, "Shoulder Abduction Right", avgAngles.avgShoulderAbdAddRight);
                        checkAverageAndAddAlert(jointAlerts, "Lower Arm Pronation Left", avgAngles.avgLowerarmPronSupLeft);
                        checkAverageAndAddAlert(jointAlerts, "Lower Arm Pronation Right", avgAngles.avgLowerarmPronSupRight);
                        checkAverageAndAddAlert(jointAlerts, "Upper Arm Rotation Left", avgAngles.avgUpperarmRotationLeft);
                        checkAverageAndAddAlert(jointAlerts, "Upper Arm Rotation Right", avgAngles.avgUpperarmRotationRight);
                        checkAverageAndAddAlert(jointAlerts, "Wrist Flex Left", avgAngles.avgHandFlexExtLeft);
                        checkAverageAndAddAlert(jointAlerts, "Wrist Flex Right", avgAngles.avgHandFlexExtRight);
                        checkAverageAndAddAlert(jointAlerts, "Wrist Radial/Ulnar Left", avgAngles.avgHandRadialUlnarLeft);
                        checkAverageAndAddAlert(jointAlerts, "Wrist Radial/Ulnar Right", avgAngles.avgHandRadialUlnarRight);
                        checkAverageAndAddAlert(jointAlerts, "Neck Flex", avgAngles.avgNeckFlexExt);
                        checkAverageAndAddAlert(jointAlerts, "Neck Torsion", avgAngles.avgNeckTorsion);
                        checkAverageAndAddAlert(jointAlerts, "Head Tilt", avgAngles.avgHeadTilt);
                        checkAverageAndAddAlert(jointAlerts, "Torso Tilt", avgAngles.avgTorsoTilt);
                        checkAverageAndAddAlert(jointAlerts, "Torso Side Tilt", avgAngles.avgTorsoSideTilt);
                        checkAverageAndAddAlert(jointAlerts, "Back Curve", avgAngles.avgBackCurve);
                        checkAverageAndAddAlert(jointAlerts, "Back Torsion", avgAngles.avgBackTorsion);
                        checkAverageAndAddAlert(jointAlerts, "Knee Flex Left", avgAngles.avgKneeFlexExtLeft);
                        checkAverageAndAddAlert(jointAlerts, "Knee Flex Right", avgAngles.avgKneeFlexExtRight);
                        // --- End Completed Check ---

                        if (jointAlerts.size() > 0) {
                           logger.info("Average Angle Alert Generated for {}: {}", avgAngles.thingId, alertJson.toString());
                           out.collect(alertJson.toString());
                        }
                    } catch (Exception e) {
                        logger.error("Error formatting JSON alert for average angles", e);
                    }
                })
                .returns(Types.STRING); // Type hint for lambda

        return alertStream;
    }

    // --- Helper function checkAverageAndAddAlert (Keep as is) ---
     private static void checkAverageAndAddAlert(ArrayNode alertArray, String jointName, double averageValue) {
         if (!thresholds.containsKey(jointName)) { logger.warn("No threshold for {}", jointName); return; }
         double[] limits = thresholds.get(jointName);
         double minNormal = limits[0]; double maxNormal = limits[1];
         double minYellowBuffer = limits[2]; double maxYellowBuffer = limits[3];
         String severity = "GREEN";
         if (Double.isNaN(averageValue)) { severity = "UNKNOWN"; }
         else {
            if (averageValue < minNormal || averageValue > maxNormal) severity = "RED";
            else if (averageValue < (minNormal + minYellowBuffer) || averageValue > (maxNormal - maxYellowBuffer)) severity = "YELLOW";
         }
         if (!severity.equals("GREEN")) {
              ObjectNode jointAlert = objectMapper.createObjectNode();
              jointAlert.put("joint", jointName);
              jointAlert.put("severity", severity);
              if (!severity.equals("UNKNOWN")) { jointAlert.put("averageValue", Math.round(averageValue * 100.0) / 100.0); }
              else { jointAlert.putNull("averageValue"); }
              alertArray.add(jointAlert);
         }
    }

    // --- Completed POJO Class AverageJointAngles ---
    public static class AverageJointAngles implements Serializable {
        private static final long serialVersionUID = 2L; // Different ID
        public String thingId;
        public long windowEnd;
        public double avgElbowFlexExtLeft;
        public double avgElbowFlexExtRight;
        public double avgShoulderFlexExtLeft;
        public double avgShoulderFlexExtRight;
        public double avgShoulderAbdAddLeft;
        public double avgShoulderAbdAddRight;
        public double avgLowerarmPronSupLeft;
        public double avgLowerarmPronSupRight;
        public double avgUpperarmRotationLeft;
        public double avgUpperarmRotationRight;
        public double avgHandFlexExtLeft;
        public double avgHandFlexExtRight;
        public double avgHandRadialUlnarLeft;
        public double avgHandRadialUlnarRight;
        public double avgNeckFlexExt;
        public double avgNeckTorsion;
        public double avgHeadTilt;
        public double avgTorsoTilt;
        public double avgTorsoSideTilt;
        public double avgBackCurve;
        public double avgBackTorsion;
        public double avgKneeFlexExtLeft;
        public double avgKneeFlexExtRight;
        public AverageJointAngles() {}
    }

    // --- Flink Windowing Functions ---
    // Accumulator needs to work with MoCapReading now
    public static class JointAngleAccumulator implements Serializable {
        private static final long serialVersionUID = 3L;
        public Map<String, Tuple2<Double, Long>> sumsAndCounts = new HashMap<>();
        public String thingId = null;

        // Updated to use MoCapReading
        public void add(MoCapReading s) {
             if (thingId == null && s!= null) thingId = s.getThingid();
             if (s == null) return;
             accumulate("Elbow Flex Left", s.getElbowFlexExtLeft());
             accumulate("Elbow Flex Right", s.getElbowFlexExtRight());
             accumulate("Shoulder Flex Left", s.getShoulderFlexExtLeft());
             accumulate("Shoulder Flex Right", s.getShoulderFlexExtRight());
             accumulate("Shoulder Abduction Left", s.getShoulderAbdAddLeft());
             accumulate("Shoulder Abduction Right", s.getShoulderAbdAddRight());
             accumulate("Lower Arm Pronation Left", s.getLowerarmPronSupLeft());
             accumulate("Lower Arm Pronation Right", s.getLowerarmPronSupRight());
             accumulate("Upper Arm Rotation Left", s.getUpperarmRotationLeft());
             accumulate("Upper Arm Rotation Right", s.getUpperarmRotationRight());
             accumulate("Wrist Flex Left", s.getHandFlexExtLeft());
             accumulate("Wrist Flex Right", s.getHandFlexExtRight());
             accumulate("Wrist Radial/Ulnar Left", s.getHandRadialUlnarLeft());
             accumulate("Wrist Radial/Ulnar Right", s.getHandRadialUlnarRight());
             accumulate("Neck Flex", s.getNeckFlexExt());
             accumulate("Neck Torsion", s.getNeckTorsion());
             accumulate("Head Tilt", s.getHeadTilt());
             accumulate("Torso Tilt", s.getTorsoTilt());
             accumulate("Torso Side Tilt", s.getTorsoSideTilt());
             accumulate("Back Curve", s.getBackCurve());
             accumulate("Back Torsion", s.getBackTorsion());
             accumulate("Knee Flex Left", s.getKneeFlexExtLeft());
             accumulate("Knee Flex Right", s.getKneeFlexExtRight());
        }
        private void accumulate(String jointName, double value) {
            Tuple2<Double, Long> current = sumsAndCounts.getOrDefault(jointName, Tuple2.of(0.0, 0L));
            sumsAndCounts.put(jointName, Tuple2.of(current.f0 + value, current.f1 + 1));
        }
        // getResult() method remains the same, returning AverageJointAngles
         public AverageJointAngles getResult() {
            AverageJointAngles result = new AverageJointAngles();
            result.thingId = this.thingId;
            result.avgElbowFlexExtLeft = getAverage("Elbow Flex Left");
            result.avgElbowFlexExtRight = getAverage("Elbow Flex Right");
            result.avgShoulderFlexExtLeft = getAverage("Shoulder Flex Left");
            result.avgShoulderFlexExtRight = getAverage("Shoulder Flex Right");
            result.avgShoulderAbdAddLeft = getAverage("Shoulder Abduction Left");
            result.avgShoulderAbdAddRight = getAverage("Shoulder Abduction Right");
            result.avgLowerarmPronSupLeft = getAverage("Lower Arm Pronation Left");
            result.avgLowerarmPronSupRight = getAverage("Lower Arm Pronation Right");
            result.avgUpperarmRotationLeft = getAverage("Upper Arm Rotation Left");
            result.avgUpperarmRotationRight = getAverage("Upper Arm Rotation Right");
            result.avgHandFlexExtLeft = getAverage("Wrist Flex Left");
            result.avgHandFlexExtRight = getAverage("Wrist Flex Right");
            result.avgHandRadialUlnarLeft = getAverage("Wrist Radial/Ulnar Left");
            result.avgHandRadialUlnarRight = getAverage("Wrist Radial/Ulnar Right");
            result.avgNeckFlexExt = getAverage("Neck Flex");
            result.avgNeckTorsion = getAverage("Neck Torsion");
            result.avgHeadTilt = getAverage("Head Tilt");
            result.avgTorsoTilt = getAverage("Torso Tilt");
            result.avgTorsoSideTilt = getAverage("Torso Side Tilt");
            result.avgBackCurve = getAverage("Back Curve");
            result.avgBackTorsion = getAverage("Back Torsion");
            result.avgKneeFlexExtLeft = getAverage("Knee Flex Left");
            result.avgKneeFlexExtRight = getAverage("Knee Flex Right");
            return result;
         }
        private double getAverage(String jointName) {
             Tuple2<Double, Long> sumCount = sumsAndCounts.get(jointName);
             return (sumCount != null && sumCount.f1 > 0) ? sumCount.f0 / sumCount.f1 : Double.NaN;
         }
    }

    // AggregateFunction needs to take TimestampedMoCapReading
    public static class AverageAngleAggregator implements AggregateFunction<TimestampedMoCapReading, JointAngleAccumulator, AverageJointAngles> {
         @Override public JointAngleAccumulator createAccumulator() { return new JointAngleAccumulator(); }
         // Updated input type
         @Override public JointAngleAccumulator add(TimestampedMoCapReading value, JointAngleAccumulator acc) {
             if (value != null && value.reading != null) { acc.add(value.reading); } return acc;
         }
         @Override public AverageJointAngles getResult(JointAngleAccumulator acc) { return acc.getResult(); }
         @Override public JointAngleAccumulator merge(JointAngleAccumulator a, JointAngleAccumulator b) {
             // (Keep existing merge logic)
              b.sumsAndCounts.forEach((key, valueB) -> {
                a.sumsAndCounts.merge(key, valueB, (valueA, valB) -> Tuple2.of(valueA.f0 + valB.f0, valueA.f1 + valB.f1));
             });
             if (a.thingId == null) a.thingId = b.thingId; return a;
         }
    }

    // WindowAverageProcessor remains the same
    public static class WindowAverageProcessor extends ProcessWindowFunction<AverageJointAngles, AverageJointAngles, String, TimeWindow> {
       @Override
        public void process(String key, Context context, Iterable<AverageJointAngles> averages, Collector<AverageJointAngles> out) {
            AverageJointAngles avg = averages.iterator().next();
            avg.windowEnd = context.window().getEnd(); // Add window end timestamp
            out.collect(avg);
        }
    }
}