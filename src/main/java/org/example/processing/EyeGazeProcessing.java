package org.example.processing;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.models.EyeGazeSensorReading; // Import the POJO
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Processes EyeGazeSensorReading streams to detect periods of inattention based on:
 * 1. Average attention level over a sliding window.
 * 2. Continuous duration of inattention.
 */
public class EyeGazeProcessing {

    private static final Logger logger = LoggerFactory.getLogger(EyeGazeProcessing.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // ISO 8601 format expected from C# DateTime.UtcNow.ToString("o")
    private static final DateTimeFormatter ISO_TIMESTAMP_FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    // Configuration constants (consider making these parameters)
    private static final Duration AVG_ATTENTION_WINDOW_SIZE = Duration.ofMinutes(1);
    private static final Duration AVG_ATTENTION_WINDOW_SLIDE = Duration.ofSeconds(15);
    // Alert if attention is FALSE for more than this percentage of the window duration
    private static final double AVG_INATTENTION_THRESHOLD_PERCENT = 50.0; // e.g., alert if inattentive > 50% of the time

    /**
     * Main processing pipeline for eye gaze analysis.
     *
     * @param gazeStream                 The input stream of EyeGazeSensorReading objects.
     * @param durationInattentionThreshold The continuous duration threshold for the second alert type.
     * @return A DataStream of JSON strings representing both types of inattention alerts.
     */
    public static DataStream<String> processGazeAttention(
            DataStream<EyeGazeSensorReading> gazeStream,
            Duration durationInattentionThreshold) {

        // 1. Parse ISO Timestamp String to Long Milliseconds and assign watermarks
        DataStream<TimestampedGazeReading> timedGazeStream = gazeStream
                .map(new TimestampParserGaze())
                .filter(value -> value != null)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TimestampedGazeReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.timestampMillis)
                                .withIdleness(Duration.ofMinutes(1)) // Handle idle sources
                );

        // --- Branch 1: Average Attention over Sliding Window ---
        WindowedStream<TimestampedGazeReading, String, TimeWindow> avgWindowedStream = timedGazeStream
                .keyBy(tsr -> tsr.reading.getThingid())
                .window(SlidingEventTimeWindows.of(AVG_ATTENTION_WINDOW_SIZE, AVG_ATTENTION_WINDOW_SLIDE));

        DataStream<String> averageInattentionAlerts = avgWindowedStream
                .aggregate(new AttentionAggregator(), new AverageAttentionWindowProcessor(AVG_INATTENTION_THRESHOLD_PERCENT))
                .filter(alert -> alert != null); // Filter out windows where no alert was generated


        // --- Branch 2: Continuous Inattention Duration ---
        DataStream<String> durationInattentionAlerts = timedGazeStream
                .keyBy(tsr -> tsr.reading.getThingid())
                .process(new ProlongedInattentionDetector(durationInattentionThreshold.toMillis()));


        // --- Union the two alert streams ---
        return averageInattentionAlerts.union(durationInattentionAlerts);
    }

    // --- Helper Classes ---

    /** Wrapper for timestamp parsing */
    public static class TimestampedGazeReading implements Serializable {
        private static final long serialVersionUID = 1L;
        public long timestampMillis;
        public EyeGazeSensorReading reading;
        public TimestampedGazeReading() {}
        public TimestampedGazeReading(long ts, EyeGazeSensorReading r) { this.timestampMillis = ts; this.reading = r; }
    }

    /** Parses ISO 8601 timestamp string */
    public static class TimestampParserGaze implements MapFunction<EyeGazeSensorReading, TimestampedGazeReading> {
        @Override
        public TimestampedGazeReading map(EyeGazeSensorReading value) {
            if (value == null || value.getTimestamp() == null) return null;
            try {
                Instant instant = Instant.from(ISO_TIMESTAMP_FORMATTER.parse(value.getTimestamp()));
                long timestampMillis = instant.toEpochMilli();
                return new TimestampedGazeReading(timestampMillis, value);
            } catch (Exception e) { // Catch broader exceptions during parsing
                logger.warn("Gaze Parser: Failed to parse ISO timestamp string: {}. Skipping record.", value.getTimestamp(), e);
                return null;
            }
        }
    }

    // --- Logic for Average Attention Alert ---

    /** Accumulator for counting true/false attention states */
    public static class AttentionAccumulator implements Serializable {
        private static final long serialVersionUID = 1L;
        public long trueCount = 0L;
        public long falseCount = 0L;
        public String thingId = null; // Store key
    }

    /** Aggregates attention counts */
    public static class AttentionAggregator implements AggregateFunction<TimestampedGazeReading, AttentionAccumulator, AttentionAccumulator> {
        @Override public AttentionAccumulator createAccumulator() { return new AttentionAccumulator(); }

        @Override public AttentionAccumulator add(TimestampedGazeReading value, AttentionAccumulator acc) {
            if (acc.thingId == null) acc.thingId = value.reading.getThingid(); // Store key once
            if (value.reading.isAttention()) {
                acc.trueCount++;
            } else {
                acc.falseCount++;
            }
            return acc;
        }

        @Override public AttentionAccumulator getResult(AttentionAccumulator acc) { return acc; } // Pass accumulator through

        @Override public AttentionAccumulator merge(AttentionAccumulator a, AttentionAccumulator b) {
            a.trueCount += b.trueCount;
            a.falseCount += b.falseCount;
            if (a.thingId == null) a.thingId = b.thingId;
            return a;
        }
    }

    /** Processes the window aggregate to generate average attention alerts */
    public static class AverageAttentionWindowProcessor extends ProcessWindowFunction<AttentionAccumulator, String, String, TimeWindow> {
        private static final long serialVersionUID = 1L;
        private final double inattentionThresholdPercent;

        public AverageAttentionWindowProcessor(double inattentionThresholdPercent) {
            this.inattentionThresholdPercent = inattentionThresholdPercent;
        }

        @Override
        public void process(String key, Context context, Iterable<AttentionAccumulator> aggregates, Collector<String> out) {
            AttentionAccumulator acc = aggregates.iterator().next(); // Only one accumulator expected
            long totalCount = acc.trueCount + acc.falseCount;
            // --- ADD DEBUG LOG HERE ---
            logger.debug("Avg Attention Window Triggered: Key={}, WindowEnd={}, TrueCount={}, FalseCount={}, TotalCount={}",
            key, context.window().getEnd(), acc.trueCount, acc.falseCount, totalCount);
            // --- END DEBUG LOG ---

            if (totalCount > 0) {
                double falsePercentage = ((double) acc.falseCount / totalCount) * 100.0;

                logger.trace("Avg Attention Window: Key={}, WindowEnd={}, True={}, False={}, False%={:.2f}",
                             key, context.window().getEnd(), acc.trueCount, acc.falseCount, falsePercentage);

                if (falsePercentage > inattentionThresholdPercent) {
                    // --- Generate Average Inattention JSON Alert ---
                    try {
                        ObjectNode alert = objectMapper.createObjectNode();
                        alert.put("thingId", key);
                        alert.put("windowEndTimestamp", context.window().getEnd());
                        alert.put("feedbackType", "averageInattentionAlert");
                        alert.put("severity", "WARNING"); // Or determine based on percentage?
                        alert.put("reason", String.format("Inattention detected for %.1f%% of the last %d seconds",
                                                          falsePercentage, AVG_ATTENTION_WINDOW_SIZE.toSeconds()));
                        alert.put("falseAttentionPercentage", Math.round(falsePercentage * 10.0) / 10.0);
                        alert.put("thresholdPercentage", inattentionThresholdPercent);
                        alert.put("windowDurationSeconds", AVG_ATTENTION_WINDOW_SIZE.toSeconds());

                        out.collect(alert.toString());
                        logger.warn("Average Inattention Alert Triggered: {}", alert.toString());
                    } catch (Exception e) {
                        logger.error("Error creating average inattention JSON alert for key {}", key, e);
                    }
                }
            }
        }
    }


    // --- Logic for Continuous Duration Inattention Alert ---

    /** Detects when attention=false persists beyond a threshold */
    public static class ProlongedInattentionDetector extends KeyedProcessFunction<String, TimestampedGazeReading, String> {
        private static final long serialVersionUID = 1L;
        private final long inattentionThresholdMillis;

        private transient ValueState<Long> inattentionStartTimeState;
        private transient ValueState<Long> registeredTimerTimestampState;

        public ProlongedInattentionDetector(long inattentionThresholdMillis) {
            this.inattentionThresholdMillis = inattentionThresholdMillis;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            inattentionStartTimeState = getRuntimeContext().getState(new ValueStateDescriptor<>("inattentionStartTime", Types.LONG));
            registeredTimerTimestampState = getRuntimeContext().getState(new ValueStateDescriptor<>("registeredTimerTimestamp", Types.LONG));
            int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            int totalSubtasks = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
            logger.info("Initialized ProlongedInattentionDetector state for subtask {}/{}", subtaskIndex, totalSubtasks);
        }

        @Override
        public void processElement(TimestampedGazeReading value, Context ctx, Collector<String> out) throws Exception {
            boolean isCurrentlyAttentive = value.reading.isAttention();
            long currentTimestamp = value.timestampMillis;
            Long inattentionStartTime = inattentionStartTimeState.value();
            Long registeredTimer = registeredTimerTimestampState.value();

            logger.trace("Processing Gaze Duration: Key={}, Timestamp={}, Attention={}, StartTimeState={}, TimerState={}",
                ctx.getCurrentKey(), currentTimestamp, isCurrentlyAttentive, inattentionStartTime, registeredTimer);

            if (!isCurrentlyAttentive) {
                if (inattentionStartTime == null) {
                    inattentionStartTimeState.update(currentTimestamp);
                    long timerTimestamp = currentTimestamp + inattentionThresholdMillis;
                    ctx.timerService().registerEventTimeTimer(timerTimestamp);
                    registeredTimerTimestampState.update(timerTimestamp);
                    logger.debug("Key {}: Inattention duration started at {}. Timer set for {}.", ctx.getCurrentKey(), currentTimestamp, timerTimestamp);
                }
            } else { // Is Attentive
                if (inattentionStartTime != null) {
                    logger.debug("Key {}: Attention duration resumed at {}. Clearing state and timer {}.", ctx.getCurrentKey(), currentTimestamp, registeredTimer);
                    inattentionStartTimeState.clear();
                    if (registeredTimer != null) {
                        ctx.timerService().deleteEventTimeTimer(registeredTimer);
                        registeredTimerTimestampState.clear();
                    }
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // --- ADD/CONFIRM LOG HERE ---
            logger.info("DurationDetector onTimer: Timer fired for Key={}, Timestamp={}", ctx.getCurrentKey(), timestamp);
            // --- END LOG ---
            Long inattentionStartTime = inattentionStartTimeState.value();
            Long registeredTimer = registeredTimerTimestampState.value();
            
            if (registeredTimer != null && timestamp == registeredTimer && inattentionStartTime != null) {
                long durationMillis = timestamp - inattentionStartTime;
                logger.warn("Key {}: Prolonged Inattention Timer fired at {}. Start time: {}. Duration: {}ms", ctx.getCurrentKey(), timestamp, inattentionStartTime, durationMillis);

                // --- Generate Duration Inattention JSON Alert ---
                try {
                    ObjectNode alert = objectMapper.createObjectNode();
                    alert.put("thingId", ctx.getCurrentKey());
                    alert.put("alertTimestamp", timestamp);
                    alert.put("feedbackType", "durationInattentionAlert");
                    alert.put("severity", "HIGH"); // Typically prolonged is high severity
                    alert.put("reason", "Continuous period of inattention exceeded threshold");
                    alert.put("inattentionStartTime", inattentionStartTime);
                    alert.put("inattentionDurationMillis", durationMillis);
                    alert.put("thresholdMillis", inattentionThresholdMillis);

                    out.collect(alert.toString());
                } catch (Exception e) {
                     logger.error("Error creating duration inattention JSON alert for key {}", ctx.getCurrentKey(), e);
                }

                // --- Clean up state after firing ---
                inattentionStartTimeState.clear();
                registeredTimerTimestampState.clear();

            } else if (registeredTimer != null && timestamp == registeredTimer) {
                 logger.debug("Key {}: Timer fired at {} but inattention state already cleared. Cleaning up timer state.", ctx.getCurrentKey(), timestamp);
                 registeredTimerTimestampState.clear();
            }
            // Ignore other spurious timers
        }
    }
}
