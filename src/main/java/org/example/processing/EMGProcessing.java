package org.example.processing;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
// Import necessary TypeInformation classes
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
// Removed: import org.apache.flink.streaming.api.windowing.time.Time; // Use Duration
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.models.EMGSensorReading; // Input POJO
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration; // Use Duration
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;


public class EMGProcessing {

    private static final Logger logger = LoggerFactory.getLogger(EMGProcessing.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // Configuration Constants
    private static final Duration RMS_WINDOW_SIZE = Duration.ofSeconds(1);
    private static final Duration RMS_WINDOW_SLIDE = Duration.ofMillis(500);
    private static final Duration FATIGUE_TREND_WINDOW_DURATION = Duration.ofSeconds(30);
    private static final double HIGH_RMS_THRESHOLD = 50.0; // EXAMPLE - NEEDS CALIBRATION!
    private static final int MIN_HISTORY_SIZE_FOR_ALERT = 10;

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");


    /**
     * Main processing pipeline for EMG fatigue detection.
     */
    public static DataStream<String> processEMGFatigue(
            DataStream<EMGSensorReading> rawEmgStream,
            Set<String> musclesToMonitor) {

        // 1. Parse Timestamp and create wrapper object
        DataStream<TimestampedEMGSensorReading> timedStream = rawEmgStream
                .map(new TimestampParserEMG())
                .filter(value -> value != null)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TimestampedEMGSensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.timestampMillis)
                                .withIdleness(Duration.ofMinutes(1))
                );

        // 2. Flatten the structure
        DataStream<EMGReading> muscleStream = timedStream
                .flatMap(new MuscleDataExtractor(musclesToMonitor))
                .filter(value -> value != null);


        // 3. Calculate RMS over short sliding windows per muscle
        // --- CORRECTED keyBy with TypeInformation ---
        TypeInformation<Tuple2<String, String>> keyTypeInfo = TypeInformation.of(new TypeHint<Tuple2<String, String>>() {});
        WindowedStream<EMGReading, Tuple2<String, String>, TimeWindow> rmsWindowedStream = muscleStream
                .keyBy(r -> Tuple2.of(r.thingId, r.muscleName), keyTypeInfo) // Provide TypeInfo
                .window(SlidingEventTimeWindows.of(RMS_WINDOW_SIZE, RMS_WINDOW_SLIDE)); // Use Duration

        DataStream<Tuple4<String, String, Long, Double>> rmsStream = rmsWindowedStream
                .aggregate(new RMSAggregator(), new RMSWindowProcessor());


        // 4. Detect Fatigue Trend (Sustained High RMS) using stateful processing
        // --- CORRECTED keyBy with TypeInformation ---
        // Define TypeInformation for the key Tuple4<String, String, Long, Double> -> Tuple2<String, String>
        TypeInformation<Tuple2<String, String>> processKeyTypeInfo = TypeInformation.of(new TypeHint<Tuple2<String, String>>() {});
        DataStream<String> fatigueAlertStream = rmsStream
                .keyBy(t -> Tuple2.of(t.f0, t.f1), processKeyTypeInfo) // Provide TypeInfo
                .process(new FatigueDetector(FATIGUE_TREND_WINDOW_DURATION.toMillis(), HIGH_RMS_THRESHOLD, MIN_HISTORY_SIZE_FOR_ALERT));

        return fatigueAlertStream;
    }


    // --- Helper Classes ---

    /** Wrapper to add parsed timestamp without modifying original POJO */
    public static class TimestampedEMGSensorReading implements Serializable {
        private static final long serialVersionUID = 1L;
        public long timestampMillis;
        public EMGSensorReading reading;
        public TimestampedEMGSensorReading() {}
        public TimestampedEMGSensorReading(long ts, EMGSensorReading r) { this.timestampMillis = ts; this.reading = r; }
    }

    /** Parses the string timestamp from EMGSensorReading */
    public static class TimestampParserEMG implements MapFunction<EMGSensorReading, TimestampedEMGSensorReading> {
        @Override
        public TimestampedEMGSensorReading map(EMGSensorReading value) {
            if (value == null || value.getTimestamp() == null) return null;
            try {
                LocalDateTime ldt = LocalDateTime.parse(value.getTimestamp(), TIMESTAMP_FORMATTER);
                long timestampMillis = ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
                return new TimestampedEMGSensorReading(timestampMillis, value);
            } catch (Exception e) {
                logger.warn("EMG Parser: Failed to parse timestamp string: {}. Skipping.", value.getTimestamp(), e);
                return null;
            }
        }
    }

    /** Represents a single muscle reading at a point in time */
    public static class EMGReading implements Serializable {
        private static final long serialVersionUID = 1L;
        public String thingId;
        public long timestampMillis;
        public String muscleName;
        public double microvolts;
        public EMGReading() {}
        public EMGReading(String thingId, long timestampMillis, String muscleName, double microvolts) {
            this.thingId = thingId; this.timestampMillis = timestampMillis; this.muscleName = muscleName; this.microvolts = microvolts;
        }
         public String getThingId() { return thingId; }
         public long getTimestampMillis() { return timestampMillis; }
         public String getMuscleName() { return muscleName; }
         public double getMicrovolts() { return microvolts; }
    }

    /** Extracts individual muscle readings */
    public static class MuscleDataExtractor implements FlatMapFunction<TimestampedEMGSensorReading, EMGReading> {
         private final Set<String> musclesToMonitor;
         public MuscleDataExtractor(Set<String> musclesToMonitor) { this.musclesToMonitor = musclesToMonitor; }
         @Override
         public void flatMap(TimestampedEMGSensorReading tsReading, Collector<EMGReading> out) {
            if (tsReading == null || tsReading.reading == null) return;
            EMGSensorReading reading = tsReading.reading;
            long timestamp = tsReading.timestampMillis;
            String thingId = reading.getThingid();
            try {
                if (musclesToMonitor.contains("deltoids_left")) out.collect(new EMGReading(thingId, timestamp, "deltoids_left", reading.getDeltoids_left()));
                if (musclesToMonitor.contains("triceps_left")) out.collect(new EMGReading(thingId, timestamp, "triceps_left", reading.getTriceps_left()));
                if (musclesToMonitor.contains("biceps_left")) out.collect(new EMGReading(thingId, timestamp, "biceps_left", reading.getBiceps_left()));
                if (musclesToMonitor.contains("wrist_extensors_left")) out.collect(new EMGReading(thingId, timestamp, "wrist_extensors_left", reading.getWrist_extensors_left()));
                if (musclesToMonitor.contains("wrist_flexor_left")) out.collect(new EMGReading(thingId, timestamp, "wrist_flexor_left", reading.getWrist_flexor_left()));
                if (musclesToMonitor.contains("deltoids_right")) out.collect(new EMGReading(thingId, timestamp, "deltoids_right", reading.getDeltoids_right()));
                if (musclesToMonitor.contains("triceps_right")) out.collect(new EMGReading(thingId, timestamp, "triceps_right", reading.getTriceps_right()));
                if (musclesToMonitor.contains("biceps_right")) out.collect(new EMGReading(thingId, timestamp, "biceps_right", reading.getBiceps_right()));
                if (musclesToMonitor.contains("wrist_extensors_right")) out.collect(new EMGReading(thingId, timestamp, "wrist_extensors_right", reading.getWrist_extensors_right()));
                if (musclesToMonitor.contains("wrist_flexor_right")) out.collect(new EMGReading(thingId, timestamp, "wrist_flexor_right", reading.getWrist_flexor_right()));
                if (musclesToMonitor.contains("trapezius_left")) out.collect(new EMGReading(thingId, timestamp, "trapezius_left", reading.getTrapezius_left()));
                if (musclesToMonitor.contains("trapezius_right")) out.collect(new EMGReading(thingId, timestamp, "trapezius_right", reading.getTrapezius_right()));
                if (musclesToMonitor.contains("pectoralis_left")) out.collect(new EMGReading(thingId, timestamp, "pectoralis_left", reading.getPectoralis_left()));
                if (musclesToMonitor.contains("pectoralis_right")) out.collect(new EMGReading(thingId, timestamp, "pectoralis_right", reading.getPectoralis_right()));
                if (musclesToMonitor.contains("latissimus_left")) out.collect(new EMGReading(thingId, timestamp, "latissimus_left", reading.getLatissimus_left()));
                if (musclesToMonitor.contains("latissimus_right")) out.collect(new EMGReading(thingId, timestamp, "latissimus_right", reading.getLatissimus_right()));
            } catch (Exception e) { logger.error("Error extracting muscle data for thingId {}", thingId, e); }
         }
     }


    /** Accumulator for RMS calculation */
    public static class RMSAccumulator implements Serializable {
        private static final long serialVersionUID = 1L;
        public double sumOfSquares = 0.0; public long count = 0L;
    }

    /** Calculates RMS within a window */
    public static class RMSAggregator implements AggregateFunction<EMGReading, RMSAccumulator, Double> {
        private static final long serialVersionUID = 1L;
        @Override public RMSAccumulator createAccumulator() { return new RMSAccumulator(); }
        @Override public RMSAccumulator add(EMGReading v, RMSAccumulator a) { a.sumOfSquares += v.microvolts*v.microvolts; a.count++; return a; }
        @Override public Double getResult(RMSAccumulator a) { return (a.count==0) ? Double.NaN : Math.sqrt(a.sumOfSquares / a.count); }
        @Override public RMSAccumulator merge(RMSAccumulator a, RMSAccumulator b) { a.sumOfSquares += b.sumOfSquares; a.count += b.count; return a; }
    }

    /** Adds metadata (key, window end time) to the aggregated RMS value */
    public static class RMSWindowProcessor extends ProcessWindowFunction<Double, Tuple4<String, String, Long, Double>, Tuple2<String, String>, TimeWindow> {
         private static final long serialVersionUID = 1L;
         @Override
         public void process(Tuple2<String, String> key, Context context, Iterable<Double> aggregates, Collector<Tuple4<String, String, Long, Double>> out) {
             Double rms = aggregates.iterator().next();
             out.collect(Tuple4.of(key.f0, key.f1, context.window().getEnd(), rms));
         }
     }


    /** Stateful function to detect fatigue based on sustained high average RMS */
    public static class FatigueDetector extends KeyedProcessFunction<Tuple2<String, String>, Tuple4<String, String, Long, Double>, String> { /* ... as before, including corrected open() ... */
        private static final long serialVersionUID = 1L;
        private final long trendWindowMillis; private final double highRmsThreshold; private final int minHistorySize;
        private transient ListState<Tuple2<Long, Double>> rmsHistoryState;
        public FatigueDetector(long t, double h, int m) {this.trendWindowMillis=t; this.highRmsThreshold=h; this.minHistorySize=m;}
        @Override public void open(Configuration parameters) throws Exception {
            rmsHistoryState = getRuntimeContext().getListState(new ListStateDescriptor<>("rmsHistory", TypeInformation.of(new TypeHint<Tuple2<Long, Double>>() {})));
            logger.info("Initialized FatigueDetector state for subtask {}/{}", getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks());
        }
        @Override public void processElement(Tuple4<String, String, Long, Double> value, Context ctx, Collector<String> out) throws Exception {
             String thingId = value.f0; String muscle = value.f1; Long currentTimestamp = value.f2; Double currentRms = value.f3;
             if (currentRms == null || currentRms.isNaN()) return;
             rmsHistoryState.add(Tuple2.of(currentTimestamp, currentRms));
             List<Tuple2<Long, Double>> history = new ArrayList<>(); Long oldestAllowedTimestamp = currentTimestamp - trendWindowMillis; boolean stateChanged = false;
             for (Tuple2<Long, Double> entry : rmsHistoryState.get()) { if (entry.f0 != null && entry.f0 >= oldestAllowedTimestamp) { history.add(entry); } else { stateChanged = true; } }
             if(stateChanged || history.isEmpty()){ rmsHistoryState.update(history); }
             logger.debug("Processing EMG for Key: {}, Timestamp: {}, RMS: {:.2f}, History Size: {}", ctx.getCurrentKey(), currentTimestamp, currentRms, history.size());
             if (history.size() >= minHistorySize) {
                double sumRms = 0; for(Tuple2<Long, Double> entry : history) { sumRms += entry.f1; } double avgRms = sumRms / history.size();
                logger.trace("Key: {}, Avg RMS over last {}ms: {:.2f} (Threshold: {})", ctx.getCurrentKey(), trendWindowMillis, avgRms, highRmsThreshold);
                if (avgRms > highRmsThreshold) {
                    ObjectNode alert = objectMapper.createObjectNode();
                    alert.put("thingId", thingId); alert.put("timestamp", currentTimestamp); alert.put("feedbackType", "emgFatigueAlert");
                    alert.put("muscle", muscle); alert.put("severity", "HIGH"); alert.put("reason", "Sustained High Average RMS");
                    alert.put("averageRMS", Math.round(avgRms * 100.0) / 100.0); alert.put("checkWindowSeconds", trendWindowMillis / 1000);
                    alert.put("rmsThreshold", highRmsThreshold);
                    out.collect(alert.toString());
                    logger.warn("EMG Fatigue Alert Triggered: Key={}, Alert={}", ctx.getCurrentKey(), alert.toString());
                }
             }
        }
    }

} 