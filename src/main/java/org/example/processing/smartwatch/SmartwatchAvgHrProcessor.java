// File: Flink-CEP/src/main/java/org/example/processing/smartwatch/SmartwatchAvgHrProcessor.java
package org.example.processing.smartwatch; // New package

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction; // Import MapFunction
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;
import org.example.models.SmartwatchReading; // Updated model import
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable; // Import Serializable
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

// Renamed from SmartwatchSlidingWindowProcessor
public class SmartwatchAvgHrProcessor {
    private static final Logger logger = LoggerFactory.getLogger(SmartwatchAvgHrProcessor.class);
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // Wrapper class needed if timestamp parsing happens here
    public static class TimestampedSmartwatchReading implements Serializable {
        private static final long serialVersionUID = 301L;
        public long timestampMillis;
        public SmartwatchReading reading;
        public TimestampedSmartwatchReading() {}
        public TimestampedSmartwatchReading(long ts, SmartwatchReading r) { this.timestampMillis = ts; this.reading = r; }
    }

    // MapFunction to parse timestamp
    public static class TimestampParser implements MapFunction<SmartwatchReading, TimestampedSmartwatchReading> {
        @Override
        public TimestampedSmartwatchReading map(SmartwatchReading value) {
            if (value == null || value.getTimestamp() == null) return null;
            try {
                LocalDateTime ldt = LocalDateTime.parse(value.getTimestamp(), formatter);
                long timestampMillis = ldt.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
                return new TimestampedSmartwatchReading(timestampMillis, value);
            } catch (DateTimeParseException e) {
                logger.warn("Invalid smartwatch timestamp format: {}. Skipping.", value.getTimestamp());
                return null;
            }
        }
    }

    // Main processing function
    public static DataStream<String> applySlidingWindowAlerts(DataStream<SmartwatchReading> stream) {
        return stream
            .map(new TimestampParser()) // Parse timestamp first
            .filter(value -> value != null)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<TimestampedSmartwatchReading>forBoundedOutOfOrderness(Duration.ofSeconds(5)) // Allow some lateness
                    .withTimestampAssigner((event, ts) -> event.timestampMillis) // Use parsed millis
                    .withIdleness(Duration.ofMinutes(1)) // Handle idle sources
            )
            .keyBy(tsr -> tsr.reading.getThingid()) // Key by thingid from wrapped object
            .window(SlidingEventTimeWindows.of(Duration.ofMinutes(1), Duration.ofSeconds(10)))
            .aggregate(new AverageHeartRate(), new FormatSmartwatchOutput());
    }

    // AverageHeartRate AggregateFunction (Input type changed)
    public static class AverageHeartRate implements AggregateFunction<TimestampedSmartwatchReading, Tuple2<Integer, Integer>, Double> {
        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return Tuple2.of(0, 0);
        }

        @Override
        public Tuple2<Integer, Integer> add(TimestampedSmartwatchReading value, Tuple2<Integer, Integer> acc) {
            // Access heartrate from the wrapped reading object
            return Tuple2.of(acc.f0 + value.reading.getHeartrate(), acc.f1 + 1);
        }

        @Override
        public Double getResult(Tuple2<Integer, Integer> acc) {
            return acc.f1 == 0 ? 0.0 : (double) acc.f0 / acc.f1;
        }

        @Override
        public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
            return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
        }
    }

    // FormatSmartwatchOutput ProcessWindowFunction (Keep as is)
    public static class FormatSmartwatchOutput extends ProcessWindowFunction<Double, String, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Double> input, Collector<String> out) {
            double avg = input.iterator().next();
            String msg = String.format("Smartwatch Avg HR [%s] [%s - %s]: %.2f bpm",
                    key,
                    Instant.ofEpochMilli(context.window().getStart()),
                    Instant.ofEpochMilli(context.window().getEnd()),
                    avg);

            if (avg > 120) { // Example threshold
                msg += " \uD83D\uDEA8 ALERT: High average heart rate!";
            }

            out.collect(msg);
        }
    }
}