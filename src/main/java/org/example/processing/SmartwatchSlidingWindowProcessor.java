package org.example.processing;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;
import org.example.models.SmartwatchSensorReading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class SmartwatchSlidingWindowProcessor {
    private static final Logger logger = LoggerFactory.getLogger(SmartwatchSlidingWindowProcessor.class);
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static DataStream<String> applySlidingWindowAlerts(DataStream<SmartwatchSensorReading> stream) {
        return stream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<SmartwatchSensorReading>forMonotonousTimestamps()
                    .withTimestampAssigner((event, ts) -> parseTimestamp(event.getTimestamp()))
            )
            .keyBy(SmartwatchSensorReading::getThingid)
            .window(SlidingEventTimeWindows.of(Duration.ofMinutes(1), Duration.ofSeconds(10)))
            .aggregate(new AverageHeartRate(), new FormatSmartwatchOutput());
    }

    private static long parseTimestamp(String timestamp) {
        try {
            return LocalDateTime.parse(timestamp, formatter)
                    .atZone(ZoneId.of("UTC"))
                    .toInstant()
                    .toEpochMilli();
        } catch (DateTimeParseException e) {
            logger.warn("Invalid timestamp format: {}. Using current system time instead.", timestamp);
            return Instant.now().toEpochMilli();
        }
    }

    public static class AverageHeartRate implements AggregateFunction<SmartwatchSensorReading, Tuple2<Integer, Integer>, Double> {
        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return Tuple2.of(0, 0);
        }

        @Override
        public Tuple2<Integer, Integer> add(SmartwatchSensorReading value, Tuple2<Integer, Integer> acc) {
            return Tuple2.of(acc.f0 + value.getHeartrate(), acc.f1 + 1);
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

    public static class FormatSmartwatchOutput extends ProcessWindowFunction<Double, String, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Double> input, Collector<String> out) {
            double avg = input.iterator().next();
            String msg = String.format("Smartwatch Avg HR [%s] [%s - %s]: %.2f bpm",
                    key,
                    Instant.ofEpochMilli(context.window().getStart()),
                    Instant.ofEpochMilli(context.window().getEnd()),
                    avg);

            if (avg > 120) {
                msg += " 🚨 ALERT: High average heart rate!";
            }

            out.collect(msg);
        }
    }
}
