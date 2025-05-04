package org.example.processing;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.example.models.SmartwatchSensorReading;

import java.util.List;
import java.util.Map;

public class SmartwatchCEPProcessor {

    public static DataStream<String> applyCEP(DataStream<SmartwatchSensorReading> input) {

        // Pattern: Two consecutive heart rate readings over 120
        Pattern<SmartwatchSensorReading, ?> highHeartRatePattern = Pattern.<SmartwatchSensorReading>begin("first")
                .where(new SimpleCondition<SmartwatchSensorReading>() {
                    @Override
                    public boolean filter(SmartwatchSensorReading reading) {
                        return reading.getHeartrate() > 120;
                    }
                })
                .next("second")
                .where(new SimpleCondition<SmartwatchSensorReading>() {
                    @Override
                    public boolean filter(SmartwatchSensorReading reading) {
                        return reading.getHeartrate() > 120;
                    }
                });

        return CEP.pattern(input.keyBy(SmartwatchSensorReading::getThingid), highHeartRatePattern)
                .select(new PatternSelectFunction<SmartwatchSensorReading, String>() {
                    @Override
                    public String select(Map<String, List<SmartwatchSensorReading>> pattern) {
                        SmartwatchSensorReading first = pattern.get("first").get(0);
                        SmartwatchSensorReading second = pattern.get("second").get(0);
                        return String.format("ALERT: High heart rate detected twice in a row for device %s. Readings: %.2f -> %.2f",
                                first.getThingid(), first.getHeartrate(), second.getHeartrate());
                    }
                });
    }
}
