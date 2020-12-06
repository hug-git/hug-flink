package com.hug.api.source;

import com.hug.api.bean.SensorReading;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

public class MySource implements SourceFunction<SensorReading> {
    private boolean running = true;

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        Random random = new Random();

        HashMap<String, Double> sensorTempMap = new HashMap<String, Double>();
        for (int i = 0; i < 10; i++) {
            sensorTempMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
        }

        while (running) {
            for (String sensorId : sensorTempMap.keySet()) {
                double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                sensorTempMap.put(sensorId, newTemp);
                ctx.collect(new SensorReading(sensorId, System.currentTimeMillis(), newTemp));
            }

        }
    }

    @Override
    public void cancel() {
        running = false;
    }

}
