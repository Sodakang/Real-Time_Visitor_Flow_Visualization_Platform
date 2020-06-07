package com.uci.ics.project;

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.JedisCommands;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

public class RedisCoordinateReadBolt extends AbstractRedisBolt {

    private Properties properties;

    public RedisCoordinateReadBolt(JedisPoolConfig config) {
        super(config);
        // Load the property file.
        InputStream inputStream = getClass().getClassLoader().
                getResourceAsStream("config.properties");
        this.properties = new Properties();
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        try {
            properties.load(inputStreamReader);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String convertLatency(String latency) {
        int latencyTemp = Integer.parseInt(latency);
        if(latencyTemp <= 10) return "0.01";
        if(latencyTemp <= 30) return "0.03";
        if(latencyTemp <= 50) return "0.05";
        if(latencyTemp <= 70) return "0.07";
        if(latencyTemp <= 90) return "0.09";
        return "Expire";
    }

    @Override
    protected void process(Tuple tuple) {
        String latency = convertLatency(tuple.getStringByField("latency"));
        if(latency.equals("Expire")) return;
        String key = tuple.getStringByField("latitude") + "," +
                tuple.getStringByField("longitude") + ":" + latency;
        JedisCommands jedisCommands = null;
        try {
            jedisCommands = getInstance();
            // If the key does not exist, the value of key will be initialized as 0, and set the key as persistent.
            List<String> coordinates = jedisCommands.lrange(key, 0, -1);
            for(String coordinate: coordinates) {
                this.collector.emit(new Values(coordinate, 1));
            }
            this.collector.ack(tuple);
        }catch (Exception e) {
            this.collector.reportError(e);
            this.collector.fail(tuple);
        } finally {
            returnInstance(jedisCommands);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("location", "visitors"));
    }
}
