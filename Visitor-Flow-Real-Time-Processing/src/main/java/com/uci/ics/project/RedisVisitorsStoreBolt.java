package com.uci.ics.project;

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.JedisCommands;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class RedisVisitorsStoreBolt extends AbstractRedisBolt {

    private Properties properties;

    public RedisVisitorsStoreBolt(JedisPoolConfig config) {
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

    @Override
    protected void process(Tuple tuple) {
        String key = tuple.getStringByField("location");
        JedisCommands jedisCommands = null;
        try {
            jedisCommands = getInstance();
            // If the key does not exist, the value of key will be initialized as 0, and set the key as persistent.
            jedisCommands.incr(key);
            String visits = jedisCommands.get(key);
            if(visits.equals("1")) {  // If the key exists for the first time, we need to set the expire time.
                jedisCommands.expire(key, Integer.parseInt(properties.getProperty("EXPIRE_TIME_IN_SECONDS")));
            } else if(StringUtils.isEmpty(visits)) {  //If the key is timeout.
                jedisCommands.incr(key);
                jedisCommands.expire(key, Integer.parseInt(properties.getProperty("EXPIRE_TIME_IN_SECONDS")));
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

    }
}
