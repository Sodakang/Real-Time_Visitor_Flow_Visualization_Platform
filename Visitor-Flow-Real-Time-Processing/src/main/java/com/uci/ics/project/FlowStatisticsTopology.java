package com.uci.ics.project;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.topology.TopologyBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.UUID;

public class FlowStatisticsTopology {

    public static void main(String[] args) throws IOException {
        // Load the property file.
        InputStream inputStream = FlowStatisticsTopology.class.getClassLoader().
                getResourceAsStream("config.properties");
        Properties properties = new Properties();
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        properties.load(inputStreamReader);

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        // The Zookeeper address which used by Kafka.
        BrokerHosts brokerHosts = new ZkHosts(properties.getProperty("ZookeeperBrokerHost"));
        // The topic which Kafka stores data.
        String topic = properties.getProperty("KafkaTopic");
        // Set a root directory in Zookeeper, which stores the location information for KafkaSpout (offset).
        String zkRoot = "/" + topic;
        String id = UUID.randomUUID().toString();
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        // The operations for reading the offset.
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();

        topologyBuilder.setSpout(KafkaSpout.class.getSimpleName(), new KafkaSpout(spoutConfig));
        topologyBuilder.setBolt(LogProcessBolt.class.getName(), new LogProcessBolt())
                .shuffleGrouping(KafkaSpout.class.getSimpleName());
        topologyBuilder.setBolt(LocationCountBolt.class.getName(), new LocationCountBolt())
                .shuffleGrouping(LogProcessBolt.class.getName());

        // Connect to Redis
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig.Builder()
                .setHost(properties.getProperty("RedisHost"))
                .setPort(Integer.parseInt(properties.getProperty("RedisHostPort")))
                .build();
        RedisVisitorsStoreBolt redisVisitorsStoreBolt = new RedisVisitorsStoreBolt(jedisPoolConfig);
        topologyBuilder.setBolt(RedisVisitorsStoreBolt.class.getName(), new RedisVisitorsStoreBolt(jedisPoolConfig))
                .shuffleGrouping(LocationCountBolt.class.getName());

        // Submit the topology into a Storm cluster.
//        String topologyName = FlowStatisticsTopology.class.getSimpleName();
//        try {
//            StormSubmitter.submitTopology(topologyName,new Config(), topologyBuilder.createTopology());
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        // Submit the topology into local cluster.
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("FlowStatisticsTopology",
                new Config(), topologyBuilder.createTopology());

    }
}
