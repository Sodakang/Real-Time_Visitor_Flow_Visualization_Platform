package com.uci.ics.project;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * The bolt for count the visitors in a location.
 * It is given up.
 */
public class LocationCountBolt extends BaseRichBolt {

    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String latitude = tuple.getStringByField("latitude");
        String longitude = tuple.getStringByField("longitude");
        String location = latitude + "," + longitude;
        outputCollector.emit(new Values(location, 1));
        try {
            outputCollector.ack(tuple);
        } catch (Exception e) {
            outputCollector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("location", "visitors"));
    }
}
