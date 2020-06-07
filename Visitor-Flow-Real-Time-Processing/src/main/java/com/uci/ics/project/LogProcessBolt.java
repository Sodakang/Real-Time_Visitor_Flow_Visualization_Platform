package com.uci.ics.project;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/*
* The Bolt for receiving the data from Kafka and processing the logs.
* */
public class LogProcessBolt extends BaseRichBolt {

    private OutputCollector outputCollector;

    /*
    * Return a String array including time, longitude and latitude.
    * */
    private String[] processLog(String log) {
        String[] splits = log.split("\t");
        // String phone = splits[0];
        String[] temp = splits[1].split(",");
        String latitude = temp[0], longitude = temp[1];
        String time = splits[2];
        String latency = splits[3];
        return new String[] {time, latitude, longitude, latency};
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        byte[] binaryByField = tuple.getBinaryByField("bytes");
        String value = new String(binaryByField);
        String[] logs = processLog(value);
        try {
            long time = DateUtils.getInstance().getTime(logs[0]);
            String latitude = logs[1], longitude = logs[2], latency = logs[3];
            System.out.println(time + "," + latitude + "," + longitude + "," + latency);
            // outputCollector.emit(new Values(time, Double.parseDouble(latitude), Double.parseDouble(longitude)));
            outputCollector.emit(new Values(time, latitude, longitude, latency));
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            outputCollector.ack(tuple);
        } catch (Exception e) {
            outputCollector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("time", "latitude", "longitude", "latency"));
    }
}
