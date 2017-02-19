package cn.egova.storm_kafka.bolt.cases;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class TodayCaseRegister extends BaseRichBolt {
    private static int todayCount = 0;
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String action = tuple.getStringByField("action");
        String type = tuple.getStringByField("type");
        String content = tuple.getStringByField("content");
        if("立案".equals(action)) {
            todayCount ++;
        }
        this.collector.emit(new Values(String.format("%s#%d", "今日立案数", todayCount)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message"));
    }
}
