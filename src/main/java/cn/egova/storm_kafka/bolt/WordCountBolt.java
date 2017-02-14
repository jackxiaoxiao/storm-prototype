package cn.egova.storm_kafka.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class WordCountBolt extends BaseRichBolt {
    private OutputCollector collector;
    private HashMap<String, Long> counts = null;
    private long endTime;

    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.counts = new HashMap<String, Long>();
    }

    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("message");
        long startTime = Long.valueOf(tuple.getStringByField("time"));
        endTime = new Date().getTime();
        //Integer.parseInt();
        Long timeConsume = this.counts.get(word);
        if (timeConsume == null) {
            timeConsume = 0L;
        }
        timeConsume = endTime - startTime;
        this.counts.put(word, timeConsume);
        this.collector.emit(new Values(word, timeConsume));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
