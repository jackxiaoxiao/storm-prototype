package cn.egova.storm_kafka.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cn.egova.storm_kafka.utils.RedisMethod;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Admin on 2017/2/17.
 */
public class redisBolt extends BaseRichBolt{
    private static final long serialVersionUID = 5683648523524179434L;
    private OutputCollector collector;
    private HashMap<String, Integer> counters = null;


    public void prepare(Map stormConf, TopologyContext context,OutputCollector collector) {
        this.collector = collector;
        this.counters = new HashMap<String, Integer>();
    }

    //load into redis
    public void execute(Tuple input) {
        String str = input.getString(0);
        RedisMethod jedistest = new RedisMethod();
        jedistest.setup();
        System.out.println("Connection to server sucessfully");
        if (!counters.containsKey(str)) {
            counters.put(str, 1);
        } else {
            Integer c = counters.get(str) + 1;
            counters.put(str, c);
        }
        jedistest.jedis.zincrby("storm2redis",1,str);

        System.out.println("WordCounter+++++++++++++++++++++++++++++++++++++++++++");
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(new Fields("key"));
//        declarer.declare(new Fields("value"));

    }

}
