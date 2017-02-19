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

import java.util.*;

public class TodayHotCaseType extends BaseRichBolt {
    public static Map<String, Integer> result = new HashMap<String, Integer>();
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String action = tuple.getStringByField("action");
        String type = tuple.getStringByField("type");

        if (TodayCaseReport.TYPE.equals(action)) {
            if (result.get(type) != null) {
                Integer integer = result.get(type);
                result.put(type, integer + 1);
            } else {
                result.put(type, 1);
            }
        }

        StringBuilder stringBuilder = new StringBuilder();
        Map<String, Integer> stringIntegerMap = sortByValue(result);
        for (Map.Entry<String, Integer> entry : stringIntegerMap.entrySet()) {
            stringBuilder.append(entry.getKey() + "" + entry.getValue() + ";");
        }
        this.collector.emit(new Values(String.format("%s#%s", "今日高发", stringBuilder)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message"));
    }

    public static <K, V extends Comparable<? super V>> Map<K, V>
    sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list =
                new LinkedList<Map.Entry<K, V>>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return (o2.getValue()).compareTo(o1.getValue());
            }
        });

        Map<K, V> result = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }
}
