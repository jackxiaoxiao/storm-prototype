package cn.egova.storm_kafka.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.util.Map;
public class PythonBoltB extends BaseRichBolt {
    private OutputCollector collector;
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        Integer flag = tuple.getIntegerByField("flag");
        if(flag==0){
            System.out.println("python脚本已经处理完了，可以执行下一步操作！");
        }else{
            System.out.println("继续等待哈！");
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
