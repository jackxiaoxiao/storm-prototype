package cn.egova.storm_kafka.topo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import cn.egova.storm_kafka.bolt.*;
import cn.egova.storm_kafka.spout.MessageScheme;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;
import storm.kafka.trident.TridentKafkaState;

import java.util.Properties;

/**
 * storm从kafka消费经过计算处理到kafka、redis、MySQL数据库中。
 * @Author tcb.
 */
public class KafkaToplogy {

    private static final String KAFKA_SPOUT_ID = "kafkaspout";
    private static final String SPLIT_BOLT_ID = "msgSplitBolt";
    private static final String MSG_CAL_BOLT_ID = "msgCalBolt";
    private static final String REPORT_BOLT_ID = "reportBolt";
    private static final String MYSQL_BOLT_ID = "mysqlBolt";
    private static final String KAFKA_BOLT_ID = "forwardToKafka";
    private static final String REDIS_BOLT_ID= "redisBolt";
    private static final String PYTHON_BOLT_IDA="pythonBoltA";
    private static final String PYTHON_BOLT_IDB="pythonBoltB";
    private static final String CONSUME_TOPIC = "origin";
    private static final String PRODUCT_TOPIC = "kafkatopic";
    private static final String ZK_ROOT = "/kafka-storm";
    private static final String DEFAULT_TOPOLOGY_NAME = "msgCalTime";

    public static void main(String[] args) throws Exception {

        BrokerHosts brokerHosts = new ZkHosts("dev17:2181,dev19:2181");
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, CONSUME_TOPIC, ZK_ROOT, KAFKA_SPOUT_ID);
        spoutConfig.forceFromStart = false;
        spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(KAFKA_SPOUT_ID, new KafkaSpout(spoutConfig),3);
        builder.setBolt(SPLIT_BOLT_ID, new MsgSpliter(),2).shuffleGrouping(KAFKA_SPOUT_ID);
        builder.setBolt(MSG_CAL_BOLT_ID, new MsgCalBolt(),4).fieldsGrouping(SPLIT_BOLT_ID, new Fields("message"));
        builder.setBolt(REPORT_BOLT_ID,new ReportBolt(),2).shuffleGrouping(MSG_CAL_BOLT_ID);
        builder.setBolt(REDIS_BOLT_ID,new redisBolt(),2).shuffleGrouping(REPORT_BOLT_ID);
        builder.setBolt(PYTHON_BOLT_IDA,new PythonBoltA(),2).shuffleGrouping(REPORT_BOLT_ID);
        builder.setBolt(PYTHON_BOLT_IDB,new PythonBoltB(),2).shuffleGrouping(PYTHON_BOLT_IDA);
        builder.setBolt(MYSQL_BOLT_ID, new MysqlBolt(),4).fieldsGrouping(MSG_CAL_BOLT_ID, new Fields("message", "processTime"));
        KafkaBolt bolt = new KafkaBolt();
        bolt.withTopicSelector(new DefaultTopicSelector(PRODUCT_TOPIC))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
        builder.setBolt(KAFKA_BOLT_ID, bolt, 1).shuffleGrouping(REPORT_BOLT_ID);
        Config conf = new Config();
        //3. 设置kafka producer的配置
        Properties props = new Properties();
        props.put("metadata.broker.list", "192.168.101.17:9092,192.168.101.19:9092,192.168.101.20:9092");
        props.put("producer.type","async");
        props.put("request.required.acks", "0"); // 0 ,-1 ,1
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        conf.put(TridentKafkaState.KAFKA_BROKER_PROPERTIES, props);
        conf.put("topic",PRODUCT_TOPIC);
        conf.setNumWorkers(3);
        conf.setDebug(true);
        if(args.length > 0){
            // cluster submit.
            try {
                StormSubmitter.submitTopology(DEFAULT_TOPOLOGY_NAME, conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        }else{
            new LocalCluster().submitTopology(DEFAULT_TOPOLOGY_NAME, conf, builder.createTopology());
        }

    }
}

