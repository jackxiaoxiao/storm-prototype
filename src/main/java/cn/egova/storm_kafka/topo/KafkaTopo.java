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

import java.util.Arrays;
import java.util.Properties;

/**
 * storm消费kafka的消息经过计算处理存入到kafka、MySQL中。
 * 常用三种提交topo方式
 * @Author tcb
 */
public class KafkaTopo {

	private static final String KAFKA_SPOUT_ID = "kafkaspout";
	private static final String SPLIT_BOLT_ID = "wordSplitBolt";
	private static final String WORD_COUNT_BOLT_ID = "wordCountBolt";
	private static final String REPORT_BOLLT_ID = "reportBolt";
	private static final String MYSQL_BOLT_ID = "mysqlBolt";
	private static final String KAFKA_BOLT_ID = "forwardToKafka";
	private static final String REDIS_BOLT_ID= "redisBolt";
	private static final String PYTHON_BOLT_IDA="pythonBoltA";
	private static final String PYTHON_BOLT_IDB="pythonBoltB";
	private static final String CONSUME_TOPIC = "origin";
	private static final String PRODUCT_TOPIC = "kafkatopic";
	private static final String ZK_ROOT = "/kafka-storm";
	private static final String ZK_ID = "wordcount";
	private static final String DEFAULT_TOPOLOGY_NAME = "wordCountKafka";
	/**
	 * 远程提交服务器参数定义
	 */
	private static final  String NIMBUS_HOST="dev17";
	private static final Integer NIMBUS_THRIFT_PORT=6627;
	private static final String[] STORM_ZOOKEEPER_SERVERS={"dev17","dev19"};
	private static final Integer STORM_ZOOKEEPER_PORT=2181;
	private static final Boolean isDebug=true;

	public static void main(String[] args) throws Exception {


		BrokerHosts brokerHosts = new ZkHosts("dev17:2181,dev19:2181");
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, CONSUME_TOPIC, ZK_ROOT, KAFKA_SPOUT_ID);
		spoutConfig.forceFromStart = false;
		spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(KAFKA_SPOUT_ID, new KafkaSpout(spoutConfig),3);
		builder.setBolt(SPLIT_BOLT_ID, new WordSpliter(),2).shuffleGrouping(KAFKA_SPOUT_ID);
		builder.setBolt(WORD_COUNT_BOLT_ID, new WordCountBolt(),4).fieldsGrouping(SPLIT_BOLT_ID, new Fields("message"));
		builder.setBolt(REPORT_BOLLT_ID,new ReportBolt(),2).shuffleGrouping(WORD_COUNT_BOLT_ID);
		builder.setBolt(PYTHON_BOLT_IDA,new PythonBoltA(),2).shuffleGrouping(REPORT_BOLLT_ID);
		builder.setBolt(PYTHON_BOLT_IDB,new PythonBoltB(),2).shuffleGrouping(PYTHON_BOLT_IDA);
		builder.setBolt(MYSQL_BOLT_ID, new MysqlBolt(),4).fieldsGrouping(WORD_COUNT_BOLT_ID, new Fields("message", "processTime"));
//		builder.setBolt("writer", new WriterBolt(), 4).fieldsGrouping("word-spilter", new Fields("word"));
     	KafkaBolt bolt = new KafkaBolt();
		bolt.withTopicSelector(new DefaultTopicSelector(PRODUCT_TOPIC))
				.withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
		builder.setBolt(KAFKA_BOLT_ID, bolt, 1).shuffleGrouping(REPORT_BOLLT_ID);
		Config conf = new Config();
		//3. 设置kafka producer的配置
		Properties props = new Properties();
		props.put("metadata.broker.list", "192.168.101.19:9092");
		props.put("producer.type","async");
		props.put("request.required.acks", "0"); // 0 ,-1 ,1
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		conf.put(TridentKafkaState.KAFKA_BROKER_PROPERTIES, props);
		conf.put("topic",PRODUCT_TOPIC);
//		conf.setNumWorkers(3);
//		conf.setNumAckers(0);
//		conf.setDebug(true);
		/**
		 * 远程集群提交方式
		 */
		conf.put(Config.NIMBUS_HOST,NIMBUS_HOST);
		conf.put(Config.NIMBUS_THRIFT_PORT,NIMBUS_THRIFT_PORT);
		conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(STORM_ZOOKEEPER_SERVERS));
		conf.put(Config.STORM_ZOOKEEPER_PORT,STORM_ZOOKEEPER_PORT);
		conf.setDebug(isDebug);
		conf.setNumWorkers(3);
		System.setProperty("storm.jar","E:\\数字政通\\stormTokafka\\storm-example-master\\classes\\artifacts\\storm_example_jar\\storm-example.jar");
		StormSubmitter.submitTopology("kafkatest", conf, builder.createTopology());
	/*	*//**
		 * 本地提交方式、集群提交方式
		 *//*
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
		}*/

	}

}
