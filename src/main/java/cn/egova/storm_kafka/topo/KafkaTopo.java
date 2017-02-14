package cn.egova.storm_kafka.topo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import cn.egova.storm_kafka.bolt.MysqlBolt;
import cn.egova.storm_kafka.bolt.WordCountBolt;
import cn.egova.storm_kafka.bolt.WordSpliter;
import cn.egova.storm_kafka.spout.MessageScheme;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

public class KafkaTopo {

	public static void main(String[] args) throws Exception {
		
		String topic = "origin";
		String zkRoot = "/kafka-storm";
		String spoutId = "KafkaSpout";
		BrokerHosts brokerHosts = new ZkHosts("dev17:2181,dev19:2181"); 
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "origin", zkRoot, spoutId);
		spoutConfig.forceFromStart = false;
		spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("KafkaSpout", new KafkaSpout(spoutConfig),2);
		builder.setBolt("word-spilter", new WordSpliter(),2).shuffleGrouping(spoutId);
		builder.setBolt("WordCountBolt", new WordCountBolt(),4).fieldsGrouping("word-spilter", new Fields("message"));
		builder.setBolt("mysql", new MysqlBolt(),4).fieldsGrouping("WordCountBolt", new Fields("word", "count"));
//		builder.setBolt("writer", new WriterBolt(), 4).fieldsGrouping("word-spilter", new Fields("word"));
		//1. KafkaBolt的前置组件emit出来的(可以是spout也可以是bolt)
////		Spout spout = new Spout(new Fields("key", "message"));
//		WordCountBolt wc=new WordCountBolt(new Fields("key","message"));
//		builder.setBolt("spout", wc);
		//2. 给KafkaBolt配置topic及前置tuple消息到kafka的mapping关系
	/*	KafkaBolt bolt = new KafkaBolt();
		bolt.withTopicSelector(new DefaultTopicSelector("kafkatopic"))
				.withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
		builder.setBolt("forwardToKafka", bolt, 1).shuffleGrouping("WordCountBolt");

		Config conf = new Config();
		//3. 设置kafka producer的配置
		Properties props = new Properties();
		props.put("metadata.broker.list", "192.168.101.17:9092");
		props.put("producer.type","async");
		props.put("request.required.acks", "0"); // 0 ,-1 ,1
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		conf.put(TridentKafkaState.KAFKA_BROKER_PROPERTIES, props);
		conf.put("topic","kafkatopic");*/
		Config conf = new Config();
		conf.setNumWorkers(3);
//		conf.setNumAckers(0);
		conf.setDebug(false);
		if(args.length > 0){
			// cluster submit.
			try {
				StormSubmitter.submitTopology("kafkaboltTest", conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
		}else{
			new LocalCluster().submitTopology("WordCount", conf, builder.createTopology());
		}

	}
	
	/*	LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("WordCount", conf, builder.createTopology());*/
		
		//提交topology到storm集群中运行
//		StormSubmitter.submitTopology("sufei-topo", conf, builder.createTopology());

}
