package cn.egova.storm_kafka.bolt;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ProducerDemo {
	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put("zk.connect", "dev17:2181,dev19:2181");
		props.put("metadata.broker.list","dev17:9092,dev19:9092,dev20:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);

		for (int i = 1; i <= 100; i++) {	
			Thread.sleep(500);
			producer.send(new KeyedMessage<String, String>("wordcount",
					"hello word hbase spark storm hive zhe yang"));
		}

	}
}