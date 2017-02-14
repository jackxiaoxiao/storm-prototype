package cn.egova.storm_kafka.spout;

import java.io.UnsupportedEncodingException;
import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MessageScheme implements Scheme {
	
	private static final long serialVersionUID = 8423372426211017613L;

	@Override
	public List<Object> deserialize(byte[] bytes) {
			try {
				String msg = new String(bytes, "UTF-8");
				return new Values(msg); 
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			return null;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("msg");
	}

}