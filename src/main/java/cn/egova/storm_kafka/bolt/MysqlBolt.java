package cn.egova.storm_kafka.bolt;

import java.sql.*;
import java.text.MessageFormat;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class MysqlBolt extends BaseRichBolt{
	private OutputCollector collector;
	Connection conn = null;
	String from = "stormkafka"; //表名
	private String message;
	private Number processTime;
	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		try {
			LinkDB();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void LinkDB() throws InstantiationException, IllegalAccessException, SQLException {
		// TODO Auto-generated method stub
		String host_port = "192.168.101.17:3306";
		String database = "test";
		String username = "egova";
		String password = "egova";
		String url = "jdbc:mysql://" + host_port + "/" + database;
			try {
				Class.forName("com.mysql.jdbc.Driver");
				conn  = DriverManager.getConnection(url, username, password);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		 message= tuple.getString(0);
		 processTime = tuple.getLong(1);

		InsertDB(message, processTime);
	}

	private void InsertDB(String message, Number processTime) {
		// TODO Auto-generated method stub
		String sql =null;
		sql = "insert into " + this.from+ "(message,processTime) values ('" +message+"',"+processTime+ ")";
		String selectSql = MessageFormat.format("select * from stormkafka where message = {0} and processTime = {1}", "'"+message+"'",""+processTime+"");
		String uadateSql = MessageFormat.format("update stormkafka set message = {0} where processTime = {1}", "'"+message+"'",""+processTime+"");
		try {
		    Statement	statement = conn.createStatement();
			statement.executeUpdate(sql);
			ResultSet rs =  statement.executeQuery(selectSql);
			if(rs.next()){//更新
				statement.executeUpdate(uadateSql);
			}//插入
			else{
				statement.executeUpdate(sql);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}