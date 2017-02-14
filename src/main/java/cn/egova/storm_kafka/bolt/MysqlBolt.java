package cn.egova.storm_kafka.bolt;

import java.sql.*;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
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
	private String word;
	private Number num;
	//private String endtime;
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
		String word= tuple.getString(0);
		Number num = tuple.getLong(1);

		InsertDB(word, num);
	}

	private void InsertDB(String word, Number num) {
		// TODO Auto-generated method stub
		String sql =null;
		 sql = "insert into " + this.from+ "(word, num) values ('" +word+"',"+num+ ")";
		String selectSql = MessageFormat.format("select * from stormkafka where word = {0} and num = {1}", "'"+word+"'",""+num+"");
		String uadateSql = MessageFormat.format("update stormkafka set word = {0} where num = {1}", "'"+word+"'",""+num+"");
		try {
		    Statement	statement = conn.createStatement();
//			statement.executeUpdate(sql);
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