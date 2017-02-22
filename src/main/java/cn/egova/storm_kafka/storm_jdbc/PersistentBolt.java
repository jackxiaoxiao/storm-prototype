package cn.egova.storm_kafka.storm_jdbc;

import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.guava.collect.Lists;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.bolt.JdbcLookupBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcLookupMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;

import backtype.storm.tuple.Fields;

public class PersistentBolt {
    private static Map<String,Object> hikariConfigMap = new HashMap<String, Object>(){{
        put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        put("dataSource.url", "jdbc:mysql://192.168.101.17/test");
        put("dataSource.user","egova");
        put("dataSource.password", "egova");
    }};
    public static ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

    public static JdbcInsertBolt getJdbcInsertBolt(){
        //使用tablename进行插入数据，需要指定表中的所有字段
            /*String tableName="userinfo";
            JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);
            JdbcInsertBolt jdbcInsertBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                                            .withTableName("userinfo")
                                            .withQueryTimeoutSecs(50);*/
        //使用schemaColumns，可以指定字段要插入的字段
        List<Column> schemaColumns = Lists.newArrayList(new Column("user_id", Types.VARCHAR),
                new Column("resource_id", Types.VARCHAR), new Column("count", Types.INTEGER));
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(schemaColumns);
        JdbcInsertBolt jdbcInsertBolt = new JdbcInsertBolt(connectionProvider,simpleJdbcMapper)
                .withInsertQuery("insert into userinfo(id,user_id,resource_id,count) values(?,?,?)")
                .withQueryTimeoutSecs(50);
        return jdbcInsertBolt;
    }

    public static JdbcLookupBolt getJdbcLookupBlot(){
        //查询
        //指定bolt的输出字段
        Fields outputFields = new Fields("user_id","resource_id","count");
        //指定查询条件字段
        List<Column> queryColumns = Lists.newArrayList(new Column("user_id", Types.VARCHAR),new Column("resource_id",Types.VARCHAR));
        String selectSql = "select count from userinfo where user_id=? and resource_id=?";
        SimpleJdbcLookupMapper lookupMapper = new SimpleJdbcLookupMapper(outputFields, queryColumns);
        JdbcLookupBolt jdbcLookupBolt  = new JdbcLookupBolt(connectionProvider, selectSql, lookupMapper);
        return jdbcLookupBolt;
    }
}
