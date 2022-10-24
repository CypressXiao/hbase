package HbaseDay01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @ClassName HbaseDemo1
 * @Description 查找hbase中所有的表
 * @Author 肖榆柏
 * @Date 2022/6/9 15:33
 * @Version 1.0
 */

public class HbaseDemo1 {
    public static void main(String[] args) throws IOException {
        //创建配置对象
        Configuration conf = HBaseConfiguration.create();
        //设置zookeeper的地址,可用通过zk连接到hbase数据库
        conf.set("hbase.zookeeper.quorum" , "hadoop001:2181,hadoop002:2181,hadoop003:2181");
        //创建hbase连接
        Connection conn = ConnectionFactory.createConnection(conf);
        //admin进行ddl操作的对象
        Admin admin = conn.getAdmin();
        //table进行dml操作的对象
        //Table table = conn.getTable(表名);
        //获取所有表名
        TableName[] tbs = admin.listTableNames();
        for (TableName tb : tbs) {
            String name = tb.getNameAsString();
            System.out.println(name);
        }
        conn.close();
    }
}
