package HbaseUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * @ClassName HbaseUtils
 * @Description TODO
 * @Author 肖榆柏
 * @Date 2022/6/9 15:55
 * @Version 1.0
 */

public class HbaseUtils {

    public static Connection getHbaseConnection() throws Exception {
        //创建配置对象
        Configuration conf = HBaseConfiguration.create();
        //设置zookeeper的地址,可用通过zk连接到hbase数据库
        conf.set("hbase.zookeeper.quorum" , "hadoop001:2181,hadoop002:2181,hadoop003:2181");
        //创建hbase连接
        return ConnectionFactory.createConnection(conf);
    }

    public static Admin getAdmin(Connection conn) throws Exception {
        return conn.getAdmin();
    }

    public static Table getTable(Connection conn,String name) throws Exception {
        return conn.getTable(TableName.valueOf(name));
    }

    /**
     * 解析hbase中的行数据
     * @param result
     */
    public static void showData(Result result){
        while (result.advance()) {
            Cell cell = result.current();
            byte[] row = CellUtil.cloneRow(cell);
            byte[] family = CellUtil.cloneFamily(cell);
            byte[] qualifier = CellUtil.cloneQualifier(cell);
            byte[] value = CellUtil.cloneValue(cell);

            if (!"score".equals(Bytes.toString(qualifier))) {
                System.out.println("行:" + Bytes.toString(row) + " 列族:" + Bytes.toString(family) + " 属性:" + Bytes.toString(qualifier)
                        + " 值:" + Bytes.toString(value));
            } else {
                System.out.println("行:" + Bytes.toString(row) + " 列族:" + Bytes.toString(family) + " 属性:" + Bytes.toString(qualifier)
                        + " 值:" + Bytes.toDouble(value));
            }
        }
    }
}
