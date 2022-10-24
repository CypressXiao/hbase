package HbaseDay01;

import HbaseUtils.HbaseUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @ClassName DMLOprete
 * @Description TODO
 * @Author 肖榆柏
 * @Date 2022/6/9 18:06
 * @Version 1.0
 */

public class DMLOperate1 {
    public static void main(String[] args) throws Exception {
        String tb = "tb";
        Connection conn = HbaseUtils.getHbaseConnection();
        Table table = HbaseUtils.getTable(conn, tb);

        //向表中添加数据
        //确定行
        Put put = new Put("2".getBytes());
        put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("score"),Bytes.toBytes(90.5));
        table.put(put);
        table.close();
        conn.close();

    }
}
