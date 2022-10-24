package HbaseDay01;

import HbaseUtils.HbaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName DMLOperate2
 * @Description TODO
 * @Author 肖榆柏
 * @Date 2022/6/10 10:17
 * @Version 1.0
 */

public class DMLOperate2 {
    public static void main(String[] args) throws Exception {
        Connection conn = HbaseUtils.getHbaseConnection();


        conn.close();

    }

    /**
     * @description: 全表查询
     * @param: conn对象
     * @author 肖榆柏
     * @date: 2022/6/10 14:40
     */

    private static void ScanTab(Connection conn) throws Exception {
        Table tb = HbaseUtils.getTable(conn, "tb");
        Scan scan = new Scan();
        //scan.addFamily("cf1".getBytes()); 指定查询的列族

        ResultScanner scanner = tb.getScanner(scan);
        for (Result result : scanner) {
            HbaseUtils.showData(result);
        }
    }

    /**
     * @description: 得到Hbase中指定行的数据
     * @param: table的对象
     * @author 肖榆柏
     * @date: 2022/6/10 11:48
     */

    private static void getData(Table tb) throws IOException {
        Get get = new Get("2".getBytes());
        Result result = tb.get(get);
        //result就是一个迭代器
        HbaseUtils.showData(result);
    }

    /**
     * @description: 缓存输出
     * @param: 连接对象
     * @author 肖榆柏
     * @date: 2022/6/10 11:06
     */

    private static void CachePut(Connection conn) throws IOException {
        BufferedMutator mutator = conn.getBufferedMutator(TableName.valueOf("tb"));

        Put put1 = new Put("1".getBytes());
        put1.addColumn("cf1".getBytes(), "gender".getBytes(), "男".getBytes());

        Put put2 = new Put("2".getBytes());
        put2.addColumn("cf1".getBytes(), "name".getBytes(), "lss".getBytes());
        List<Mutation> list = new ArrayList<>();
        list.add(put1);
        list.add(put2);

        mutator.mutate(list);
        mutator.flush();
    }
}
