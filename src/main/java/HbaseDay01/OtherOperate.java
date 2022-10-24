package HbaseDay01;

import HbaseUtils.HbaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @ClassName OtherOperate
 * @Description TODO
 * @Author 肖榆柏
 * @Date 2022/6/9 17:53
 * @Version 1.0
 */

public class OtherOperate {
    public static void main(String[] args) throws Exception {
        //删除表
        tabDel();

    }

    /**
     * @description: 删除表
     * @param: null
     * @author 肖榆柏
     * @date: 2022/6/9 18:04
     */

    private static void tabDel() throws Exception {
        Connection conn = HbaseUtils.getHbaseConnection();
        Admin admin = HbaseUtils.getAdmin(conn);
        TableName tb1_java = TableName.valueOf("tb1_java");
        if (admin.tableExists(tb1_java)) {
            if (!admin.isTableDisabled(tb1_java)) {
                admin.disableTable(tb1_java);
            }
            admin.deleteTable(tb1_java);
        }

        conn.close();
        admin.close();
    }
}
