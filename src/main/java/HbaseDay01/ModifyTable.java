package HbaseDay01;

import HbaseUtils.HbaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

/**
 * @ClassName ModifyTable
 * @Description 修改表
 * @Author 肖榆柏
 * @Date 2022/6/9 17:26
 * @Version 1.0
 */

public class ModifyTable {
    public static void main(String[] args) throws Exception {
        Connection conn = HbaseUtils.getHbaseConnection();
        Admin admin = HbaseUtils.getAdmin(conn);

        admin.close();
        conn.close();
    }

    /**
     * @description: 删除列族
     * @param:admin对象
     * @author 肖榆柏
     * @date: 2022/6/9 17:52
     */

    private static void delCol(Admin admin) throws IOException {
        TableName tb1_java = TableName.valueOf("tb1_java");
        //删除列族
        admin.deleteColumnFamily(tb1_java,"nf".getBytes());
    }

    /**
     * @description: 修改列族的属性
     * @param: admin对象
     * @author 肖榆柏
     * @date: 2022/6/9 17:50
     */

    private static void modifyTab(Admin admin) throws IOException {
        TableName tb1_java = TableName.valueOf("tb1_java");
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder("nf".getBytes());
        //修改版本
        columnFamilyDescriptorBuilder.setMaxVersions(4);
        ColumnFamilyDescriptor build = columnFamilyDescriptorBuilder.build();
        admin.modifyColumnFamily(tb1_java,build);
    }

    /**
     * @description: 增加列族
     * @param: admin对象
     * @author 肖榆柏
     * @date: 2022/6/9 17:44
     */

    private static void addCol(Admin admin) throws IOException {
        TableName tb1_java = TableName.valueOf("tb1_java");
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder("nf".getBytes());
        ColumnFamilyDescriptor build = columnFamilyDescriptorBuilder.build();
        admin.addColumnFamily(tb1_java, build);
    }
}
