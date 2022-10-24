package HbaseDay01;

import HbaseUtils.HbaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @ClassName HbaseDemo
 * @Description 创建表并添加单列族
 * @Author 肖榆柏
 * @Date 2022/6/9 16:46
 */
public class HbaseCreateTab {
    public static void main(String[] args) throws Exception {
        Connection conn = HbaseUtils.getHbaseConnection();
        Admin admin = HbaseUtils.getAdmin(conn);
        createTabPreRegion(admin);

        admin.close();
        conn.close();
    }

    /**
     * @description: 创建分区表
     * @param: admin
     * @author 肖榆柏
     * @date: 2022/6/9 17:24
     */

    private static void createTabPreRegion(Admin admin) throws IOException {
        TableDescriptorBuilder tb3_java = TableDescriptorBuilder.newBuilder(TableName.valueOf("tb3_java"));
        //列描述器构建器
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder("cf1".getBytes());
        //列描述器
        ColumnFamilyDescriptor cf = columnFamilyDescriptorBuilder.build();
        //设置列族
        tb3_java.setColumnFamily(cf);
        TableDescriptor tableDescriptor = tb3_java.build();
        byte[][] bytes = {"f".getBytes(), "o".getBytes()};
        admin.createTable(tableDescriptor, bytes);
    }

    /**
     * @description: 创建表并设置属性
     * @param: admin对象
     * @author 肖榆柏
     * @date: 2022/6/9 17:18
     */

    private static void setProperty(Admin admin) throws IOException {
        TableDescriptorBuilder tb2_java = TableDescriptorBuilder.newBuilder(TableName.valueOf("tb2_java"));
        //列描述器构建器
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder("cf1".getBytes());
        //设置列族属性
        columnFamilyDescriptorBuilder.setMaxVersions(4);
        columnFamilyDescriptorBuilder.setTimeToLive(200);
        //列描述器
        ColumnFamilyDescriptor cf1 = columnFamilyDescriptorBuilder.build();
        //设置列族
        tb2_java.setColumnFamily(cf1);
        TableDescriptor tableDescriptor = tb2_java.build();
        admin.createTable(tableDescriptor);
    }

    /**
     * @description: 创建表包含多个列族
     * @param: admin
     * @author 肖榆柏
     * @date: 2022/6/9 17:12
     */

    private static void createTableMultiCol(Admin admin) throws IOException {
        TableDescriptorBuilder tb1_java = TableDescriptorBuilder.newBuilder(TableName.valueOf("tb1_java"));
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder("cf1".getBytes());
        ColumnFamilyDescriptor cf1 = columnFamilyDescriptorBuilder.build();

        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder1 = ColumnFamilyDescriptorBuilder.newBuilder("cf2".getBytes());
        ColumnFamilyDescriptor cf2 = columnFamilyDescriptorBuilder1.build();

        List<ColumnFamilyDescriptor> list = Arrays.asList(cf1, cf2);
        tb1_java.setColumnFamilies(list);

        TableDescriptor tableDescriptor = tb1_java.build();
        admin.createTable(tableDescriptor);
    }

    /**
     * @description: Hbase创建表, 包含单个列族
     * @param: Admin对象
     * @author 肖榆柏
     * @date: 2022/6/9 17:11
     */

    private static void createTableSingleCol(Admin admin) throws IOException {
        TableDescriptorBuilder tb_java = TableDescriptorBuilder.newBuilder(TableName.valueOf("tb_java"));
        //列描述器构建器
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder("cf1".getBytes());
        //列描述器
        ColumnFamilyDescriptor cf = columnFamilyDescriptorBuilder.build();
        //设置列族
        tb_java.setColumnFamily(cf);
        TableDescriptor tableDescriptor = tb_java.build();
        admin.createTable(tableDescriptor);
    }
}
