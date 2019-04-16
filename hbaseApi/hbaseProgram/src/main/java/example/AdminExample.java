package example;

import dao.HBaseClient;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import java.io.IOException;

/**
 * @author ASUS
 * Created at 15:36.2019/4/14
 */
public class AdminExample {

    public static void main(String[] args) throws IOException {
        HBaseClient client = new HBaseClient();
        Admin admin = client.getAdmin();
//        createTable(admin);
//        dropTable(admin);
        modifyColumn(admin);
    }

    /**
     * 建表
     * @param admin
     * @throws IOException
     */
    private static  void  createTable(Admin admin) throws IOException {
        try {
            TableName hbTest = TableName.valueOf("hbTest");
            HTableDescriptor tableDescriptor = new HTableDescriptor(hbTest);
            tableDescriptor.addFamily(new HColumnDescriptor("info"));
            tableDescriptor.addFamily(new HColumnDescriptor("area"));
            tableDescriptor.addFamily(new HColumnDescriptor("education"));
            admin.createTable(tableDescriptor);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (admin != null){
                admin.close();
            }
        }
    }

    /**
     * 删表
     * @param admin
     * @throws IOException
     */
    private static  void  dropTable(Admin admin) throws IOException {
        try {
            TableName hbTest = TableName.valueOf("hbTest");
            admin.disableTable(hbTest);
            admin.deleteTable(hbTest);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (admin != null){
                admin.close();
            }
        }
    }

    /**
     * 列族的处理
     * @param admin
     * @throws IOException
     */
    private static void modifyColumn(Admin admin) throws IOException {
        try {
            TableName hbTest = TableName.valueOf("hbTest");
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(hbTest);
            //增加列族
//        tableDescriptor.addFamily(new HColumnDescriptor("other"));
            //删除列族
            tableDescriptor.removeFamily("other".getBytes());
            admin.modifyTable(hbTest,tableDescriptor);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (admin != null){
                admin.close();
            }
        }
    }
}
