package example;

import dao.HBaseClient;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import java.io.IOException;

/**
 * @author ASUS
 * Created at 17:46.2019/4/14
 */
public class DeleteExample {

    public static void main(String[] args) throws IOException {
        HBaseClient client = new HBaseClient();
       deleteData(client);
    }

    /**
     * 删除数据
     * @param client
     * @throws IOException
     */
    private static  void deleteData(HBaseClient client) throws IOException {
        Table tableAd = null;
        try {
            tableAd = client.getTable(TableName.valueOf("hbTest"));
            Delete delete = new Delete("row3".getBytes());
            //删除指定列的数据
//            delete.addColumn("info".getBytes(), "uv".getBytes());
            tableAd.delete(delete);
            System.out.println(String.format("delete data %s from %s","row4",tableAd.getName()));
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (tableAd != null){
                tableAd.close();
            }
        }
    }
}
