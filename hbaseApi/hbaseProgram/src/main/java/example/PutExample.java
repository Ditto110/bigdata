package example;

import dao.HBaseClient;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author ASUS
 * Created at 17:21.2019/4/14
 */
public class PutExample {
    public static void main(String[] args) throws IOException {

        HBaseClient client = new HBaseClient();
        Table tableAd = client.getTable(TableName.valueOf("hbTest".getBytes()));
        //写入单条数据
//        putData(tableAd);
        //写入多条数据
        putMulti(tableAd);
        tableAd.close();
    }

    /**
     * 写入单条数据
     * @param table
     * @throws IOException
     */
    private static void putData(Table table) throws IOException {
        Put put = new Put("row1".getBytes());
        put.addColumn("info".getBytes(), "pv".getBytes(),new String(1001+"").getBytes());
        put.addColumn("info".getBytes(), "uv".getBytes(),new String(1002+"").getBytes());
        put.addColumn("info".getBytes(), "tv".getBytes(),new String(1003+"").getBytes());
        table.put(put);
        System.out.println(String.format("put row1 to %s",table.getName()));
    }

    /**
     * 写入多条数据
     * @param table
     * @throws IOException
     */
    private static void  putMulti(Table table) throws IOException {
        Put putRow2 = new Put("row2".getBytes());
        putRow2.addColumn("info".getBytes(), "pv".getBytes(),new String(1001+"").getBytes());
        putRow2.addColumn("info".getBytes(), "uv".getBytes(),new String(1002+"").getBytes());
        putRow2.addColumn("info".getBytes(), "tv".getBytes(),new String(1003+"").getBytes());
        Put putRow3 = new Put("row3".getBytes());
        putRow3.addColumn("area".getBytes(), "now".getBytes(),new String("成都").getBytes());
        putRow3.addColumn("area".getBytes(), "from".getBytes(),new String("南充").getBytes());
        List<Put> put = new ArrayList<>();
        put.add(putRow2);
        put.add(putRow3);
        table.put(put);
        System.out.println(String.format("put %s,%s to %s","row2","row3",table.getName()));
    }

}
