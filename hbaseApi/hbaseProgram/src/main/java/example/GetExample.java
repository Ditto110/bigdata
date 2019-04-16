package example;

import dao.HBaseClient;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author ASUS
 * Created at 15:56.2019/4/14
 * 可以获取相对scang更加精确的数据
 *          LESS,
 *         LESS_OR_EQUAL,
 *         EQUAL,
 *         NOT_EQUAL,
 *         GREATER_OR_EQUAL,
 *         GREATER,
 *         NO_OP;
 */
public class GetExample {

    public static void main(String[] args) throws IOException {
        HBaseClient client = new HBaseClient();
        //获取单条数据
//        getSingle(client);
        //获取多条数据
        getMultiData(client);
    }

    /**
     * 获取单条数据
     * @param client
     * @throws IOException
     */
    private static  void  getSingle(HBaseClient client) throws IOException {
        Table tableAd = null;
        try {
            tableAd = client.getTable(TableName.valueOf("hbTest"));
            Get row1 = new Get("row1".getBytes());
            row1.addFamily("info".getBytes());
            row1.addFamily("area".getBytes());
            Result result = tableAd.get(row1);
            byte[] pv = result.getValue("info".getBytes(), "pv".getBytes());
            byte[] uv = result.getValue("info".getBytes(), "uv".getBytes());
            System.out.println( String.format("result info:pv %s,uv %s", Bytes.toString(pv),Bytes.toString(uv)));
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (tableAd !=null){
                tableAd.close();
            }
        }
    }

    /**
     * 获取多条数据
     * @param client
     * @throws IOException
     */
    private static  void getMultiData(HBaseClient client) throws IOException {
        Table tableAd = null;
        try {
            tableAd = client.getTable(TableName.valueOf("hbTest"));
            //获取指定行的数据
            Get row1 = new Get("row1".getBytes());
            //获取指定列族的数据
            row1.addFamily("info".getBytes());
            //获取指定行的数据
            Get row2 = new Get("row3".getBytes());
            List<Get> gets = new ArrayList<Get>();
            gets.add(row1);
            gets.add(row2);
            Result[] resultList = tableAd.get(gets);
            for (Result result : resultList) {
                byte[] pv = result.getValue("info".getBytes(), "pv".getBytes());
                byte[] from = result.getValue("area".getBytes(), "from".getBytes());
                byte[] now = result.getValue("area".getBytes(), "now".getBytes());
                System.out.println( String.format("resultList info:pv %s,area from:%s,area now:%s",
                        Bytes.toString(pv),Bytes.toString(from),Bytes.toString(now)));
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (tableAd !=null){
                tableAd.close();
            }
        }
    }
}
