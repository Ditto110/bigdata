package dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import java.io.IOException;

/**
 * @author ASUS
 * Created at 15:01.2019/4/14
 */
public class HBaseClient {

    private Connection conn;
    private final static String ZK_URL = "hbase.zookeeper.quorum";

    public HBaseClient() {
        Configuration configuration = HBaseConfiguration.create();
        Configuration config = configuration;
        config.set(ZK_URL,"192.168.128.132");
        try {
            this.conn = ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * ddl 操作是通过admin 执行
     * @return
     */
    public Admin getAdmin(){
        try {
            return conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * dml 操作是通过table 执行
     * @param tableName
     * @return
     */
    public Table getTable(TableName tableName){
        try {
            return conn.getTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return  null;
    }

    public void upsert(String tableName, HBaseRecord record){
        final Put put = new Put(record.getRowKey().getBytes(), record.getVersion());

        record.getData().forEach((k,v)-> put.addColumn("info".getBytes(),k.getBytes(),v.getBytes()));
        try ( Table table= getTable(TableName.valueOf(tableName.getBytes()))){
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
