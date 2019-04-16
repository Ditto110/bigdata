package dao;

import java.util.HashMap;
import java.util.Map;

/**
 * @author ASUS
 * Created at 15:13.2019/4/14
 */
public class HBaseRecord {

    private String rowKey;
    private Map<String, String> data = new HashMap<>();
    private long version = System.currentTimeMillis();

    public HBaseRecord(String rowKey, Map<String, String> data) {
        this.rowKey = rowKey;
        this.data = data;
    }

    public HBaseRecord(String rowKey, Map<String, String> data, long version) {
        this.rowKey = rowKey;
        this.data = data;
        this.version = version;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public Map<String, String> getData() {
        return data;
    }

    public void setData(Map<String, String> data) {
        this.data = data;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return "dao.HBaseRecord{" +
                "rowKey='" + rowKey + '\'' +
                ", data=" + data +
                ", version=" + version +
                '}';
    }
}
