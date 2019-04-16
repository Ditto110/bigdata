package example;

import dao.HBaseClient;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;

/**
 * @author ASUS
 * Created at 15:42.2019/4/14
 *
 * 通过scan 获取数据，同时通过不同的过滤器实现数据过滤
 *    使用过滤器来缩小范围：过滤器的使用
 *         scan.setFilter()
 *         RowFilter
 *         RandomRowFilter
 *         FamilyFilter
 *         QualifierFilter
 *         ColumnPrefixFilter
 *         SingleColumnValueFilter
 *         SingleColumnValueExcludeFilter
 *
 */
public class ScanExample {
    public static void main(String[] args) throws IOException {
        HBaseClient client = new HBaseClient();
        TableName tb = TableName.valueOf("hbTest");
        Table tableAd = client.getTable(tb);
        Scan scan = new Scan();
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,new BinaryComparator("row1".getBytes()));
        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator("info".getBytes()));
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filterList.addFilter(rowFilter);
        filterList.addFilter(familyFilter);
        scan.setFilter(filterList);
        ResultScanner scanner = tableAd.getScanner(scan);
        for (Result result : scanner) {
            boolean exist = result.containsColumn("info".getBytes(), "pv".getBytes());
            if (exist){
                byte[] value = result.getValue("info".getBytes(), "pv".getBytes());
                System.out.println(String.format("info:pv=%s", Bytes.toString(value)));
            }
        }
        tableAd.close();
    }

}
