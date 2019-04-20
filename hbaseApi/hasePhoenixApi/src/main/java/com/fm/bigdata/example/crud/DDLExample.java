package com.fm.bigdata.example.crud;

import com.fm.bigdata.util.HBaseClientUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author ASUS
 * Created at 16:57.2019/4/20
 * DDL 操作
 */
public class DDLExample {

    /**
     * 建表
     */
    public void createTb() {
        try (
                Connection conn = HBaseClientUtils.getPhoenixConnection();
        ) {
            String sql = "select count(1) as num = ? from qTable = ?";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, "num");
            boolean execute = ps.execute(sql);
            ResultSet resultSet = ps.executeQuery();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
