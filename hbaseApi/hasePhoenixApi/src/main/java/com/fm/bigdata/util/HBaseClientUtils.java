package com.fm.bigdata.util;

import java.sql.*;

/**
 * @author ASUS
 * Created at 16:35.2019/4/20
 */
public class HBaseClientUtils {

    private static final String URL = "jdbc:phoenix:Ditto.com";

    public static Connection getPhoenixConnection(){
        try{
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            Connection connection = DriverManager.getConnection(URL);
            return connection;
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) throws SQLException {
        try (
                Connection conn = HBaseClientUtils.getPhoenixConnection();
                ){
            String tbName = "\"test\"";
            String sql = "select * from "+ tbName +" where \"age\" = ?";
            PreparedStatement ps = conn.prepareStatement(sql);
            //如果表名或字段是英文，需要加上" 以表示大小写敏感，对于数字则不需要
            ps.setString(1,"23");
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                System.out.println("result:"+resultSet.getString(1));
                System.out.println("result:"+resultSet.getString(2));
                System.out.println("result:"+resultSet.getString(3));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
