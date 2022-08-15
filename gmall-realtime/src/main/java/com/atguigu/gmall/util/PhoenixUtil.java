package com.atguigu.gmall.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author yhm
 * @create 2022-08-15 15:25
 */
public class PhoenixUtil {
    public static void executeSql(String sql, Connection conn){
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = conn.prepareStatement(sql);
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            close(preparedStatement,conn);
        }
    }

    private static void close(PreparedStatement preparedStatement, Connection conn) {
       try {
           if (preparedStatement != null){
               preparedStatement.close();
           }
           if (conn != null ){
               conn.close();
           }
       } catch (SQLException e) {
           e.printStackTrace();
       }
    }

}
