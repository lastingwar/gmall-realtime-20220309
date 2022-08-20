package com.atguigu.gmall.util;

/**
 * @author yhm
 * @create 2022-08-20 15:07
 */
public class MysqlUtil {


    public static String getBaseDicDDL(){
        return "CREATE TEMPORARY TABLE base_dic (\n" +
                "  `dic_code` string,\n" +
                "  `dic_name` string,\n" +
                "  `parent_code` string,\n" +
                "  `create_time` string,\n" +
                "  `operate_time` string\n" +
                ") " + getMysqlDDL("base_dic");
    }

    public static String getMysqlDDL(String tableName){
        return "WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://hadoop102:3306/gmall',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '123456',\n" +
                "  'driver' = 'com.mysql.cj.jdbc.Driver', \n" +
                // 可以设置存活时间进行优化
//                "  'lookup.cache.max-rows' = '10',\n" +
//                "  'lookup.cache.ttl' = '1 hour',\n" +
                "  'table-name' = '" + tableName + "'\n" +
                ")";

    }
}
