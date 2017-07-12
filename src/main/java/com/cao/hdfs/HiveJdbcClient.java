package com.cao.hdfs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by czf on 17-7-12.
 */
public class HiveJdbcClient {
    private static String driverName =
            "org.apache.hive.jdbc.HiveDriver";
    public static void main(String[] args)throws Exception{
        try {
            Class.forName(driverName);
        }catch (ClassNotFoundException e){
            e.printStackTrace();
            System.exit(1);
        }
        Connection con= DriverManager.getConnection("jdbc:hive2://localhost:10000/default", 
                "root", "000000");
        Statement stmt=con.createStatement();
        String tableName="u1_data";
        stmt.execute("drop table "+tableName);
      
        boolean res1=stmt.execute("create table "+ tableName + "(userid int, " +
                "movieid int ," +
                "city string," +
                "viewTime string) " +
                "row format delimited " +
                "fields terminated by '\t' " +
                "stored as textfile");
        ResultSet res;
         String sql="show tables";
        System.out.println("running: "+sql+":");
        res=stmt.executeQuery(sql);
        if (res.next()){
            System.out.println(res.getString(1));
        }
        sql="describe "+tableName;
        System.out.println("running: "+sql);
        res=stmt.executeQuery(sql);
        while (res.next()){
            System.out.println(res.getString(1)+"\t"+res.getString(2));
        }
        //load
        String filepath="/home/czf/m/u.data";
        sql="load data local inpath '"+filepath+"'overwrite into table  "+tableName;
        System.out.println("running: "+sql);
        res1=stmt.execute(sql);
        //select
        sql="select * from "+tableName+" limit 5";
        System.out.println("running: "+sql);
        res=stmt.executeQuery(sql);
        while (res.next()){
            System.out.println(String.valueOf(res.getString(3)+"\t"+
                    res.getString(4)));
        }
        sql="select count(*) from "+tableName;
        System.out.println("running: "+sql);
        res=stmt.executeQuery(sql);
        while (res.next()){
            System.out.println(res.getString(1));
        }
    }
}
