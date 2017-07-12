package com.cao.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by czf on 17-7-12.
 */
public class HBaseTest {
    static Configuration cfg=HBaseConfiguration.create();
    public static void  create(String tanlename,String columnFamily)throws Exception{
        HBaseAdmin admin=new HBaseAdmin(cfg);
        if (admin.tableExists(tanlename)){
            System.out.println("table exists");
            System.exit(0);
        }else {
            HTableDescriptor tableDesc=new HTableDescriptor(tanlename);
            tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(tableDesc);
            System.out.println("create table success");
        }
    }
    public static void put(String tablename,String row,String columnFamily,
                           String column,String data)throws Exception{
        HTable table=new HTable(cfg,tablename);
        Put p1=new Put(Bytes.toBytes(row));
        p1.add(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(data));
        table.put(p1);
        System.out.println("put '"+row+"','"+columnFamily+":"+column+"','"+data+"'");
        
    }
    public static void get(String tablename,String row)throws Exception{
        HTable table=new HTable(cfg,tablename);
        Get get=new Get(Bytes.toBytes(row));
        Result result=table.get(get);
        System.out.println("get: "+result);
    }
    public static void scan(String tablename)throws Exception{
        HTable table=new HTable(cfg,tablename);
        Scan scan=new Scan();
        ResultScanner rs=table.getScanner(scan);
        for (Result r:rs){
            System.out.println("scan: "+r);
        }
    }
    public static boolean delete(String tablename)throws Exception{
        HBaseAdmin admin=new HBaseAdmin(cfg);
        if (admin.tableExists(tablename)) {
        
            try {
                admin.disableTable(tablename);
                admin.deleteTable(tablename);
            }catch (Exception e){
                e.printStackTrace();;
                return false;
            }
        }
        return true;
    }
    public static void main(String [] args){
        String tablename="hbase_tb";
        String columnFamily="cf";
        try {
            HBaseTest.create(tablename,columnFamily);
            HBaseTest.put(tablename,"row1",columnFamily,"cl1","data");
            HBaseTest.get(tablename,"row1");
            HBaseTest.scan(tablename);
            if (true==HBaseTest.delete(tablename)){
                System.out.println("delete table:"+tablename+"success");
            }
            
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
