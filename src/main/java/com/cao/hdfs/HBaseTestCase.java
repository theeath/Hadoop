package com.cao.hdfs;


import  java.io.IOException;
        import java.util.ArrayList;
        import java.util.List;

        import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTestCase {

    private static final String TABLE_NAME = "demo_table";

    public static Configuration conf = null;
    public HTable table = null;
    public HBaseAdmin admin = null;

    static {
        conf = HBaseConfiguration.create();
        System.out.println(conf.get("hbase.zookeeper.quorum")) ;
    }

    /**
     * 创建一张表
     */
    public static void creatTable(String tableName, String[] familys)
            throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)) {
            System.out.println("table already exists!");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (int i = 0; i < familys.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
            }
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName + " ok.");
        }
    }

    /**
     * 删除表
     */
    public static void deleteTable(String tableName) throws Exception {
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("delete table " + tableName + " ok.");
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
    }

    /**
     * 插入一行记录
     */
    public static void addRecord(String tableName, String rowKey,
                                 String family, String qualifier, String value) throws Exception {
        try {
            HTable table = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),
                    Bytes.toBytes(value));
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table "
                    + tableName + " ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除一行记录
     */
    public static void delRecord(String tableName, String rowKey)
            throws IOException {
        HTable table = new HTable(conf, tableName);
        List list = new ArrayList();
        Delete del = new Delete(rowKey.getBytes());
        list.add(del);
        table.delete(list);
        System.out.println("del recored " + rowKey + " ok.");
    }

    /**
     * 查找一行记录
     */
    public static void getOneRecord(String tableName, String rowKey)
            throws IOException {
        HTable table = new HTable(conf, tableName);
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        for (KeyValue kv : rs.raw()) {
            System.out.print(new String(kv.getRow()) + " ");
            System.out.print(new String(kv.getFamily()) + ":");
            System.out.print(new String(kv.getQualifier()) + " ");
            System.out.print(kv.getTimestamp() + " ");
            System.out.println(new String(kv.getValue()));
        }
    }

    /**
     * 显示所有数据
     */
    public static void getAllRecord(String tableName) {
        try {
            HTable table = new HTable(conf, tableName);
            Scan s = new Scan();
            ResultScanner ss = table.getScanner(s);
            for (Result r : ss) {
                for (KeyValue kv : r.raw()) {
                    System.out.print(new String(kv.getRow()) + " ");
                    System.out.print(new String(kv.getFamily()) + ":");
                    System.out.print(new String(kv.getQualifier()) + " ");
                    System.out.print(kv.getTimestamp() + " ");
                    System.out.println(new String(kv.getValue()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        // TODO Auto-generated method stub
        try {
            String tablename = "scores";
            String[] familys = { "grade", "course" };
            HBaseTestCase.creatTable(tablename, familys);

            // add record zkb
            HBaseTestCase.addRecord(tablename, "zkb", "grade", "", "5");
            HBaseTestCase.addRecord(tablename, "zkb", "course", "", "90");
            HBaseTestCase.addRecord(tablename, "zkb", "course", "math", "97");
            HBaseTestCase.addRecord(tablename, "zkb", "course", "art", "87");
            // add record baoniu
            HBaseTestCase.addRecord(tablename, "baoniu", "grade", "", "4");
            HBaseTestCase
                    .addRecord(tablename, "baoniu", "course", "math", "89");

            System.out.println("===========get one record========");
            HBaseTestCase.getOneRecord(tablename, "zkb");

            System.out.println("===========show all record========");
            HBaseTestCase.getAllRecord(tablename);

            System.out.println("===========del one record========");
            HBaseTestCase.delRecord(tablename, "baoniu");
            HBaseTestCase.getAllRecord(tablename);

            System.out.println("===========show all record========");
            HBaseTestCase.getAllRecord(tablename);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}