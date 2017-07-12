package com.cao.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 * Created by czf on 17-7-12.
 */
public class WordCountHbase {
    public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>{
        private IntWritable i=new IntWritable(1);
        public void map(LongWritable key,Text value,Context context)throws IOException
            ,InterruptedException{
            String s[] =value.toString().trim().split(" ");
            for (String m:s){
                context.write(new Text(m),i);
            }
        }
    }
    public static class Reduce extends TableReducer<Text,IntWritable,NullWritable>{
        public void reduce(Text key, Iterable<IntWritable> values, Context context)throws 
                IOException,InterruptedException{
            int sum=0;
            for (IntWritable i:values){
                sum+=i.get();
            }
            Put put=new Put(Bytes.toBytes(key.toString()));
            put.add(Bytes.toBytes("content"),Bytes.toBytes("count"),Bytes.toBytes(String.valueOf(sum)));
            context.write(NullWritable.get(),put);
        }
    }
    public static void createHbaseTable(String tablename)throws IOException{
        HTableDescriptor htd=new HTableDescriptor(tablename);
        HColumnDescriptor col=new HColumnDescriptor("content");
        htd.addFamily(col);
        HBaseConfiguration config=new HBaseConfiguration();
        HBaseAdmin admin=new HBaseAdmin(config);
        if (admin.tableExists(tablename)){
            System.out.println("table exists ,try recreate table!");
            admin.disableTable(tablename);
            admin.deleteTable(tablename);
        }
        System.out.println("create new table: "+tablename);
        admin.createTable(htd);
    }
    public static void main(String [] args)throws Exception{
        String tablename="wordcount";
        Configuration conf=new Configuration();
        conf.set(TableOutputFormat.OUTPUT_TABLE,tablename);
        createHbaseTable(tablename);
        String input=args[0];
        Job job=new Job(conf,"WordCount table with "+input);
        job.setJarByClass(WordCountHbase.class);
        job.setNumReduceTasks(3);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        FileInputFormat.addInputPath(job,new Path(input));
        System.exit(job.waitForCompletion(true)?0:1);

    }
}
