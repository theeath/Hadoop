package com.cao.java;

import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;


/**
 * Created by czf on 17-6-11.
 */
public class Score_Process extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(Score_Process.class);
        job.setJobName("Score_Process");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }
    public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        public void reduce(Text key, Iterable<IntWritable> values,Context context)throws IOException,InterruptedException{
            int sum = 0;
            int count = 0;
            Iterator<IntWritable> iterator = values.iterator();
            while (iterator.hasNext()){
                sum +=iterator.next().get();//计算总分
                count++;//统计总的科目
            }
            int average = (int)sum/count;//平均成绩
            context.write(key,new IntWritable(average));
        }
    }



    public static class Map extends Mapper<LongWritable,Text,Text,IntWritable> {
        public void map(LongWritable key, org.apache.hadoop.io.Text value, Context context)throws IOException,InterruptedException{
            String line=value.toString();//将文本的数据转换成string
            System.out.println(line);
            //将数据按行进行分割
            StringTokenizer tokenizerArticle = new StringTokenizer(line,"\n");
            while (tokenizerArticle.hasMoreTokens()){
                //每行按空格划分
                StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken());
                String strName = tokenizerLine.nextToken();//学生姓名部分
                String strScore = tokenizerLine.nextToken();//成绩部分
                Text name =new Text(strName);//学生姓名
                int scoreInt = Integer.parseInt(strScore);//成绩
                context.write(name,new IntWritable(scoreInt));
            }
        }
    }
    public  static void main(String [] args)throws Exception{
        int ret = ToolRunner.run(new Score_Process(),args);
        System.exit(ret);
    }

}
