package com.cao.java;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by czf on 17-6-11.
 */
public class Map extends Mapper<LongWritable,Text,Text,IntWritable> {
    public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
        String line=value.toString();//将文本的数据转换成string
        System.out.println(line);
        //将数据按行进行分割
        StringTokenizer tokenizerArticle = new StringTokenizer(line,"\n");
        while (tokenizerArticle.hasMoreTokens()){
            //每行按空格划分
            StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken());
            String strName = tokenizerLine.nextToken();//学生姓名部分
            String strScore = tokenizerLine.nextToken();//成绩部分
            Text name = new Text(strName);//学生姓名
            int scoreInt = Integer.parseInt(strScore);//成绩
            context.write(name,new IntWritable(scoreInt));
        }
    }
}
