package com.cao.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.net.URI;

/**
 * Created by czf on 17-7-8.
 * 读取数据
 */
public class DoubleCat {
    public static void main(String [] args)throws Exception{
        String uri=args[0];
        Configuration configuration=new Configuration();
        FileSystem fs=FileSystem.get(URI.create(uri),configuration);
        FSDataInputStream in=null;
        try {
            in=fs.open(new Path(uri));
            IOUtils.copyBytes(in,System.out,4096,false);
            in.seek(3);
            IOUtils.copyBytes(in,System.out,4096,false);
        }finally {
            IOUtils.closeStream(in);
        }
    }
}
