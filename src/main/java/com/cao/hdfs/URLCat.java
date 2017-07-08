package com.cao.hdfs;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URL;

/**
 * Created by czf on 17-7-8.
 * 读取数据
 */
public class URLCat {
    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }
    public static void main(String [] args)throws Exception{
        InputStream in =null;
        try {
            //URL类中的openStream()方法，可以读取一个URL对象所指定的资源，返回一个InputStream对象。
            in=new URL(args[0]).openStream();
            IOUtils.copyBytes(in,System.out,4096,false);
        }finally {
            IOUtils.closeStream(in);
        }
    }
}
