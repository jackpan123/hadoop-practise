package com.jackpan.practice;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URL;

/**
 * URLCatPractice操作类
 *
 * @author JackPan
 * @date 2022/06/11 08:07
 **/
public class URLCatPractice {

    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args) throws Exception {
//        URL url = new URL("hdfs://localhost/user/jackpan/sample_mnm.txt");
        URL url = new URL(args[0]);
        InputStream in = null;

        try {
            in = url.openStream();
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
