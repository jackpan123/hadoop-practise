package com.jackpan.practice;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * BeansCounter操作类
 *
 * @author JackPan
 * @date 2022/05/30 10:51
 **/
public class BeansCounter {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("You should <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(BeansCounter.class);
        job.setJobName("Bean Counter");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(BeansCounterMapper.class);
        job.setReducerClass(BeansCounterReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
