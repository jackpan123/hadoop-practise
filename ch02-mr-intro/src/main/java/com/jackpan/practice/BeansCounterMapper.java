package com.jackpan.practice;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * BeansCounterMapper操作类
 *
 * @author JackPan
 * @date 2022/05/30 10:57
 **/
public class BeansCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split(",");
        if (split.length == 3) {
            String color = split[1];
            String number = split[2];
            context.write(new Text(color), new IntWritable(Integer.parseInt(number)));
        }

    }
}
