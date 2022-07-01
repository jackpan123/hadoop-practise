package v1;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.*;

/**
 * @author jackpan
 * @version v1.0 2021/9/24 13:10
 */
public class MaxTemperatureReducerTest {

    @Test
    public void returnsMaximumIntegerInValues() throws IOException,
        InterruptedException {

        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new MaxTemperatureReducer())
                .withInput(new Text("1950"),
                        Arrays.asList(new IntWritable(10), new IntWritable(6)))
                .withOutput(new Text("1950"), new IntWritable(10))
                .runTest();
    }
}
