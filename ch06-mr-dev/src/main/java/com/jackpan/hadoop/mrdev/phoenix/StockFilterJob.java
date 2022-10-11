package com.jackpan.hadoop.mrdev.phoenix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.phoenix.mapreduce.PhoenixOutputFormat;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;

import java.io.IOException;

/**
 * StockFilterJob操作类
 *
 * @author JackPan
 * @date 2022/10/10 10:59
 **/
public class StockFilterJob {

        public static void main(String[] args) throws Exception {
//            if (args.length != 2) {
//                System.err.println("Usage: com.jackpan.hadoop.mrdev.phoenix.StockFilterJob <input path> <output path>");
//                System.exit(-1);
//            }

            Configuration configuration = HBaseConfiguration.create();
            final Job job = Job.getInstance(configuration, "phoenix-mr-job");

// We can either specify a selectQuery or ignore it when we would like to retrieve all the columns
            final String selectQuery = "SELECT STOCK_NAME,RECORDING_YEAR,RECORDINGS_QUARTER FROM JACKPAN.STOCK ";

// com.jackpan.hadoop.mrdev.phoenix.StockWritable is the DBWritable class that enables us to process the Result of the above query
            PhoenixMapReduceUtil.setInput(job, StockWritable.class, "STOCK",  selectQuery);

// Set the target Phoenix table and the columns
            PhoenixMapReduceUtil.setOutput(job, "JACKPAN.STOCK", "STOCK_NAME,MAX_RECORDING");

            job.setMapperClass(StockMapper.class);
            job.setReducerClass(StockReducer.class);
            job.setOutputFormatClass(PhoenixOutputFormat.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(DoubleWritable.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(StockWritable.class);
//            TableMapReduceUtil.addDependencyJars(job);
            job.setJarByClass(StockFilterJob.class);
            job.waitForCompletion(true);
        }


    public static class StockMapper extends Mapper<NullWritable, StockWritable, Text, DoubleWritable> {

        private Text stock = new Text();
        private DoubleWritable price = new DoubleWritable();

        @Override
        protected void map(NullWritable key, StockWritable stockWritable, Context context) throws IOException, InterruptedException {
            double[] recordings = stockWritable.getRecordings();
            final String stockName = stockWritable.getStockName();
            double maxPrice = Double.MIN_VALUE;
            for (double recording : recordings) {
                if (maxPrice < recording) {
                    maxPrice = recording;
                }
            }
            stock.set(stockName);
            price.set(maxPrice);
            context.write(stock, price);
        }
    }


    public static class StockReducer extends Reducer<Text, DoubleWritable, NullWritable, StockWritable> {

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> recordings, Context context) throws IOException, InterruptedException {
            double maxPrice = Double.MIN_VALUE;
            for(DoubleWritable recording : recordings) {
                if(maxPrice < recording.get()) {
                    maxPrice = recording.get();
                }
            }
            final StockWritable stock = new StockWritable();
            stock.setStockName(key.toString());
            stock.setMaxPrice(maxPrice);
            context.write(NullWritable.get(),stock);
        }

    }
}
