package com.atguigu.mr.worldcount;

import com.atguigu.constant.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class WorldCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private Logger logger = LoggerFactory.getLogger(WorldCountReducer.class);
    IntWritable v = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        logger.info("key:{}",key.toString());
        int sum = 0;
        for(IntWritable v : values){
            sum += v.get();
        }
        v.set(sum);
        context.write(key,v);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("清理操作....");
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        long endTime = new DateTime(System.currentTimeMillis()).toDateMidnight().plusDays(-8).getMillis();
        long startTime = new DateTime(System.currentTimeMillis()).toDateMidnight().plusDays(-11).getMillis();
        while(startTime <= endTime){
            String fileName = Constants.path + new DateTime(startTime).toString("yyyy-MM-dd");
            fileSystem.delete(new Path(fileName),true );
            logger.info("删除{}..",fileName );
            startTime = startTime + Constants.ONE_DAY;

        }

    }
}
