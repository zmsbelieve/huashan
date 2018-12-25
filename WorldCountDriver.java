package com.atguigu.mr.worldcount;

import com.atguigu.constant.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class WorldCountDriver {
    public static Logger logger = LoggerFactory.getLogger(WorldCountDriver.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置jar 关联mapper、reducer
        job.setJarByClass(WorldCountDriver.class);
        job.setMapperClass(WorldCountMapper.class);
        job.setReducerClass(WorldCountReducer.class);
        //设置mapper输出的key、value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置最终输出的key、value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);
        long endTime = new DateTime(System.currentTimeMillis()).toDateMidnight().getMillis();
        long startTime = new DateTime(System.currentTimeMillis()).toDateMidnight().plusDays(-7).getMillis();
        FileSystem fileSystem = FileSystem.get(conf);
        while(startTime <= endTime){
            String fileName = Constants.path + new DateTime(startTime).toString("yyyy-MM-dd");
            Path path = new Path(fileName);
            if(fileSystem.exists(path)){
                FileInputFormat.addInputPath(job, path);
            }else{
                logger.info("fileName:{} is not exists",fileName );
            }
            startTime = startTime + Constants.ONE_DAY;
        }
//        FileInputFormat.addInputPath(job, new Path(Constants.path+"2018-12-19" ));
//        FileInputFormat.addInputPath(job, new Path(Constants.path+"2018-12-20" ));
        if(fileSystem.exists(new Path(args[0]))){
            fileSystem.delete(new Path(args[0]),true );
        }
        //设置文件的输入、输出位置
        FileOutputFormat.setOutputPath(job,new Path(args[0]));
        //提交job
//        job.submit();
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
