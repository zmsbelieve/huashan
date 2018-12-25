package com.atguigu.mr.worldcount;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WorldCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    private Text k = new Text();
    private IntWritable v = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");
//        Arrays.stream(words).forEach((w)->{
//            k.set(w);
//            context.write(k,v);
//        });
        for(String w : words){
            if(!StringUtils.isEmpty(w)){
                k.set(w);
                context.write(k,v);
            }
        }
//        context.write(new Text(words[0]),new IntWritable(Integer.parseInt(words[1])) );
    }
}
