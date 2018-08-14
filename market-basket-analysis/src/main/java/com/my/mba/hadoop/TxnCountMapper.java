package com.my.mba.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 计算原始交易数据总大小的Mapper
 */
public class TxnCountMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable>{
    public long count = 0;//记录数
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        count += 1;//每过滤一行，记录数+1
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new LongWritable(count), NullWritable.get());
    }
}
