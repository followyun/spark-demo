package com.my.mba.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 计算原始交易数据总大小的Reducer
 */
public class TxnCountReducer extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable>{
    public long count = 0;
    @Override
    protected void reduce(LongWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        count += key.get();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new LongWritable(count), NullWritable.get());
    }
}
