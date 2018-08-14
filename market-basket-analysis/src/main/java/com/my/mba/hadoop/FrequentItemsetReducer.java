package com.my.mba.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 频繁项集Reducer
 */
public class FrequentItemsetReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long totalVaule = 0L;
        for (LongWritable value : values) {
            totalVaule += value.get();
        }
        long minSupportCount = context.getConfiguration().getInt("MIN_SUPPORT_COUNT", 1);
        //只加入大于等于支持数的数据
        if(totalVaule >= minSupportCount) {
            context.write(key, new LongWritable(totalVaule));
        }
    }
}
