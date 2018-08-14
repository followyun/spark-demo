package com.my.mba.hadoop;

import com.my.mba.util.Combination;
import com.my.mba.util.Utils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

/**
 *  phase one：生成频繁项集的Map：
 *  输入是原始数据的每一条交易数据，输出是任何可能存在的项集的计数
 *  假设，输入为：
 *  a,b,c
 *  a,b,d
 *  b,c
 *  b,c
 *  则输出为：
 (a -> 1)
 (b -> 1)
 (c -> 1)
 (a,b -> 1)
 (a,c -> 1)
 (b,c -> 1)
 (a,b,c -> 1)
 (a -> 1)
 (b -> 1)
 (d -> 1)
 (a,b -> 1)
 (a,d -> 1)
 (b,d -> 1)
 (a,b,d -> 1)
 (b -> 1)
 (c -> 1)
 (b,c -> 1)
 (b -> 1)
 (c -> 1)
 (b,c -> 1)
 */
public class FrequentItemsetMapper extends Mapper<LongWritable,Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1、将每一条交易数据按照指定的分隔符进行分割，得到一个Item列表
        List<String> list = Utils.toList(value.toString(), ",");
        //2.生成并排序好这个item列表的所有子集
        List<List<String>> combinations = Combination.findSortedCombinations(list);
        //3.将每一个子集的出现的次数标记为1，然后输出到reduce中
        for(List<String> combination : combinations){
            if(combination.size() > 0)
                context.write(new Text(Utils.list2string(combination)), new LongWritable(1));
        }
    }

}
