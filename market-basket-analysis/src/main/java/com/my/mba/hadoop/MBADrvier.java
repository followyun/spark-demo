package com.my.mba.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * 购物篮分析MaperDuce
 * 包含3个job
 * 1. 计算原始交易数据的总大小
 * 2. 生成频繁项集
 * 3. 根据频繁项集生成关联规则
 */
public class MBADrvier extends Configured implements Tool {
    private final static String USAGE = "USAGE %s: <input dir path> <output dir path> <min. support> <min. confidence> <transaction delimiter>\n";
    private static String defFS; //hdfs基本路径
    private static String inputDir; //原始数据集在hdfs上的路径目录
    private static String outputDir; //中间数据以及最终结果数据的输出的HDFS目录
    private static int txnCount; //原始交易数据的总大小
    private static double minSupport; //最小支持度
    private static double minConfidence; //最小置信度
    private static String delimiter; // 原始交易数据item之间的分隔符

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MBADrvier(), args); //这里借助了hadoop工具集来写mapduce程序

        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        inputDir = args[0];
        outputDir = args[1];
        minSupport = Double.parseDouble(args[2]);
        minConfidence = Double.parseDouble(args[3]);
        delimiter = args[4];
        Configuration conf = new Configuration();
        defFS = conf.get("fs.defaultFS");
//       * 1. 计算原始交易数据的总大小
        txnCount = countTransaction();
        System.out.println("原始交易数据的总大小=" + txnCount);
        //计算最小支持数
        int minSupportCount = (int) Math.ceil(minSupport * txnCount);
//        * 2. 生成频繁项集
        jobFrequentItemsetMining(minSupportCount);
//        * 3. 根据频繁项集生成关联规则
        jobAssociationRuleMining();
        return 0;
    }

    /**
     * 计算原始交易数据的总大小
     *
     * @return 原始交易数据的总大小
     */
    private int countTransaction() throws IOException, ClassNotFoundException, InterruptedException {
        String hdfsInputPath = defFS + inputDir;
        System.out.println("hdfsInputPath=" + hdfsInputPath);
        String hdfsOutputPath = defFS + outputDir + "/transaction-count";
        System.out.println("hdfsOutputPath=" + hdfsOutputPath);
        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "Transaction Counter");
        job.setJarByClass(MBADrvier.class);
        job.setMapperClass(TxnCountMapper.class);
        job.setReducerClass(TxnCountReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(1); //指定reduce task个数

        FileSystem.get(config).delete(new Path(hdfsOutputPath), true);//如果目录已存在则先删除
        FileInputFormat.addInputPath(job, new Path(hdfsInputPath));
        FileOutputFormat.setOutputPath(job, new Path(hdfsOutputPath));

        boolean isSuccess = job.waitForCompletion(true);
        if (!isSuccess) {
            throw new IllegalStateException(" run Transaction Counter error");
        } else {
            System.out.println("run Transaction Counter success");
        }

        //从hdfs读出原始数据的总大小结果
        FileSystem fileSystem = FileSystem.get(config);
        FileStatus fileStatus = fileSystem.listStatus(new Path(hdfsOutputPath))[0];
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(fileStatus.getPath())));
        String txnCountStr = reader.readLine().trim();

        return Integer.parseInt(txnCountStr);
    }

    /**
     * 生成频繁项集
     *
     * @param minSupportCount 最小支持数
     */
    private void jobFrequentItemsetMining(int minSupportCount) throws IOException, ClassNotFoundException, InterruptedException {
        String hdfsInputPath = defFS + inputDir;
        System.out.println("hdfsInputPath=" + hdfsInputPath);
        String hdfsOutputPath = defFS + outputDir + "/frequent-itemset";
        System.out.println("hdfsOutputPath=" + hdfsOutputPath);
        Configuration config = new Configuration();
        //config的设置一定要在job实例化之前
        config.setInt("MIN_SUPPORT_COUNT", minSupportCount);
        config.set("DELIMITER", delimiter);
        Job job = Job.getInstance(config, "Frequent Itemset");
        job.setJarByClass(MBADrvier.class);
        job.setMapperClass(FrequentItemsetMapper.class);
        job.setReducerClass(FrequentItemsetReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setNumReduceTasks(1); //指定reduce task个数

        FileSystem.get(config).delete(new Path(hdfsOutputPath), true);//如果目录已存在则先删除
        FileInputFormat.addInputPath(job, new Path(hdfsInputPath));
        FileOutputFormat.setOutputPath(job, new Path(hdfsOutputPath));

        boolean isSuccess = job.waitForCompletion(true);
        if (!isSuccess) {
            throw new IllegalStateException(" run Frequent Itemset error");
        } else {
            System.out.println("run Frequent Itemset success");
        }
    }

    /**
     * 根据频繁项集生成关联规则
     */
    private void jobAssociationRuleMining() throws IOException, ClassNotFoundException, InterruptedException {
        String hdfsInputPath = defFS + outputDir + "/frequent-itemset";
        System.out.println("hdfsInputPath=" + hdfsInputPath);
        String hdfsOutputPath = defFS + outputDir + "/association-rule";
        System.out.println("hdfsOutputPath=" + hdfsOutputPath);
        Configuration config = new Configuration();
        //config的设置一定要在job实例化之前
        //config.setInt("MIN_SUPPORT_COUNT", minSupportCount);
        config.set("DELIMITER", delimiter);
        config.setDouble("MIN_CONFIDENCE", minConfidence);
        config.setInt("TRANSACTION_COUNT", txnCount);
        Job job = Job.getInstance(config, "Association Rule");
        job.setJarByClass(MBADrvier.class);
        job.setMapperClass(AssociationRuleMapper.class);
        job.setReducerClass(AssociationRuleReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1); //指定reduce task个数

        FileSystem.get(config).delete(new Path(hdfsOutputPath), true);//如果目录已存在则先删除
        FileInputFormat.addInputPath(job, new Path(hdfsInputPath));
        FileOutputFormat.setOutputPath(job, new Path(hdfsOutputPath));

        boolean isSuccess = job.waitForCompletion(true);
        if (!isSuccess) {
            throw new IllegalStateException(" run Association Rule error");
        } else {
            System.out.println("run Association Rule success");
        }
    }
}
