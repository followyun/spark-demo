package com.my.submit;

import org.apache.spark.launcher.SparkLauncher;

import java.io.File;
import java.io.IOException;

/**
 * 以java代码的方式提交spark应用
 */
public class ProcessCodeSubmitSparkTest {
    public static void main(String[] args) throws InterruptedException, IOException {
        Process process = new SparkLauncher()
                .setAppName("ProcessCodeSubmitSparkTest")
                .setAppResource("F:\\BigData\\workspace\\spark-demo\\spark-core\\target\\spark-core-1.0-SNAPSHOT.jar")
                .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
                .setMainClass("com.my.spark.project.topN.TopN")
                .setMaster("local[*]")
                .redirectError()
                .redirectOutput(new File("F:\\BigData\\workspace\\spark-demo\\spark-submit\\src\\main\\resources")).launch();

        process.waitFor();
    }
}
