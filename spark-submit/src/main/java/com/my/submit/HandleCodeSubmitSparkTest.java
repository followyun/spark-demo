package com.my.submit;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.File;
import java.io.IOException;

/**
 * java代码启动spark应用方式二
 */
public class HandleCodeSubmitSparkTest {
    public static void main(String[] args) throws IOException {
        SparkAppHandle handle = new SparkLauncher()
                .setAppName("ProcessCodeSubmitSparkTest")
                .setAppResource("F:\\BigData\\workspace\\spark-demo\\spark-core\\target\\spark-core-1.0-SNAPSHOT.jar")
                .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
                .setMainClass("com.my.spark.project.topN.TopN")
                .setMaster("local[*]")
                .redirectError()
                .redirectOutput(new File("F:\\BigData\\workspace\\spark-demo\\spark-submit\\src\\main\\resources")).startApplication();

        handle.addListener(new SparkAppHandle.Listener() { //添加监听者，相比process可以监听spark应用状态
            @Override
            public void stateChanged(SparkAppHandle sparkAppHandle) {
                System.out.println("state发生改变");
                System.out.println("state=" + sparkAppHandle.getState());
            }

            @Override
            public void infoChanged(SparkAppHandle sparkAppHandle) {
                System.out.println("info发生改变");
                System.out.println("info=" + sparkAppHandle.getAppId());
            }
        });

        //这里一定要轮询，否则spark应用还没正常启动就退出了
        while (handle.getState() != SparkAppHandle.State.FINISHED) {
            System.out.println("state=" + handle.getState());
            System.out.println("info=" + handle.getAppId());
        }

        System.out.println("spark应用执行结束");
    }
}
