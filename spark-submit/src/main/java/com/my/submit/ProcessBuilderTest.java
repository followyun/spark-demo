package com.my.submit;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * java启动jvm测试
 */
public class ProcessBuilderTest {
    public static void main(String[] args) throws IOException, InterruptedException {
        List<String> cmdList = buildCmdList();
        ProcessBuilder process = new ProcessBuilder();
        //java
        //-Dtest=processBuilderTest -Xms20m
        //com.my.submit.HelloWorld t1
        process.command(cmdList); //设置jvm启动参数
        process.directory(new File("F:\\BigData\\workspace\\spark-demo\\spark-submit\\src\\main\\java\\com\\my\\submit"));//设置工作目录
        //process.directory(new File("F:\\BigData\\workspace\\spark-demo\\spark-submit\\target\\classes\\com\\my\\submit"));
        //设置子process的错误输出地方，这里设置成和当前进程一样
        process.redirectError(ProcessBuilder.Redirect.INHERIT);
        //设置子process输出地方，这里设置成和当前进程输出一致
        process.redirectOutput(ProcessBuilder.Redirect.INHERIT);

        //设置子进程的启动环境变量，子进程可以通过System.getenv("testEnv")获取到设置的值
        process.environment().put("testEnv", "testEnvValue");

        //启动子进程
        Process p = process.start();

        p.waitFor();//等待子进程结束
    }

    /**
     *
     * 构建参数列表
     * @return
     */
    private static List<String> buildCmdList(){
        List<String> cmdList = new ArrayList<>();
        //String javaHome = System.getProperty("java.home");
        //if(javaHome == null){
        String  javaHome = "F:\\Java\\JavaEnvironment\\jdk1.8.0_151\\jre\\bin";
       // }else {
         //   System.out.println("find javahome="+javaHome);
       // }
        //添加java命令
        String javaCmd = javaHome + "\\"+"java";
        cmdList.add(javaCmd);

        //添加jar和zip文件的类搜索路径
        cmdList.add("-cp");
        cmdList.add("F:\\BigData\\workspace\\spark-demo\\spark-submit\\target\\classes");

        cmdList.add("-Dtest=processBuilderTest");
        cmdList.add("-Xms20m");
        cmdList.add("com.my.submit.HelloWorld");
        cmdList.add("t1");
        return cmdList;
    }
}
