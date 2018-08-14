package com.my.submit;

/**
 *
 */
public class HelloWorld {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("HelloWorld程序启动");
        System.out.println("args[0]="+args[0]);
        System.out.println("Hello World...");
        Thread.sleep(2000L);
        System.out.println("休息两秒结束");
        String testEnv = System.getenv("testEnv");
        System.out.println("testEnv="+testEnv);

    }
}
