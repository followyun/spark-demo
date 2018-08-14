package com.my.spark.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 */
public class JavaHDFSUtil {
    public static void deleteFileIfExisted(String pathFile){
        Path path = new Path(pathFile);
        try {
            FileSystem fs = path.getFileSystem(new Configuration());
            if (fs.exists(path)) {
                System.out.println(path+"存在，删除！");
                fs.delete(path, true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
