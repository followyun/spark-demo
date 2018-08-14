package com.my.spark.util

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  */
object HDFSUtil {
  def deleteFileIfExisted(pathFile: String, config:Configuration): Unit = {
    val path = new Path(pathFile)
    try {
      val fs = path.getFileSystem(config)
      if (fs.exists(path)) {
        System.out.println(path + "存在，删除！")
        fs.delete(path, true)
      }
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }


}
