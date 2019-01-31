/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example.datagenerate

import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter

/**
 * @author Hongten
 * @created 31 Jan, 2019
 */
object DataGenerate {

  def main(args: Array[String]) {
    val sBuilder = new StringBuilder();
    val i = 0;
    for (a <- 1 to 10000000) {
      sBuilder.append("hello").append(" ").append("hongten").append(" ").append(i).append("\n")
    }

    val writename = new File("./resources/test_big_data")
    if (!writename.exists()) {
      writename.createNewFile()
    }
    val out = new BufferedWriter(new FileWriter(writename))
    out.write(sBuilder.toString())
    out.flush()
    out.close()
    println("done.")
  }
}