/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * Noted: 运行之前先运行com.b510.hongten.scala.example.datagenerate.DataGenerate生成测试数据
 * 
 * 运行结果： 不同的机器，结果不同，但是我们发现，使用了cache方法，速度明显快了很多
 * 
 * Cache默认将数据存入内存中，是持久化算子，懒执行算子，需要Action算子触发
 *
 * @author Hongten
 * @created 31 Jan, 2019
 */
object Cache {

  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setMaster("local").setAppName("cache")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("./resources/test_big_data")

    val start = System.currentTimeMillis();
    val result1 = lines.count;
    val end = System.currentTimeMillis();
    println("result1: " + result1 + ", time : " + (end - start) + " ms")
    //output: result1: 10000000, time : 4433 ms
    
    //把数据放入内存中
    lines.cache
    //lines.persist(StorageLevel.MEMORY_ONLY)
    
    val start2 = System.currentTimeMillis();
    val result2 = lines.count;
    val end2 = System.currentTimeMillis();
    println("result2: " + result2 + ", time : " + (end2 - start2) + " ms")
    //output: result2: 10000000, time : 7095 ms

    val start3 = System.currentTimeMillis();
    val result3 = lines.count;
    val end3 = System.currentTimeMillis();
    println("result3: " + result3 + ", time : " + (end3 - start3) + " ms")
    //output: result3: 10000000, time : 162 ms

    sc.stop
  }
}