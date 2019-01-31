/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 *
 * 功能：
 * 1.将数据放入磁盘
 * 2.有切断RDD之间的依赖关系
 * 3.与persist区别在于，persist的数据在application运行完后会被回收，而checkpoint数据不会被回收
 * 4.某些特定场景需要使用checkpoint--SparkStreaming里面就用到了
 *
 * 场景：如果我们的job有很长的lineage，可以使用checkpoint。 优化：对某个RDD使用checkpoint之前，最好先cache一下这个RDD
 *
 * 与persist区别：
 * persist：Application运行完，数据会被回收
 * checkpoint：Application运行完，数据还存在磁盘
 *
 * @author Hongten
 * @created 31 Jan, 2019
 */
object CheckPoint {

  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setMaster("local").setAppName("check point")

    val sc = new SparkContext(conf)
    sc.setCheckpointDir("./checkpoint")

    val start = System.currentTimeMillis()
    val lines = sc.textFile("./resources/test_data")

    val wordAndTotalNum = lines.filter(!_.contains("hello1")).flatMap(_.split(" ")).map(new Tuple2(_, 1)).reduceByKey(_ + _)

    wordAndTotalNum.cache
    wordAndTotalNum.checkpoint

    val result = wordAndTotalNum.map(_.swap).sortByKey(false)

    result.foreach(println(_))
    
    val end = System.currentTimeMillis()
    println("time : " + (end - start) + " ms")

    sc.stop
  }
}