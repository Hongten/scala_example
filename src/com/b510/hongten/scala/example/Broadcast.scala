/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 广播变量
 *
 * 1.不能讲RDD广播出去，可以将RDD的结果广播出去
 * 2.广播变量只能在Driver定义，在Executor端使用，不能在Executor端改变
 * 3.如果不使用广播变量，在一个Executor中有多少个task，就有多少个变量副本，
 *   如果使用广播变量，在每个Executor中只有一份Driver端的广播变量副本
 *
 * @author Hongten
 * @created 24 Feb, 2019
 */
object Broadcast {

  def main(args: Array[String]) {

    val conf = new SparkConf
    conf.setMaster("local").setAppName("broadcast")

    val sc = new SparkContext(conf)

    val blackList = List[String]("hello1", "hongten1")

    val broadcastList = sc.broadcast(blackList)

    val lines = sc.textFile("./resources/test_data")
    val words = lines.flatMap(line => {
      line.split(" ")
    })

    val filteredRDD = words.filter(word => {
      //获取广播变量
      val blackList = broadcastList.value
      //过滤掉所有关键字
      !blackList.contains(word)
    })

    /**
     * output:
     * (hello,9)
     * (hongten,10)
     * (Hongten,11)
     */
    filteredRDD.map(word => {
      new Tuple2(word, 1)
    }).reduceByKey(_ + _).foreach(println)

    sc.stop

  }

}