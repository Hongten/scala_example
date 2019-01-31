/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * Action算子：获取单词总个数
 * 
 * 会将Worker端的结果回收到Driver端
 * 
 * @author Hongten
 * @created 31 Jan, 2019
 */
object Count {

  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setMaster("local").setAppName("count")

    val sc = new SparkContext(conf)
    val words = sc.textFile("./resources/test_data").flatMap(_.split(" "))
    //获取单词总个数
    val totalNum = words.count;

    println(totalNum)

    sc.stop

  }
}