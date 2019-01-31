/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 获取第一行记录
 *
 * @author Hongten
 * @created 31 Jan, 2019
 */
object First {

  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setMaster("local").setAppName("first")

    val sc = new SparkContext(conf)

    val firstLine = sc.textFile("./resources/test_data").first

    println(firstLine)
    sc.stop
  }
}