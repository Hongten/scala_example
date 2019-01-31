/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 数据随机抽样
 * @author Hongten
 * @created 31 Jan, 2019
 */
object Sample {

  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setMaster("local").setAppName("sample")

    val sc = new SparkContext(conf)
    //可以指定分区数：3
    val lines = sc.textFile("./resources/test_data", 3)
    //true - 表示不会放回抽样
    //0.2 - 抽样比列
    //10 - 抽样种子 - 针对同一数据集，抽样结果是一样的。
    //如果想要实现随机抽样，那么把seed去掉即可，即 val result = lines.sample(true, 0.2)
    val result = lines.sample(true, 0.2, 10);

    result.foreach(println(_))

    sc.stop
  }
}