/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * zip: 将两个RDD压缩成一个K,V格式的RDD，两个RDD钟每个分区的数据个数要一致
 *
 *
 * @author Hongten
 * @created 7 Feb, 2019
 */
object Zip {

  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setMaster("local").setAppName("zip")

    val sc = new SparkContext(conf)

    val arrayData1 = List(
      new Tuple2("Hongten", "Basketball"),
      new Tuple2("Susan", "Football"),
      new Tuple2("Tom", "Surfing"))

    val arrayData2 = List(
      new Tuple2("Hongten", 23),
      new Tuple2("Tom", 26),
      new Tuple2("Susan", 17) 
      // Can only zip RDDs with same number of elements in each partition
      // ,new Tuple2("Cat", 17) 
      )

    val dataRDD1 = sc.parallelize(arrayData1, 3)
    val dataRDD2 = sc.parallelize(arrayData2, 3)

    /**
     * output:
     * ((Hongten,Basketball),(Hongten,23))
     * ((Susan,Football),(Tom,26))
     * ((Tom,Surfing),(Susan,17))
     */
    dataRDD1.zip(dataRDD2).foreach(println(_))

    sc.stop
  }
}