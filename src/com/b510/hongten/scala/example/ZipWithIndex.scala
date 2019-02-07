/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * zipWithIndex: 给RDD中的每个元素与当前元素的下标压缩成一个K，V格式的RDD
 * 
 * @author Hongten
 * @created 7 Feb, 2019
 */
object ZipWithIndex {

  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setMaster("local").setAppName("zipWithIndex")

    val sc = new SparkContext(conf)

    val arrayData1 = List(
      new Tuple2("Hongten", "Basketball"),
      new Tuple2("Susan", "Football"),
      new Tuple2("Tom", "Surfing"))

    val dataRDD1 = sc.parallelize(arrayData1, 2)

    /**
     * output:
     * ((Hongten,Basketball),0)
     * ((Susan,Football),1)
     * ((Tom,Surfing),2)
     */
    dataRDD1.zipWithIndex.foreach(println(_))

    sc.stop
  }
}