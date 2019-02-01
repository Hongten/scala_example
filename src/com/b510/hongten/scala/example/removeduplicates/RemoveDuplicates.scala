/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example.removeduplicates

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Arrays

/**
 * 去重
 *
 * @see com.b510.hongten.scala.example.Distinct
 *
 * @author Hongten
 * @created 1 Feb, 2019
 */
object RemoveDuplicates {

  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setMaster("local").setAppName("remove duplicates")

    val sc = new SparkContext(conf)

    val data = sc.parallelize(List("a", "a", "c", "c", "c", "1", "2", "a"));

    /**
     * output:
     * a
     * 2
     * 1
     * c
     */
    data.map(new Tuple2(_, 1)).reduceByKey(_ + _).foreach(tuple => { println(tuple._1) })

    sc.stop

  }
}