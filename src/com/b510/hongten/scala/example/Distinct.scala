/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 去重复数据
 *
 * @see com.b510.hongten.scala.example.removeduplicates.RemoveDuplicates
 * 
 * @author Hongten
 * @created 1 Feb, 2019
 */
object Distinct {

  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setMaster("local").setAppName("distinct")

    val sc = new SparkContext(conf)

    val data = sc.parallelize(List("a", "a", "c", "c", "c", "1", "2", "a"));

    /**
     * output:
     * a
     * 2
     * 1
     * c
     */
    data.distinct.foreach(println(_))

    sc.stop

  }
}