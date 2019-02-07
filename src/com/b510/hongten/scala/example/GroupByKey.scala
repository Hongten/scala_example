/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * groupByKey: 按照Key分组
 *
 * @author Hongten
 * @created 7 Feb, 2019
 */
object GroupByKey {

  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setMaster("local").setAppName("groupByKey")

    val sc = new SparkContext(conf)

    val arrayData = List(
      new Tuple2("hongten", "Basketball"),
      new Tuple2("hongten", "Football"),
      new Tuple2("Tom", "Surfing"),
      new Tuple2("Tom", "Boxing"),
      new Tuple2("John", "Boxing"),
      new Tuple2("John", "Cycling"),
      new Tuple2("John", "BodyBuilding"),
      new Tuple2("Cat", "Tennis"),
      new Tuple2("Cat", "badminton"),
      new Tuple2("Cat", "Bowling"),
      new Tuple2("Susan", "Golf"))

    val dataRDD = sc.parallelize(arrayData, 3)

    /**
     * output:
     * (Susan,CompactBuffer(Golf))
     * (John,CompactBuffer(Boxing, Cycling, BodyBuilding))
     * (Tom,CompactBuffer(Surfing, Boxing))
     * (Cat,CompactBuffer(Tennis, badminton, Bowling))
     * (hongten,CompactBuffer(Basketball, Football))
     */
    dataRDD.groupByKey.foreach(println(_))

    sc.stop
  }
}