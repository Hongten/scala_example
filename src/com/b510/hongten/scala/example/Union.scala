/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * union：两个RDD类型要一致
 *
 * @author Hongten
 * @created 1 Feb, 2019
 */
object Union {

  def main(args: Array[String]) {

    val conf = new SparkConf
    conf.setMaster("local").setAppName("left join")

    val sc = new SparkContext(conf)

    val list1 = Array(
      ("hongten", "12"),
      ("tome", "23"),
      ("john", "14"),
      ("kee", "10"))

    val list2 = Array(
      ("hongten", "122"),
      ("tome", "232"),
      ("john", "142"),
      ("kee", "102"),
      ("Foo", "39"))

    val rdd1 = sc.parallelize(list1, 2)

    val rdd2 = sc.parallelize(list2, 4)

    /**
     * output:
     * (hongten,12)
     * (tome,23)
     * (john,14)
     * (kee,10)
     * (hongten,122)
     * (tome,232)
     * (john,142)
     * (kee,102)
     * (Foo,39)
     */
    rdd1.union(rdd2).foreach(tuple => {
      println(tuple)
    })
    
    /**
     * 是两个RDD分区之和
     * output:
     * 	partition size : 6
     */
    println("partition size : " + rdd1.union(rdd2).partitions.size)

    sc.stop

  }
}