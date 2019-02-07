/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author Hongten
 * @created 1 Feb, 2019
 */
object FullOuterJoin {

  def main(args: Array[String]) {

    val conf = new SparkConf
    conf.setMaster("local").setAppName("full outer join")

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
	 * (hongten,(Some(12),Some(122)))
	 * (Foo,(None,Some(39)))
	 * (kee,(Some(10),Some(102)))
	 * (john,(Some(14),Some(142)))
     */
    rdd1.fullOuterJoin(rdd2).foreach(tuple => {
      println(tuple)
    })

    sc.stop

  }
}