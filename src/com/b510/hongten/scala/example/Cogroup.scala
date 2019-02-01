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
object Cogroup {
  def main(args: Array[String]) {

    val conf = new SparkConf
    conf.setMaster("local").setAppName("left join")

    val sc = new SparkContext(conf)

    val list1 = Array(
      ("hongten", "English:90"),
      ("tome", "Enlish:80"),
      ("john", "Enlish:67"),
      ("kee", "Enlish:84"))

    val list2 = Array(
      ("hongten", "Math:97"),
      ("tome", "Math:79"),
      ("john", "Math:84"),
      ("kee", "Math:30"),
      ("Foo", "Math:59"))

    val rdd1 = sc.parallelize(list1, 2)
    
    val rdd2 = sc.parallelize(list2, 4)

    /**
     * output:
	 * (hongten,(CompactBuffer(English:90),CompactBuffer(Math:97)))
	 * (Foo,(CompactBuffer(),CompactBuffer(Math:59)))
	 * (tome,(CompactBuffer(Enlish:80),CompactBuffer(Math:79)))
	 * (kee,(CompactBuffer(Enlish:84),CompactBuffer(Math:30)))
	 * (john,(CompactBuffer(Enlish:67),CompactBuffer(Math:84)))
     */
    rdd1.cogroup(rdd2).foreach(tuple => {
      println(tuple)
    })

    sc.stop

  }
}