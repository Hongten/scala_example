/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 累加器 - accumulator相当于集群中统筹大变量
 * 
 * 1.累加器只能在Driver定义并初始化，不能再Executor端定义初始化
 * 2.累加器取值accumulator.value只能在Driver读取，不能在Executor端accumulator.value读取值
 *
 * @author Hongten
 * @created 23 Feb, 2019
 */
object Accumulator {

  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setMaster("local").setAppName("accumulator")
    val sc = new SparkContext(conf)
    //accumulator相当于集群中统筹大变量
    val accumulator = sc.accumulator(0)
    val lines = sc.textFile("./resources/test_data", 2)

    val rdd = lines.map(line => {
      accumulator += 1
      line
    })

    rdd.collect

    /**
     * output:
     * total line number : 11
     */
    println("total line number : " + accumulator.value)

    sc.stop

  }
}