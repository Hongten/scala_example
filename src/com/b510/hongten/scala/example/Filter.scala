/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/**
 * 过滤数据
 * @author Hongten
 * @created 31 Jan, 2019
 */
object Filter {

  def main(args: Array[String]) {
	  val conf = new SparkConf
	  conf.setMaster("local").setAppName("filter")
	  
	  val sc = new SparkContext(conf)
	  
	  val lines = sc.textFile("./resources/test_data")
	  
	  //对记录进行过滤
	  val filteredLines = lines.filter(line =>{line.contains("hello1")})
	  
	  filteredLines.flatMap(line =>{line.split(" ")}).map(new Tuple2(_, 1)).reduceByKey(_+_).foreach(println(_))
	  
	  sc.stop
  }
}