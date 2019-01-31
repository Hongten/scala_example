/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * 对单词进行统计，然后输出
 *
 * @author Hongten
 * @created 31 Jan, 2019
 */
object WordCount {

  def main(args: Array[String]) {

    //method 1
    //    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("wordCount"))
    //    sc.textFile("./resources/wordcount_test_data.txt").flatMap(_.split(" ")).map(Tuple2(_, 1)).reduceByKey(_ + _).foreach(println);
    //    sc.stop()

    // method 2
    val conf = new SparkConf();
    conf.setMaster("local").setAppName("wordCount");

    val sc1 = new SparkContext(conf);
    val lines: RDD[String] = sc1.textFile("./resources/wordcount_test_data.txt")
    val words = lines.flatMap(line => {
      println("flatMap -- 读取行 ： " + line);
      line.split(" ")
    })

    //<word, 1>
    val mapOutput: RDD[(String, Int)] = words.map(word => {
      println("map -- 处理单词 ： " + word);
      new Tuple2(word, 1)
    })

    //<word, totalNum>
    val result = mapOutput.reduceByKey((v1: Int, v2: Int) => {
      println("map -- 累加 ： v1= " + v1 + " ,  v2=" + v2);
      v1 + v2
    });

    //对结果降序排序并打印 -> 对totalNum进行降序排序
    result.sortBy(tuple => { tuple._2 }, false).foreach(tuple => {
      println(tuple);
    })

    //<word, totalNum> -> <totalNum, word> -> sortByKey
    result.map(tuple => { tuple.swap }).sortByKey(false).foreach(tuple => {
      println(tuple.swap);
    })

    sc1.stop();

  }
}