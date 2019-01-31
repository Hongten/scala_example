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
 * 对单词进行统计，然后输出<br>
 * 
 * 数据不移动，计算移动。
 * 
 * RDD(Resilient Distributed Dataset) - 弹性分布式数据集
 * RDD五大特性：
 * 1.	RDD是由一系列的partition组成的。
 * 2.	函数是作用在每一个partition（split）上的。
 * 3.	RDD之间有一系列的依赖关系。
 * 4.	分区器是作用在K,V格式的RDD上。
 * 5.	RDD提供一系列最佳的计算位置。利于数据处理本地化。
 * 
 * 
 * A:什么是K，V格式的RDD？
 * RDD中的每一个元素是一个个的二元组，那么这个RDD就是一个K，V格式的RDD
 * 
 * B:sc1.textFile("./resources/wordcount_test_data.txt"),Spark没有直接读取HDFS文件的方法，textFile()底层利用个但是MR读取HDFS文件的方法，
 * 首先会split，每个split默认大小为128M，就是一个block大小，每个split对应一个partition
 *
 * C:一个partition只能由一个task处理
 * 
 * D:在集群中，每个task是并行执行任务的。
 * 
 * E:哪里现了RDD的弹性?
 * 1.RDD partition的个数是可多可少（a.在分区中，把一个partition分为2个，增加了partition的个数，只要CUP个数足够，可以提高效率； b.启动1个task的时间如果是10s，1个task处理一个任务花费200s，共210s。100个task处理该任务花费2s完成，那么加上task启动时间这样就会花费1002s完成任务。）
 * 2.RDD之间有依赖关系
 * 
 * F:哪里体现RDD的分布式
 * RDD中的partition分布在多个节点上面。
 * 
 * G:RDD中是不存数据的，partition也不数据的。
 * 
 * 
 * 
 * Spark中任务调度
 * Driver，JVM进程，当启动一个Spark应用程序，就会启动一个Driver，Driver会发生task到Worker节点。Driver功能：发送task到worker端，回收worker端计算的结果。
 * Worker，Standalone模式下的JVM进程，Worker接收Driver端发送的task，读取数据，进行计算，并把结果返回到Driver端
 * Master，Standalone模式下的JVM进程，Master管理Worker
 * 
 * 
 * Spark中的算子：
 * https://blog.csdn.net/dream0352/article/details/62229977
 * 
 * Transformation变换/转换算子：
 * 这种变换并不会触发提交作业，完成作业中间过程处理。如果要执行这类算子，需要Action行动算子触发。
 * Transformation算子是懒执行，即延迟计算的。也就是说从一个RDD转换到另一个RDD的转换操作不是马上执行，而是需要等到
 * 有Action算子的时候才会真正触发执行。
 * Action行动算子：
 * 这类算子会触发SparkContext提交Job作业。
 * 
 * 
 * 
 * 下面的WordCount是一个Application。在这个Application中由两个job(两个foreach，即两个Action算子)。
 * 
 * 
 * 
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

    //SparkContext是通往集群的唯一通道
    val sc1 = new SparkContext(conf);
    val lines: RDD[String] = sc1.textFile("./resources/test_data")
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

    //排序1： 对结果降序排序并打印 -> 对totalNum进行降序排序
    result.sortBy(tuple => { tuple._2 }, false).foreach(tuple => {
      println(tuple);
    })

    //排序2： <word, totalNum> -> <totalNum, word> -> sortByKey
    result.map(tuple => { tuple.swap }).sortByKey(false).foreach(tuple => {
      println(tuple.swap);
    })

    sc1.stop();

  }
}