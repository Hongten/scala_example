/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

/**
 * 
 * Noted: 运行之前先运行com.b510.hongten.scala.example.datagenerate.DataGenerate生成测试数据
 * 
 * https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html#rdd-persistence
 * https://github.com/apache/spark/blob/v2.2.0/core/src/main/scala/org/apache/spark/storage/StorageLevel.scala
 *
 * 持久化算子
 *
 * 可以手动指定持久化级别
 *
 *
 * private var _useDisk: Boolean,               --使用磁盘
 * private var _useMemory: Boolean,             --使用内存
 * private var _useOffHeap: Boolean,            --使用对外内存
 * private var _deserialized: Boolean,          --不序列化
 * private var _replication: Int = 1            --副本数，默认为1
 *
 * object StorageLevel {
 * val NONE = new StorageLevel(false, false, false, false)                        --不设置任何以上变量
 * val DISK_ONLY = new StorageLevel(true, false, false, false)                    --只存磁盘，避免使用  --和没有设置持久化级别是一样的
 * val DISK_ONLY_2 = new StorageLevel(true, false, false, false, 2)               --只存磁盘，副本为2 -- 也要避免(数据不是很重要)使用副本为2的，应为涉及到拷贝I/O
 * val MEMORY_ONLY = new StorageLevel(false, true, false, true)                   --常用
 * val MEMORY_ONLY_2 = new StorageLevel(false, true, false, true, 2)
 * val MEMORY_ONLY_SER = new StorageLevel(false, true, false, false)              --常用
 * val MEMORY_ONLY_SER_2 = new StorageLevel(false, true, false, false, 2)
 * val MEMORY_AND_DISK = new StorageLevel(true, true, false, true)                --常用，内存不够的时候，才会放到磁盘
 * val MEMORY_AND_DISK_2 = new StorageLevel(true, true, false, true, 2)
 * val MEMORY_AND_DISK_SER = new StorageLevel(true, true, false, false)           --常用
 * val MEMORY_AND_DISK_SER_2 = new StorageLevel(true, true, false, false, 2)
 * val OFF_HEAP = new StorageLevel(true, true, true, false, 1)
 *
 * ...
 * }
 *
 *
 * @author Hongten
 * @created 31 Jan, 2019
 */
object Presist {

  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setMaster("local").setAppName("presist")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("./resources/test_big_data")

    val start = System.currentTimeMillis();
    val result1 = lines.count;
    val end = System.currentTimeMillis();
    println("result1: " + result1 + ", time : " + (end - start) + " ms")
    //output: result1: 10000000, time : 4453 ms

    //把数据放入内存中，如果运行完成，数据会被回收
    //lines.cache
    lines.persist(StorageLevel.MEMORY_ONLY)

    val start2 = System.currentTimeMillis();
    val result2 = lines.count;
    val end2 = System.currentTimeMillis();
    println("result2: " + result2 + ", time : " + (end2 - start2) + " ms")
    //output: result2: 10000000, time : 5960 ms

    val start3 = System.currentTimeMillis();
    val result3 = lines.count;
    val end3 = System.currentTimeMillis();
    println("result3: " + result3 + ", time : " + (end3 - start3) + " ms")
    //output: result3: 10000000, time : 166 ms

    sc.stop
  }
}