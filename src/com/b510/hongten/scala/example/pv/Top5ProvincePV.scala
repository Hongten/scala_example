/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example.pv

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.b510.hongten.scala.example.datagenerate.Common
import com.b510.hongten.scala.example.datagenerate.DataGeneratePVUV

/**
 * 统计前面5个PV(Page View)最大的省份
 *
 * @see com.b510.hongten.scala.example.datagenerate.DataGeneratePVUV
 *
 * @author Hongten
 * @created 7 Feb, 2019
 */
object Top5ProvincePV {

  def main(args: Array[String]) {

    //生产数据
    generateData()

    var common = new Common
    val conf = new SparkConf
    conf.setMaster("local").setAppName("Top 5 Province PV")

    val sc = new SparkContext(conf)

    var lines = sc.textFile(common.PV_UV_FILE_PATH)

    var filteredRDD = lines.filter(line => {
      !(line.contains("Logout") || line.contains("Login"))
    })

    //<provinceName, 1>
    val provinceNameAndValueRDD = filteredRDD.map(line => {
      new Tuple2(line.split("\t")(1), 1)
    })

    var totalResult = provinceNameAndValueRDD
      //相同的Key的值相加
      .reduceByKey(_ + _)
      //交换K,V位置
      .map(tuple => {
        tuple.swap
      })
      //降序排序
      .sortByKey(false)
      //交换K,V位置
      .map(tuple => {
        tuple.swap
      })

    //取前面5个
    var result = totalResult.take(5)

    /**
     * output:
     * Top 5 Province PV : 山西, Total Number : 43915
     * Top 5 Province PV : 澳门, Total Number : 22405
     * Top 5 Province PV : 江西, Total Number : 22377
     * Top 5 Province PV : 福建, Total Number : 22246
     * Top 5 Province PV : 四川, Total Number : 22225
     */
    if (result != null && result.size > 0) {
      for ((k, v) <- result) {
        println("Top 5 Province PV : " + k + ", Total Number : " + v)
      }
    }

    sc.stop
  }

  def generateData() {
    var dataGeneratePVUV = DataGeneratePVUV
    dataGeneratePVUV.generateData
  }
}