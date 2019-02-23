/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.AccumulatorParam

/**
 * 自定义累加器
 *
 * @author Hongten
 * @created 23 Feb, 2019
 */
object AccumulatorCustom {

  def main(args: Array[String]) {

    /**
     * 自定义累加器
     */
    object MyAccumulator extends AccumulatorParam[String] {

      //自定义累加器初始化操作
      def zero(initialValue: String): String = {
        "A=0,B=0,C=0,D=0"
      }

      //累加器操作
      def addInPlace(v1: String, v2: String): String = {
        if (v1 == "") {
          v2
        } else {
          val oldValue = getFieldFromConcatString(v1, ",", v2)
          //在原来值基础上加1
          val newValue = Integer.valueOf(oldValue) + 1
          setFieldInConcatString(v1, ",", v2, String.valueOf(newValue))
        }
      }

      //根据对应的Key，返回其value值
      def getFieldFromConcatString(str: String, delimiter: String, field: String): String = {
        val fields = str.split(delimiter)
        for (concatField <- fields) {
          if (concatField.split("=").length == 2) {
            val fieldName = concatField.split("=")(0)
            val fieldValue = concatField.split("=")(1)
            if (fieldName.equals(field)) {
              fieldValue
            }
          }
        }
        "0"
      }

      //找到对应的key，并且把最新的value更新
      def setFieldInConcatString(str: String,
        delimiter: String, field: String, newFieldValue: String): String = {
        val fields = str.split(delimiter)
        var i = 0
        for (i <- 0 until fields.length) {
          val name = fields(i).split("=")(0)
          if (name.equals(field)) {
            val updatedField = name + "=" + newFieldValue
            fields(i) = updatedField
          }
        }

        val buffer = new StringBuffer("")
        for (f <- fields) {
          buffer.append(f).append(",")
        }

        val result = buffer.toString()
        result.substring(0, result.length() - 1)
      }
    }

    val conf = new SparkConf
    conf.setMaster("local").setAppName("Accumulator Custom")

    val sc = new SparkContext(conf)

    //在Driver端定义自己的累加器
    val myAccumulator = sc.accumulator("")(MyAccumulator)

    val arr = Array("A", "C")
    val rdd = sc.parallelize(arr, 1)

    //调用foreach Action算子触发
    rdd.foreach(myAccumulator.add(_))

    /**
     * output:
     * result : A=1,B=0,C=1,D=0
     */
    println("result : " + myAccumulator.value)

    sc.stop

  }

}