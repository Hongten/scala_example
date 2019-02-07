/**
 * Big Data Example
 * Mail: hongtenzone@foxmail.com
 * Blog: http://www.cnblogs.com/hongten
 */
package com.b510.hongten.scala.example.datagenerate

import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Random
import java.util.concurrent.Executors

/**
 * 生成./resources/test_big_data_pv_uv数据文件
 *
 * @author Hongten
 * @created 7 Feb, 2019
 */
object DataGeneratePVUV {

  def main(args: Array[String]) {
    generateData
  }

  def generateData() {
    var exc = Executors.newCachedThreadPool();
    exc.execute(new GenerateDataUtils)
  }

}

class GenerateDataUtils extends Runnable {

  override def run() {
    var common = new Common
    var random = new Random
    var sb = new StringBuffer
    var num: Int = 0
    var writename = new File(common.PV_UV_FILE_PATH)
    if (!writename.exists()) {
      writename.createNewFile()
    }
    while (true) {
      var date = new Date();
      var simpleDateFormat = new SimpleDateFormat(common.DATE_FORMAT_YYYYDDMM);
      var d = simpleDateFormat.format(date);
      var timestamp = new Date().getTime();
      // ip地址生成
      var ip = random.nextInt(common.MAX_IP_NUMBER) + "." + random.nextInt(common.MAX_IP_NUMBER) + "." + random.nextInt(common.MAX_IP_NUMBER) + "." + random.nextInt(common.MAX_IP_NUMBER);
      // ip地址对应的address(这里是为了构造数据，并没有按照真实的ip地址，找到对应的address)
      var address = common.ADDRESS(random.nextInt(common.ADDRESS.length));

      var userid = Math.abs(random.nextLong());
      var action = common.USER_ACTION(random.nextInt(common.USER_ACTION.length));
      // 日志信息构造
      // example : 199.80.45.117 云南 2018-12-20 1545285957720 3086250439781555145 www.hongten.com Buy
      var data = ip + "\t" + address + "\t" + d + "\t" + timestamp + "\t" + userid + "\t" + common.WEB_SITE + "\t" + action;
      sb.append(data).append(common.LINE_BREAK)
      num += 1
      //println(sb)
      if (num > common.MAX_LINE_NUMBER) {
        var out = new BufferedWriter(new FileWriter(writename))
        out.write(sb.toString())
        out.flush()
        out.close()
        println("Generate Data Done.")
        return
      }
    }
  }

}

// 所有的常量定义
class Common {

  val PV_UV_FILE_PATH = "./resources/test_big_data_pv_uv"

  val CHAR_FORMAT: String = "UTF-8"
  val LINE_BREAK: String = "\n"
  val MAX_LINE_NUMBER: Int = 900000

  val DATE_FORMAT_YYYYDDMM: String = "yyyy-MM-dd"

  // this is a test web site
  val WEB_SITE: String = "www.hongten.com"

  // 用户的action
  val USER_ACTION = Array("Register", "Login", "View", "Click", "Double_Click", "Buy", "Shopping_Car", "Add", "Edit", "Delete", "Comment", "Logout")

  val MAX_IP_NUMBER: Int = 224
  // ip所对应的地址
  val ADDRESS = Array("北京", "天津", "上海", "广东", "重庆", "河北", "山东", "河南", "云南", "山西", "甘肃", "安徽", "福建", "黑龙江", "海南", "四川", "贵州", "宁夏", "新疆", "湖北", "湖南", "山西", "辽宁", "吉林", "江苏", "浙江", "青海", "江西", "西藏", "内蒙", "广西", "香港", "澳门", "台湾")
}