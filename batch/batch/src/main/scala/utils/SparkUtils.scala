package utils

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.commons.cli.{BasicParser, Options}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors

object SparkUtils {
  /*
    星座     日期(公历)    英文名

    魔羯座 (12/22 - 1/19) Capricorn
    水瓶座 (1/20 - 2/18) Aquarius
    双鱼座 (2/19 - 3/20) Pisces
    牡羊座 (3/21 - 4/20) Aries
    金牛座 (4/21 - 5/20) Taurus
    双子座 (5/21 - 6/21) Gemini
    巨蟹座 (6/22 - 7/22) Cancer
    狮子座 (7/23 - 8/22) Leo
    处女座 (8/23 - 9/22) Virgo
    天秤座 (9/23 - 10/22) Libra
    天蝎座 (10/23 - 11/21) Scorpio
    射手座 (11/22 - 12/21) Sagittarius
*/
  val constellationArr = List("水瓶座", "双鱼座", "牡羊座", "金牛座", "双子座", "巨蟹座", "狮子座", "处女座", "天秤座",
    "天蝎座", "射手座", "魔羯座")
  val constellationEdgeDay = List(20, 19, 21, 21, 21, 22, 23, 23, 23, 23, 22, 22)

  def date2Constellation(date: String): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    println(date)
    val dateFormat = sdf.parse(date)
    val calendar = Calendar.getInstance()
    calendar.setTime(dateFormat)
    var month = calendar.get(Calendar.MONTH)
    val day = calendar.get(Calendar.DAY_OF_MONTH)
    if (day < constellationEdgeDay(month)) {
      month = month - 1;
    }
    if (month >= 0) {
      return constellationArr(month);
    }
    //default to return 魔羯
    return constellationArr(11);
  }

  //传参
  def functionArgs(args: Array[String]): String = {
    val parser = new BasicParser()
    val options = new Options()
    options.addOption("t", true, "table")
    //    options.addOption("m", true, "time")

    //    options.addOption("u", false, "update models")

    val cmd = parser.parse(options, args)
    val arg1: String = cmd.getOptionValue("t")
    //    val arg2: String = cmd.getOptionValue("t")

    arg1
  }


  def numToVectors(num: Long) = {
    val vector: linalg.Vector = Vectors.dense(num)
    vector
  }

  //判断年月日
  def judgeDurationTime(timetype: Int): (String, String) = {
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance
    calendar.add(Calendar.HOUR_OF_DAY, -24)
    val yesterdayDate = dateFormat.format(calendar.getTime)
    var monthDate = ""
    //月/周/天
    if (timetype == 0) {
      calendar.add(Calendar.MONTH, -1)
      monthDate = dateFormat.format(calendar.getTime)
    } else if (timetype == 1) {
      calendar.add(Calendar.HOUR_OF_DAY, -144)
      monthDate = dateFormat.format(calendar.getTime)
    } else if (timetype == 2) {
      calendar.add(Calendar.HOUR_OF_DAY, -24)
      monthDate = dateFormat.format(calendar.getTime)
    }

    (yesterdayDate, monthDate)
  }

  /**
   * 一个月的全部日期yyyyMMdd
   * author: hbf
   * 20191023
   */
  def datesOfMonth(): Array[String] = {
    //日期格式
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance()
    val m = calendar.get(Calendar.MONTH)
    calendar.add(Calendar.MONTH, -1);

    //月份的天数
    var numOfMonth = 0
    while(calendar.get(Calendar.MONTH) < m){
      numOfMonth = calendar.get(Calendar.DAY_OF_MONTH)
      //println(dateFormat.format(calendar.getTime()))
      //println(calendar.get(Calendar.MONTH) < m)
      calendar.add(Calendar.DAY_OF_MONTH, 1)
    }
    calendar.add(Calendar.DAY_OF_MONTH, -1)

    var dateStrArr: Array[String] = new Array[String](numOfMonth)
    var idx = 0

    //将日期降至1
    while(calendar.get(Calendar.DAY_OF_MONTH) != 1){
      calendar.add(Calendar.DAY_OF_MONTH, -1)
      //val dateNum = calendar.get(Calendar.DAY_OF_MONTH)
    }
    //获取一个月最大日期
    while(calendar.get(Calendar.MONTH) < m){
      val date = calendar.get(Calendar.DAY_OF_MONTH)
      //println(dateFormat.format(calendar.getTime()))
      dateStrArr(idx) = dateFormat.format(calendar.getTime())
      //println(idx)
      idx += 1
      calendar.add(Calendar.DAY_OF_MONTH, 1)
    }
    //返回结果
    dateStrArr
  }

  /**
   * 一周的全部日期yyyyMMdd
   * author: hbf
   * 20191023
   */
  def datesOfWeek(): Array[String] = {
    var dateStrArr: Array[String] = new Array[String](7)

    //日期格式
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance()
    val m = calendar.get(Calendar.WEEK_OF_MONTH)
    calendar.add(Calendar.WEEK_OF_MONTH, -1);

    //将日期降至周一
    while(calendar.get(Calendar.DAY_OF_WEEK) != 1){
      calendar.add(Calendar.DAY_OF_MONTH, -1)
      //val dateNum = calendar.get(Calendar.DAY_OF_MONTH)
    }

    for(idx <- 0 to 6){
      //val date = calendar.get(Calendar.DAY_OF_MONTH)
      //println(dateFormat.format(calendar.getTime()))
      calendar.add(Calendar.DAY_OF_MONTH, 1)
      dateStrArr(idx) = dateFormat.format(calendar.getTime())
      //println(idx)
    }

    dateStrArr
  }

  /**
   * 一个月的全部日期yyyyMMdd
   * author: hbf
   * 20191023
   */
  def datesOfMonth(diff: Int): Array[String] = {
    //日期格式
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.MONTH, diff)
    val m = calendar.get(Calendar.MONTH)
    calendar.add(Calendar.MONTH, -1);

    //月份的天数
    var numOfMonth = 0
    while(calendar.get(Calendar.MONTH) < m){
      numOfMonth = calendar.get(Calendar.DAY_OF_MONTH)
      //println(dateFormat.format(calendar.getTime()))
      //println(calendar.get(Calendar.MONTH) < m)
      calendar.add(Calendar.DAY_OF_MONTH, 1)
    }
    calendar.add(Calendar.DAY_OF_MONTH, -1)

    var dateStrArr: Array[String] = new Array[String](numOfMonth)
    var idx = 0

    //将日期降至1
    while(calendar.get(Calendar.DAY_OF_MONTH) != 1){
      calendar.add(Calendar.DAY_OF_MONTH, -1)
      //val dateNum = calendar.get(Calendar.DAY_OF_MONTH)
    }
    //获取一个月最大日期
    while(calendar.get(Calendar.MONTH) < m){
      val date = calendar.get(Calendar.DAY_OF_MONTH)
      //println(dateFormat.format(calendar.getTime()))
      dateStrArr(idx) = dateFormat.format(calendar.getTime())
      //println(idx)
      idx += 1
      calendar.add(Calendar.DAY_OF_MONTH, 1)
    }
    //返回结果
    dateStrArr
  }

  def hbaseConfSet(scan: Scan, table: String)  = {
    val scanString = TableMapReduceUtil.convertScanToString(scan)
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.regionsizecalculator.enable", "false") //aliyun hbase 不加有问题
    hbaseConf.set(TableInputFormat.INPUT_TABLE, table)
    hbaseConf.set(TableInputFormat.SCAN, scanString)
    hbaseConf
  }


  def main(args: Array[String]): Unit = {
    val test = date2Constellation("2019-05-15")
    println(test)

    println(judgeDurationTime(1))
  }
}
