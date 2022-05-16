package test

import java.text.SimpleDateFormat
import java.util.Calendar

object testCode {
  def main(args: Array[String]): Unit = {
    val arr = datesOfMonth()
    for(s <- arr){
      println(s)
    }
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
 }
