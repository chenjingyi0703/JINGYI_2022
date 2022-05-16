package utils

import java.text.SimpleDateFormat
import java.time.ZoneId
import java.util.{Calendar, Date}

/**
 * @author LJK
 * @date 2019/10/29 4:35 下午
 */
object DateUtils {

  val zoneId = ZoneId.of("Asia/Shanghai")


  def getToday: String = {
    val dayFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    dayFormat.format(new Date())
  }

  def getYesterday: String = {
    val dayFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -1)
    dayFormat.format(calendar.getTime)
  }

  def getBeforeYesterday: String = {
    val dayFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -2)
    dayFormat.format(calendar.getTime)
  }

  def getWeekOfMonday: String = {
    val dayFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance()
    calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    dayFormat.format(calendar.getTime)
  }

  def getWeekEnd: String = {
    val dayFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance()
    calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    calendar.add(Calendar.DATE, -1)
    dayFormat.format(calendar.getTime)
  }

  def getWeekStart: String = {
    val dayFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance()
    calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    calendar.add(Calendar.DATE, -7)
    dayFormat.format(calendar.getTime)
  }
}
