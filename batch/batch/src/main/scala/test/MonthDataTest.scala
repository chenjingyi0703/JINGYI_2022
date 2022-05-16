package test

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.SparkUtils

object MonthDataTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("ActionOperation")
      .config("spark.yarn.maxAppAttempts", "1")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //获取上个月的数据
    val dateArr: Array[String]  = SparkUtils.datesOfMonth(1)
    val yesterdayDate = dateArr(0)
    val scan0 = new Scan()
    println(dateArr(22))
    scan0.setRowPrefixFilter(DigestUtils.sha1Hex(dateArr(22)).getBytes())
    val hbaseConf = SparkUtils.hbaseConfSet(scan0, "dy:product-daily")

    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.
      newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
        classOf[ImmutableBytesWritable],classOf[Result])

    val c = hbaseRDD.count()
    println(c)
    /*
    for(i <- 1 to dateArr.length - 1){
      val scan = new Scan()
      scan.setRowPrefixFilter(DigestUtils.sha1Hex(dateArr(i)).getBytes())
      val hbaseConf = SparkUtils.hbaseConfSet(scan, "dy:product-daily")

      val tmpRDD: RDD[(ImmutableBytesWritable, Result)] = sc.
        newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
          classOf[ImmutableBytesWritable],classOf[Result])

      hbaseRDD.union(tmpRDD)
    }*/

  }
}
