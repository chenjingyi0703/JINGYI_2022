package spark.author

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 博主日榜单
 */

object FansListAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ActionOperation")
      .config("spark.yarn.maxAppAttempts", "1")
      //      .master("local[*]")
      .getOrCreate()


    val sc = spark.sparkContext
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance
//    val testDate = dateFormat.format(calendar.getTime)
    calendar.set(Calendar.HOUR_OF_DAY, -24)
    val yesterdayDate = dateFormat.format(calendar.getTime)
    calendar.set(Calendar.HOUR_OF_DAY, -24)
    val yesterdayBeforeDate = dateFormat.format(calendar.getTime)
    println(yesterdayDate + "," + yesterdayBeforeDate)

    /**
     * 扫描昨天的数据
     */
    val scan = new Scan()
    scan.setRowPrefixFilter(DigestUtils.sha1Hex(yesterdayDate).getBytes())
    val scanString = TableMapReduceUtil.convertScanToString(scan)
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.regionsizecalculator.enable", "false") //aliyun hbase 不加有问题
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "dy:author-daily")
    hbaseConf.set(TableInputFormat.SCAN, scanString)

    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.
      newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
        classOf[ImmutableBytesWritable],
        classOf[Result])

    val odsData: DataFrame = hbaseRDD.filter(line => {
      if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("fc")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("fic")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("frc")) != null) {
        true
      } else {
        false
      }
    }).mapPartitions(part => {
      val tuples: Iterator[(String, String, String, Long, Long, Long, String, String, String, Int)] = part.map(r => {
        var vLabel = 0

        if (r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("vt")) != null) {
          vLabel = Bytes.toInt(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("vt")))
        }

        (
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ai"))),
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("nn"))),
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("a"))),
          Bytes.toLong(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("fc"))),
          Bytes.toLong(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("fic"))),
          Bytes.toLong(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("frc"))),
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("p"))),
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("c"))),
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("l"))),
          vLabel //蓝v 黄v
        )
      })
      tuples
    }).toDF("ai", "nn", "a", "fc", "fic", "frc", "p", "c", "l", "vLabel")
      .filter($"ai".notEqual("") && $"l".notEqual(""))
    odsData.cache().show(100, false)
    odsData.createOrReplaceTempView("tmp")

    /**
     * 扫描前天的数据
     */
    val scanBeforeYesterday = new Scan()
    scanBeforeYesterday.setRowPrefixFilter(DigestUtils.sha1Hex(yesterdayBeforeDate).getBytes())
    val scanBeforeYesterdayString = TableMapReduceUtil.convertScanToString(scanBeforeYesterday)
    val hbaseConfBeforeYesterday = HBaseConfiguration.create()
    hbaseConfBeforeYesterday.set("hbase.regionsizecalculator.enable", "false") //aliyun hbase 不加有问题
    hbaseConfBeforeYesterday.set(TableInputFormat.INPUT_TABLE, "dy:author-daily")
    hbaseConfBeforeYesterday.set(TableInputFormat.SCAN, scanBeforeYesterdayString)

    val hbaseBeforeYesterdayRDD: RDD[(ImmutableBytesWritable, Result)] = sc.
      newAPIHadoopRDD(hbaseConfBeforeYesterday, classOf[TableInputFormat],
        classOf[ImmutableBytesWritable],
        classOf[Result])

    val odsBeforeYesterdayData: DataFrame = hbaseBeforeYesterdayRDD.filter(line => {
      if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("fc")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("fic")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("frc")) != null) {
        true
      } else {
        false
      }
    }).mapPartitions(part => {
      val tuples: Iterator[(String, String, String, Long, Long, Long, String, String, String, Int)] = part.map(r => {
        var vLabel = 0
        if (r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("vt")) != null) {
          vLabel = Bytes.toInt(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("vt")))
        }

        (
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ai"))),
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("nn"))),
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("a"))),
          Bytes.toLong(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("fc"))),
          Bytes.toLong(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("fic"))),
          Bytes.toLong(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("frc"))),
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("p"))),
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("c"))),
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("l"))),
          vLabel //蓝v 黄v
        )
      })
      tuples
    }).toDF("ai", "nn", "a", "fc", "fic", "frc", "p", "c", "l", "vLabel")
      .filter($"ai".notEqual("") && $"l".notEqual(""))
    odsBeforeYesterdayData.cache().show(false)

    //两天的数据
    val odsTwoTimeData = odsData.join(odsBeforeYesterdayData, odsData("ai") === odsBeforeYesterdayData("ai"))
      .select(odsData("*"), odsBeforeYesterdayData("frc").alias("ybfrc"))

    odsTwoTimeData.show(false)
    //不限涨粉
    val odsMapData: DataFrame = odsTwoTimeData.withColumn("cha", $"frc" - $"ybfrc").filter($"cha" > 0)
    odsMapData.show(false)

    val increamFansListOrder = odsMapData
      .select((row_number() over (Window.orderBy($"cha".desc))).alias("rank"), $"*")
      .where("rank < 1501")

    increamFansListOrder.show(false)

    increamFansListOrder
      .rdd.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf("dy:author-rank"))
      val table = connection.getTable(TableName.valueOf("dy:author-snapshot"))

      part.foreach(arrInfo => {
        //        val arrInfo = part.next.toString().split(",")
        val rank = arrInfo(0).toString
        val Label = "B"
        val aid = arrInfo(1).toString
        val nn = arrInfo(2).toString
        val tx = arrInfo(3).toString
        val fc = arrInfo(4).toString.toLong
        val frc = arrInfo(6).toString.toLong
        val cha = arrInfo(12).toString.toLong
        val l = "不限"

        val get = new Get(Bytes.toBytes((DigestUtils.sha1Hex(aid) + yesterdayDate)))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("cmt"))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("srt"))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("dgt"))

        val result = table.get(get)
        val cmt: Array[Byte] = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("cmt"))
        val srt = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("srt"))
        val dgt = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("dgt"))
        //        println(Bytes.toLong(cmt) + "," + srt)

        val put = new Put((Label + yesterdayDate + l + rank).getBytes)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(aid))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("fc"), Bytes.toBytes(fc))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("frc"), Bytes.toBytes(frc))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("nn"), Bytes.toBytes(nn))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a"), Bytes.toBytes(tx))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("cmt"), cmt)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("srt"), srt)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("dgt"), dgt)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("fzl"), Bytes.toBytes(cha))

        mutator.mutate(put)
      })
      mutator.close()
    })


    //行业涨粉
    val increamFansOrder = odsMapData.filter($"l".notEqual(""))
      .select(
        (row_number() over (Window.partitionBy($"l").orderBy($"cha".desc))).alias("rank"),
        $"*"
      ).where("rank < 1501")

    increamFansOrder.show(false)

    increamFansOrder
      .rdd.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf("dy:author-rank"))
      val table = connection.getTable(TableName.valueOf("dy:author-snapshot"))

      part.foreach(arrInfo => {
        val rank = arrInfo(0).toString
        val Label = "B"
        val aid = arrInfo(1).toString
        val nn = arrInfo(2).toString
        val tx = arrInfo(3).toString
        val fc = arrInfo(4).toString.toLong
        val frc = arrInfo(6).toString.toLong
        val l = arrInfo(9)
        val cha = arrInfo(12).toString.toLong

        //        println(rank + "," + aid + "," + nn + "," + tx + "," + fc + "," + frc + "," + l + "," + cha)

        val get = new Get(Bytes.toBytes((DigestUtils.sha1Hex(aid) + yesterdayDate)))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("cmt"))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("srt"))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("dgt"))

        val result = table.get(get)
        val cmt = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("cmt"))
        val srt = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("srt"))
        val dgt = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("dgt"))

        val put = new Put((Label + yesterdayDate + l + rank).getBytes)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(aid))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("fc"), Bytes.toBytes(fc))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("frc"), Bytes.toBytes(frc))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("nn"), Bytes.toBytes(nn))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a"), Bytes.toBytes(tx))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("cmt"), cmt)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("srt"), srt)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("dgt"), dgt)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("fzl"), Bytes.toBytes(cha))


        mutator.mutate(put)
      })
      mutator.close()
    })

    /**
     * 蓝v
     */
    val blueLabelData = odsData.filter($"vLabel".equalTo(2))
    blueLabelData.cache().show(100, false)

    //蓝v不限
    val blueLabelOrder = blueLabelData.select((row_number() over (Window.orderBy($"frc".desc))).alias("rank"), $"*").where("rank < 1501")
    blueLabelOrder.show(100, false)

    blueLabelOrder.rdd.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf("dy:author-rank"))
      val table = connection.getTable(TableName.valueOf("dy:author-snapshot"))

      part.foreach(arrInfo => {
        //        val arrInfo = part.next.toString().split(",")
        val rank = arrInfo(0).toString
        val Label = "E"
        val aid = arrInfo(1).toString
        val nn = arrInfo(2).toString
        val tx = arrInfo(3).toString
        val fc = arrInfo(4).toString.toLong
        val frc = arrInfo(6).toString.toLong
        val l = "不限"

        val get = new Get(Bytes.toBytes((DigestUtils.sha1Hex(aid) + yesterdayDate)))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("cmt"))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("srt"))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("dgt"))

        val result = table.get(get)
        val cmt: Array[Byte] = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("cmt"))
        val srt = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("srt"))
        val dgt = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("dgt"))
        //        println(Bytes.toLong(cmt) + "," + srt)

        val put = new Put((Label + yesterdayDate + l + rank).getBytes)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(aid))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("fc"), Bytes.toBytes(fc))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("frc"), Bytes.toBytes(frc))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("nn"), Bytes.toBytes(nn))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a"), Bytes.toBytes(tx))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("cmt"), cmt)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("srt"), srt)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("dgt"), dgt)

        mutator.mutate(put)
      })
      mutator.close()
    })

    //蓝v行业分类
    val blueDiffLabelOrder = blueLabelData
      .filter($"l".notEqual(""))
      .select(
        (row_number() over (Window.partitionBy($"l").orderBy($"frc".desc))).alias("rank"),
        $"*"
      ).where("rank < 1501")

    blueDiffLabelOrder.rdd.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf("dy:author-rank"))
      val table = connection.getTable(TableName.valueOf("dy:author-snapshot"))

      part.foreach(arrInfo => {
        val rank = arrInfo(0).toString
        val Label = "E"
        val aid = arrInfo(1).toString
        val nn = arrInfo(2).toString
        val tx = arrInfo(3).toString
        val fc = arrInfo(4).toString.toLong
        val frc = arrInfo(6).toString.toLong
        val l = arrInfo(9)
        println(rank + "," + aid + "," + nn + "," + tx + "," + fc + "," + frc + "," + l)

        val get = new Get(Bytes.toBytes((DigestUtils.sha1Hex(aid) + yesterdayDate)))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("cmt"))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("srt"))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("dgt"))

        val result = table.get(get)
        val cmt = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("cmt"))
        val srt = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("srt"))
        val dgt = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("dgt"))

        val put = new Put((Label + yesterdayDate + l + rank).getBytes)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(aid))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("fc"), Bytes.toBytes(fc))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("frc"), Bytes.toBytes(frc))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("nn"), Bytes.toBytes(nn))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a"), Bytes.toBytes(tx))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("cmt"), cmt)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("srt"), srt)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("dgt"), dgt)

        mutator.mutate(put)
      })
      mutator.close()
    })
    //    val blueDiffProvOrder = blueLabelData.filter($"p".notEqual("")).select($"ai", $"p", (row_number() over (Window.partitionBy($"p").orderBy($"frc".desc))).alias("rank"), $"frc")
    //    val blueDiffCityOrder = blueLabelData.filter($"c".notEqual("")).select($"ai", $"c", (row_number() over (Window.partitionBy($"c").orderBy($"frc".desc))).alias("rank"), $"frc")

    blueDiffLabelOrder.show(100, false)

    val labelNotNullData: DataFrame = odsData.where($"l".isNotNull).select("*")
    labelNotNullData.createOrReplaceTempView("labelNotNullData")
    labelNotNullData.show()

    /**
     * 省排名
     */
    val provinceOrder = spark.sql("select * from (select rank() over(partition by p order by frc desc) as d,* from(select * from tmp where p !='')tmp) dim where dim.d < 1501")
    provinceOrder.show(false)

    provinceOrder.rdd.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf("dy:author-rank"))
      val table = connection.getTable(TableName.valueOf("dy:author-snapshot"))

      part.foreach(arrInfo => {
        //        val arrInfo = part.next.toString().split(",")
        //        val rank = arrInfo(0).substring(1, arrInfo(0).length)
        val rank = arrInfo(0).toString
        val Label = "D"
        val aid = arrInfo(1).toString
        val nn = arrInfo(2).toString
        val tx = arrInfo(3).toString
        val fc = arrInfo(4).toString.toLong
        val frc = arrInfo(6).toString.toLong
        val p = arrInfo(7).toString

        //        val c = arrInfo(8).substring(0, arrInfo(8).length - 1)

        val get = new Get(Bytes.toBytes((DigestUtils.sha1Hex(aid) + yesterdayDate)))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("cmt"))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("srt"))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("dgt"))

        val result = table.get(get)
        val cmt = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("cmt"))
        val srt = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("srt"))
        val dgt = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("dgt"))
        //        println(Bytes.toLong(cmt) + "," + srt)

        val put = new Put((Label + yesterdayDate + p + rank).getBytes)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(aid))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("fc"), Bytes.toBytes(fc))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("frc"), Bytes.toBytes(frc))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("nn"), Bytes.toBytes(nn))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a"), Bytes.toBytes(tx))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("cmt"), cmt)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("srt"), srt)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("dgt"), dgt)

        mutator.mutate(put)
      })
      mutator.close()
    })

    /**
     * 城市排名
     */
    val cityOrder = spark.sql("select * from(select rank() over(partition by c order by frc desc) as t,* from (select * from tmp where c !='')tmp ) dim where dim.t<1501")
    cityOrder.show(2000, false)
    cityOrder.rdd.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf("dy:author-rank"))
      val table = connection.getTable(TableName.valueOf("dy:author-snapshot"))

      part.foreach(arrInfo => {
        //        val arrInfo = part.next.toString().split(",")
        val rank = arrInfo(0).toString
        val Label = "D"
        val aid = arrInfo(1).toString
        val nn = arrInfo(2).toString
        val tx = arrInfo(3).toString
        val fc = arrInfo(4).toString.toLong
        val frc = arrInfo(6).toString.toLong
        //        val p = arrInfo(7).toString
        val c = arrInfo(8).toString

        val get = new Get(Bytes.toBytes((DigestUtils.sha1Hex(aid) + yesterdayDate)))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("cmt"))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("srt"))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("dgt"))

        val result = table.get(get)
        val cmt = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("cmt"))
        val srt = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("srt"))
        val dgt = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("dgt"))

        val put = new Put((Label + yesterdayDate + c + rank).getBytes)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(aid))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("fc"), Bytes.toBytes(fc))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("frc"), Bytes.toBytes(frc))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("nn"), Bytes.toBytes(nn))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a"), Bytes.toBytes(tx))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("cmt"), cmt)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("srt"), srt)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("dgt"), dgt)

        mutator.mutate(put)
      })
      mutator.close()
    })

    cityOrder.createOrReplaceTempView("dataOrder")


    /**
     * 粉丝排名：综合
     */
    spark.sql("select rank() over(order by frc desc) as d,* from tmp limit 1501")
      .rdd.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf("dy:author-rank"))
      val table = connection.getTable(TableName.valueOf("dy:author-snapshot"))

      part.foreach(arrInfo => {
        //        val arrInfo = part.next.toString().split(",")
        val rank = arrInfo(0).toString
        val Label = "A"
        val aid = arrInfo(1).toString
        val nn = arrInfo(2).toString
        val tx = arrInfo(3).toString
        val fc = arrInfo(4).toString.toLong
        val frc = arrInfo(6).toString.toLong
        val l = "不限"

        val get = new Get(Bytes.toBytes((DigestUtils.sha1Hex(aid) + yesterdayDate)))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("cmt"))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("srt"))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("dgt"))

        val result = table.get(get)
        val cmt: Array[Byte] = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("cmt"))
        val srt = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("srt"))
        val dgt = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("dgt"))
        //        println(Bytes.toLong(cmt) + "," + srt)

        val put = new Put((Label + yesterdayDate + l + rank).getBytes)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(aid))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("fc"), Bytes.toBytes(fc))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("frc"), Bytes.toBytes(frc))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("nn"), Bytes.toBytes(nn))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a"), Bytes.toBytes(tx))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("cmt"), cmt)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("srt"), srt)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("dgt"), dgt)

        mutator.mutate(put)
      })
      mutator.close()
    })


    /**
     * 粉丝排名：分类
     */
    val fansOrder: DataFrame = spark.sql("select * from (select rank() over(partition by l order by frc desc) as d,* from labelNotNullData) dim where dim.d < 1501")
    fansOrder.show(false)

    fansOrder.rdd.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf("dy:author-rank"))
      val table = connection.getTable(TableName.valueOf("dy:author-snapshot"))

      part.foreach(arrInfo => {
        val rank = arrInfo(0).toString
        val Label = "A"
        val aid = arrInfo(1).toString
        val nn = arrInfo(2).toString
        val tx = arrInfo(3).toString
        val fc = arrInfo(4).toString.toLong
        val frc = arrInfo(6).toString.toLong
        val l = arrInfo(9)
        println(rank + "," + aid + "," + nn + "," + tx + "," + fc + "," + frc + "," + l)

        val get = new Get(Bytes.toBytes((DigestUtils.sha1Hex(aid) + yesterdayDate)))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("cmt"))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("srt"))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("dgt"))

        val result = table.get(get)
        val cmt = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("cmt"))
        val srt = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("srt"))
        val dgt = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("dgt"))

        val put = new Put((Label + yesterdayDate + l + rank).getBytes)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(aid))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("fc"), Bytes.toBytes(fc))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("frc"), Bytes.toBytes(frc))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("nn"), Bytes.toBytes(nn))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a"), Bytes.toBytes(tx))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("cmt"), cmt)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("srt"), srt)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("dgt"), dgt)

        mutator.mutate(put)
      })
      mutator.close()
    })

    spark.stop()

  }
}
