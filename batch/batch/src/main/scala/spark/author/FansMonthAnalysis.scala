package spark.author

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.LongType
import utils.SparkUtils

/**
 * 博主月榜单
 */
object FansMonthAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ActionOperation")
      .config("spark.yarn.maxAppAttempts", "1")
      .master("local[*]")
      .getOrCreate()

    //todo 结果表要改成正式表
    val tableName = "test:ljk" //dy:author-month-rank"
    val dayTime = "20191031"
    val sc = spark.sparkContext
    import org.apache.spark.sql.functions._
    import spark.implicits._

    //获取昨天和一月前日期
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance
    calendar.add(Calendar.HOUR_OF_DAY, -24)

    //val yesterday = dateFormat.format(calendar.getTime)
    //todo
    val yesterday = "20191027"

    calendar.add(Calendar.HOUR_OF_DAY, -144)

    //todo
    val dayMonthAgo = "20191025"
    //val dayMonthAgo = dateFormat.format(calendar.getTime)

    println(yesterday + "," + dayMonthAgo)

    //获取上个月的数据
    val dateArr: Array[String]  = SparkUtils.datesOfMonth()
    val scanArr = new Array[Scan](dateArr.length)
    val scan0 = new Scan()
    scan0.setRowPrefixFilter(DigestUtils.sha1Hex(yesterday).getBytes())

    val scanString = TableMapReduceUtil.convertScanToString(scan0)
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.regionsizecalculator.enable", "false") //aliyun hbase 不加有问题
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "dy:author-daily")
    hbaseConf.set(TableInputFormat.SCAN, scanString)

    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.
      newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
        classOf[ImmutableBytesWritable],
        classOf[Result])

    //todo
    //for(i <- 1 to dateArr.length - 1){
    for(i <- 25 to 26){
      val scan = new Scan()
      scan.setRowPrefixFilter(DigestUtils.sha1Hex(dateArr(i)).getBytes())
      val scanString = TableMapReduceUtil.convertScanToString(scan)
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set("hbase.regionsizecalculator.enable", "false") //aliyun hbase 不加有问题
      hbaseConf.set(TableInputFormat.INPUT_TABLE, "dy:author-daily")
      hbaseConf.set(TableInputFormat.SCAN, scanString)

      val tmpRDD: RDD[(ImmutableBytesWritable, Result)] = sc.
        newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
          classOf[ImmutableBytesWritable],
          classOf[Result])

      hbaseRDD.union(tmpRDD)
    }

    val odsData: DataFrame = hbaseRDD.filter(line => {
      if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("fc")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("fic")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("frc")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("l")) != null
      ) {
        true
      } else {
        false
      }
    }).mapPartitions(part => {
      val tuples: Iterator[(String, String, String, Long, Long, Long, String, String, String, String, Int)] = part.map(r => {
        var vLabel = 0
        //        var label = ""
        //        var getLabel = r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ml"))

        if (r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("vt")) != null) {
          vLabel = Bytes.toInt(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("vt")))
        }

        //        if (getLabel != null && !Bytes.toString(getLabel).equals("")) {
        //          label = Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ml")))
        //        } else {
        //          label = Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("l")))
        //        }

        (
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ai"))), //uid
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("nn"))), //姓名
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("a"))), //头像
          Bytes.toLong(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("fc"))), //点赞数
          //          Bytes.toLong(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("cmt"))), //评论数
          //          Bytes.toLong(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("srt"))), //转发数
          Bytes.toLong(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("fic"))), //关注总数
          Bytes.toLong(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("frc"))), //粉丝总数
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("p"))), //province
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("c"))), //city
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("l"))), //label
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("d"))), //date
          vLabel
        )
      })
      tuples
    }).toDF("ai", "nn", "a", "fc", "fic", "frc", "p", "c", "l", "d", "vLabel")
    odsData.cache().show(false)
    odsData.createOrReplaceTempView("tmp")

    //过滤最大和最小的日期
    val FansIncreamingList = odsData.select(
      $"*",
      (max("d") over Window.partitionBy("ai")).alias("maxd"),
      (min("d") over Window.partitionBy("ai")).alias("mind")
    )

    val maxDataFansData = FansIncreamingList.where($"d".equalTo($"maxd")).select($"ai", $"frc")
    val minDataFansData = FansIncreamingList.where($"d".equalTo($"mind")).select($"ai", $"frc".alias("minfrc"))

    maxDataFansData.show(false)
    minDataFansData.show(false)

    //粉丝月榜
    val FansMonthData = maxDataFansData.join(minDataFansData, maxDataFansData("ai") === minDataFansData("ai"))
      //      .select(maxDataFansData("*"), minDataFansData("frc"), (maxDataFansData("frc") - minDataFansData("frc")).alias("chafrc"))
      .select(maxDataFansData("*"), minDataFansData("minfrc"))

    FansMonthData.show(false)

    //    FansMonthData.select(
    //      $"*",
    //      (row_number() over (Window.orderBy(desc("chafrc")))).alias("rank"),
    //      (row_number() over (Window.partitionBy("l").orderBy(desc("chafrc")))).alias("rankl")
    //    ).show(false)
    //
    //    val BlueVMapData = odsData.filter($"vLabel".equalTo(2)).groupBy("ai")
    //      .agg((sum("frc") / count("*")).alias("frcAvg"))
    //
    //    BlueVMapData.join(maxDataFansData, BlueVMapData("ai") === maxDataFansData("ai"))
    //      .orderBy("frcAvg")
    //      .show(false)

    /**
     * 扫描博主总表：连表取详情
     */
    val hbaseConf2 = HBaseConfiguration.create()
    hbaseConf2.set("hbase.regionsizecalculator.enable", "false")
    hbaseConf2.set(TableInputFormat.INPUT_TABLE, "dy:author")

    val hbaseRDD2: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hbaseConf2,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val odsAuthorIndexData = hbaseRDD2.filter(line => {
      if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("fc")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("frc")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("fic")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ai")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("nn")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("a")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("l")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("mmi")) != null
      ) {
        true
      } else {
        false
      }
    }).mapPartitions(part => {
      val tuples = part.map(r => {
        var vLabel = 0

        if (r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("vt")) != null) {
          vLabel = Bytes.toInt(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("vt")))
        }

        (
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ai"))), //uid
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("nn"))), //姓名
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("a"))), //头像
          Bytes.toLong(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("fc"))), //点赞数
          Bytes.toLong(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("fic"))), //关注总数
          Bytes.toLong(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("frc"))), //粉丝总数
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("p"))), //province
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("c"))), //city
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("l"))), //label
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("mmi"))), //博主传播指数
          vLabel
        )
      })
      tuples
    }).toDF("ai", "nn", "a", "fc", "fic", "frc", "p", "c", "l", "mmi", "vLabel")
      .filter($"l".notEqual(""))

    odsAuthorIndexData.show(false)

    val AuthorMonthfansData = FansMonthData.join(odsAuthorIndexData, odsAuthorIndexData("ai") === FansMonthData("ai"))
      .select(FansMonthData("*"), (FansMonthData("frc") - FansMonthData("minfrc")).alias("chafrc"), odsAuthorIndexData("l"))

    AuthorMonthfansData.show(false)

    //按粉丝增量排序
    val authorFansOrder = AuthorMonthfansData.select(
      $"*",
      (row_number() over (Window.orderBy(desc("chafrc")))).alias("rankfrc"),
      (row_number() over (Window.partitionBy("l").orderBy(desc("chafrc")))).alias("ranklfrc")
    ).filter($"rankfrc" > 1500 && $"ranklfrc" > 1500)

    authorFansOrder.show(false)

    authorFansOrder.foreachPartition(part => {
      //
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf(tableName))
      part.foreach(line => {
        val Label = "B"
        val title = "不限"
        val ai = line(0).toString
        val chafrc = line(3).toString.toLong
        val l = line(4).toString
        val rankfrc = line(5).toString
        val ranklfrc = line(6).toString

        println(ai + "," + rankfrc + "," + ranklfrc + "," + l)

        if (rankfrc.toInt < 1501) {
          val put = new Put((Label + dayTime + title + rankfrc).getBytes)
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(ai))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("l"), Bytes.toBytes(title))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rankfrc))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chafrc"), Bytes.toBytes(chafrc))

          mutator.mutate(put)
        }

        if (ranklfrc.toInt < 1501) {
          val put2 = new Put((Label + dayTime + l + ranklfrc).getBytes)
          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(ai))
          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("l"), Bytes.toBytes(l))
          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(ranklfrc))
          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chafrc"), Bytes.toBytes(chafrc))

          mutator.mutate(put2)
        }
      })
      mutator.close()
    })

    //最大标签
    val authorMaxLabel = odsData.where($"l".isNotNull && $"l".notEqual(""))
      .groupBy("ai", "l")
      .count()
      .groupBy("ai")
      .agg(max(struct("count", "l")) as "max")
      .select("ai", "max.*")

    authorMaxLabel.show(false)

    //过滤
    val odsYesterDay = odsData.filter($"p".notEqual("") && $"d".equalTo(yesterday))
    val odsFilterCity = odsData.filter($"c".notEqual("") && $"d".equalTo(yesterday))

    //蓝v不限
    val blueLabelData = odsData.filter($"vLabel".equalTo(2))
    //计算月平均粉丝数
    val blueAuthorMonth: DataFrame = blueLabelData.groupBy("ai").agg((sum("frc") / count("ai")).cast(LongType).alias("frcAvg"))

    val blueAuthorOrder = blueAuthorMonth.select($"ai", row_number().over(Window.orderBy($"frcAvg".desc)).alias("rank"), $"frcAvg").where("rank < 1501")
    blueAuthorOrder.show(false)

    blueAuthorOrder.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf(tableName))
      part.foreach(line => {
        val Label = "E"
        val l = "不限"
        val ai = line(0).toString
        val rank = line(1).toString
        val frcAvg = line(2).toString
        println(ai + "," + rank)

        val put = new Put((Label + dayTime + l + rank).getBytes)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(ai))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("l"), Bytes.toBytes(l))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("frcAvg"), Bytes.toBytes(frcAvg))

        mutator.mutate(put)
      })
      mutator.close()
    })

    //蓝v 行业分类
    val blueAuthorWithLabel = blueAuthorMonth.join(authorMaxLabel, authorMaxLabel("ai") === blueAuthorOrder("ai"))

    blueAuthorWithLabel.show(false)

    val blueLabelOrder = blueAuthorWithLabel.select(
      authorMaxLabel("l"),
      (row_number().over(Window.partitionBy("l").orderBy($"frcAvg".desc))).alias("rank"),
      authorMaxLabel("ai"),
      $"frcAvg"
    ).where("rank < 1501")

    blueLabelOrder.show(1000, false)
    blueLabelOrder.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf(tableName))
      part.foreach(line => {
        val Label = "E"
        val l = line(0).toString
        val rank = line(1).toString
        val ai = line(2).toString
        if (line(3) == null) {
          println(ai + "--- null")
        }

        println(ai + "," + rank)

        val frcAvg = line(3).toString

        val put = new Put((Label + dayTime + l + rank).getBytes)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(ai))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("l"), Bytes.toBytes(l))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("frcAvg"), Bytes.toBytes(frcAvg))

        mutator.mutate(put)
      })
      mutator.close()
    })



    //不限
    val odsfrcSum = odsData.groupBy("ai")
      .agg(
        (sum("frc") / count("ai")).cast(LongType).alias("frcAvg")
        //        (sum("cmt") / 7).cast(LongType).alias("cmtAvg"),
        //        (sum("srt") / 7).cast(LongType).alias("srtAvg")
      )

    val odsfrcOrder: DataFrame = odsfrcSum.select($"ai", row_number().over(Window.orderBy($"frcAvg".desc)).alias("rank"), $"frcAvg").where("rank < 1501")

    odsfrcSum.show(false)

    odsfrcOrder.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf(tableName))
      part.foreach(line => {
        val Label = "A"
        val l = "不限"
        val ai = line(0).toString
        val rank = line(1).toString
        val frcAvg = line(2).toString
        println(ai + "," + rank)

        val put = new Put((Label + dayTime + l + rank).getBytes)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(ai))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("l"), Bytes.toBytes(l))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("frcAvg"), Bytes.toBytes(frcAvg))

        mutator.mutate(put)
      })
      mutator.close()
    })


    //行业分类
    authorMaxLabel.show(false)
    val authorFinalData: DataFrame = authorMaxLabel.join(odsfrcSum, authorMaxLabel("ai") === odsfrcSum("ai"))

    authorFinalData.show(500, false)

    authorFinalData.select(
      authorMaxLabel("l"),
      (row_number().over(Window.partitionBy("l").orderBy($"frcAvg".desc))).alias("rank"),
      authorMaxLabel("ai"),
      $"frcAvg"
    ).where("rank < 1501")
      .foreachPartition(part => {
        val connection = ConnectionFactory.createConnection()
        val mutator = connection.getBufferedMutator(TableName.valueOf(tableName))
        part.foreach(line => {
          val Label = "A"
          val l = line(0).toString
          val rank = line(1).toString
          val ai = line(2).toString
          val frcAvg = line(3).toString

          val put = new Put((Label + dayTime + l + rank).getBytes)
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(ai))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("l"), Bytes.toBytes(l))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("frcAvg"), Bytes.toBytes(frcAvg))

          mutator.mutate(put)
        })
        mutator.close()
      })


    //省分类
    val odsProvinceData = odsYesterDay.select("ai", "p")
      .join(odsfrcSum, odsYesterDay("ai") === odsfrcSum("ai"))
      .select(odsYesterDay("ai"),
        (row_number().over(Window.partitionBy("p").orderBy($"frcAvg".desc))).alias("rank"),
        odsYesterDay("p"),
        $"frcAvg"
      ).where("rank < 1501")

    odsProvinceData.show(false)

    odsProvinceData
      .foreachPartition(part => {
        val connection = ConnectionFactory.createConnection()
        val mutator = connection.getBufferedMutator(TableName.valueOf(tableName))
        part.foreach(line => {
          val Label = "D"
          val ai = line(0).toString
          val rank = line(1).toString
          val p = line(2).toString
          val frcAvg = line(3).toString

          val put = new Put((Label + dayTime + p + rank).getBytes)
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(ai))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("p"), Bytes.toBytes(p))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("frcAvg"), Bytes.toBytes(frcAvg))

          mutator.mutate(put)
        })
        mutator.close()
      })

    //市分类
    val odsCityData = odsFilterCity.select("ai", "c")
      .join(odsfrcSum, odsYesterDay("ai") === odsfrcSum("ai"))
      .select(odsYesterDay("ai"),
        (row_number().over(Window.partitionBy("c").orderBy($"frcAvg".desc))).alias("rank"),
        odsYesterDay("c"),
        $"frcAvg"
      ).where("rank < 1501")

    odsCityData.show(false)

    odsCityData.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf(tableName))
      part.foreach(line => {
        val Label = "D"
        val ai = line(0).toString
        val rank = line(1).toString
        val c = line(2).toString
        val frcAvg = line(3).toString

        val put = new Put((Label + dayTime + c + rank).getBytes)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(ai))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("p"), Bytes.toBytes(c))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("frcAvg"), Bytes.toBytes(frcAvg))

        mutator.mutate(put)
      })
      mutator.close()
    })


  }


}
