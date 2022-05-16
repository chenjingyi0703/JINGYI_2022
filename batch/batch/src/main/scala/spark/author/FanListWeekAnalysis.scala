package spark.author

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.SparkUtils

/**
 * 博主的周榜
 */
object FanListWeekAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ActionOperation")
      .config("spark.yarn.maxAppAttempts", "1")
      //      .master("local[*]")
      .getOrCreate()

    val tableName = "dy:author-week-rank"
    //val dayTime = "20191027"
    val sc = spark.sparkContext
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val dateArr = SparkUtils.datesOfWeek()
    val yesterday = dateArr(0)
    val day7ago = dateArr(6)

    println(yesterday + "," + day7ago)

    val scan0 = new Scan()
    scan0.setRowPrefixFilter(DigestUtils.sha1Hex(dateArr(0)).getBytes())
    val hbaseConf = HBaseConfiguration.create()
    val scanString = TableMapReduceUtil.convertScanToString(scan0)
    val dailyAuthor = "dy:author-daily"

    hbaseConf.set("hbase.regionsizecalculator.enable", "false") //aliyun hbase 不加有问题
    hbaseConf.set(TableInputFormat.INPUT_TABLE, dailyAuthor)
    hbaseConf.set(TableInputFormat.SCAN, scanString)

    var hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.
      newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
        classOf[ImmutableBytesWritable],classOf[Result])

    for(i <- 1 to dateArr.length - 1){
      val scan = new Scan()
      scan.setRowPrefixFilter(DigestUtils.sha1Hex(dateArr(i)).getBytes())
      val hbaseConf = SparkUtils.hbaseConfSet(scan, dailyAuthor)

      val tmpRDD: RDD[(ImmutableBytesWritable, Result)] = sc.
        newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
          classOf[ImmutableBytesWritable],classOf[Result])
      hbaseRDD = hbaseRDD.union(tmpRDD)
    }

    val odsData: DataFrame = hbaseRDD.filter(line => {
      if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("fc")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("fic")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("frc")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("l")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("mmi")) != null
      ) {
        true
      } else {
        false
      }
    }).mapPartitions(part => {
      val tuples: Iterator[(String, String, String, Long, Long, Long, String, String, String, String, Double, Int)] = part.map(r => {
        var vLabel = 0

        if (r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("vt")) != null) {
          vLabel = Bytes.toInt(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("vt")))
        }

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
          Bytes.toDouble(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("mmi"))), //指数
          vLabel
        )
      })
      tuples
    }).toDF("ai", "nn", "a", "fc", "fic", "frc", "p", "c", "l", "d", "mmi", "vLabel")
    println("odsData")
    odsData.cache().show(false)
    odsData.createOrReplaceTempView("tmp")

    //过滤最大和最小的日期
    val FansIncreamingList = odsData.select(
      $"*",
      (max("d") over Window.partitionBy("ai")).alias("maxd"),
      (min("d") over Window.partitionBy("ai")).alias("mind")
    )

    val maxDateData = FansIncreamingList.where($"d".equalTo($"maxd")).select($"ai", $"frc")
    val minDateData = FansIncreamingList.where($"d".equalTo($"mind")).select($"ai", $"frc".alias("minfrc"))
    println("date")
    maxDateData.show(false)
    minDateData.show(false)

    //涨粉周榜
    val FansWeekData = maxDateData.join(minDateData, maxDateData("ai") === minDateData("ai"))
      //      .select(maxDataFansData("*"), minDataFansData("frc"), (maxDataFansData("frc") - minDataFansData("frc")).alias("chafrc"))
      .select(maxDateData("*"), minDateData("minfrc"))

    println("fansweekdata")
    FansWeekData.show(false)

    //    FansWeekData.select(
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
          vLabel
        )
      })
      tuples
    }).toDF("ai", "nn", "a", "fc", "fic", "frc", "p", "c", "l", "vLabel")
      .filter($"l".notEqual(""))
    println("odsAuthorIndexData")
    odsAuthorIndexData.show(false)

    val AuthorWeekfansData = FansWeekData.join(odsAuthorIndexData, odsAuthorIndexData("ai") === FansWeekData("ai"))
      .select(FansWeekData("*"), (FansWeekData("frc") - FansWeekData("minfrc")).alias("chafrc")
        , odsAuthorIndexData("l"))
    println("AuthorWeekfansData")
    AuthorWeekfansData.show(false)

    //按粉丝增量排序
    val authorFansOrder = AuthorWeekfansData.select(
      $"*",
      (row_number() over (Window.orderBy(desc("chafrc")))).alias("rankfrc"),
      (row_number() over (Window.partitionBy("l").orderBy(desc("chafrc")))).alias("ranklfrc")
    )
    println("authorFansOrder")
    authorFansOrder.show(false)

    //todo
    /*
    authorFansOrder.foreachPartition(part => {
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
          val put = new Put((Label + yesterday + title + rankfrc).getBytes)
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(ai))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("l"), Bytes.toBytes(title))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rankfrc))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chafrc"), Bytes.toBytes(chafrc))

          mutator.mutate(put)
        }

        if (ranklfrc.toInt < 1501) {
          val put2 = new Put((Label + yesterday + l + ranklfrc).getBytes)
          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(ai))
          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("l"), Bytes.toBytes(l))
          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(ranklfrc))
          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chafrc"), Bytes.toBytes(chafrc))

          mutator.mutate(put2)
        }
      })
      mutator.close()
    })*/

    //最大标签
    val authorMaxLabel = odsData.where($"l".isNotNull && $"l".notEqual(""))
      .groupBy("ai", "l")
      .count()
      .groupBy("ai")
      .agg(max(struct("count", "l")) as "max")
      .select("ai", "max.*")

    println("authorMaxLabel")
    authorMaxLabel.show(false)

    //过滤
    val odsYesterDay = odsData.filter($"p".notEqual("") && $"d".equalTo(yesterday))
    val odsFilterCity = odsData.filter($"c".notEqual("") && $"d".equalTo(yesterday))

    //蓝v不限
    val blueLabelData = odsData.filter($"vLabel".equalTo(2))
    //计算周平均指数
    //val blueAuthorWeek: DataFrame = blueLabelData.groupBy("ai").agg((sum("frc") / count("ai")).cast(LongType).alias("frcAvg"))
    val blueAuthorWeek: DataFrame = blueLabelData.groupBy("ai")
      .agg((sum("mmi") / count("ai") * 100 ).alias("mmiAvg"))

    //val blueAuthorOrder = blueAuthorWeek.select($"ai", row_number().over(Window.orderBy($"frcAvg".desc)).alias("rank"), $"frcAvg").where("rank < 1501")
    val blueAuthorOrder = blueAuthorWeek
      .select($"ai", row_number().over(Window.orderBy($"mmiAvg".desc)).alias("rank"), $"mmiAvg")
      .where("rank < 1501")
    println("blueAuthorOrder")
    blueAuthorOrder.show(false)

    //todo
    /*
    blueAuthorOrder.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf(tableName))
      part.foreach(line => {
        val Label = "E"
        val l = "不限"
        val ai = line(0).toString
        val rank = line(1).toString
        val mmiAvg = line(2).toString
        println(ai + "," + rank)

        val put = new Put((Label + yesterday + l + rank).getBytes)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(ai))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("l"), Bytes.toBytes(l))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("frcAvg"), Bytes.toBytes(mmiAvg))

        mutator.mutate(put)
      })
      mutator.close()
    })*/

    //蓝v 行业分类
    val blueAuthorWithLabel = blueAuthorWeek.join(authorMaxLabel, authorMaxLabel("ai") === blueAuthorOrder("ai"))
    println("blueAuthorWithLabel")
    blueAuthorWithLabel.show(false)

    val blueLabelOrder = blueAuthorWithLabel.select(
      authorMaxLabel("l"),
      (row_number().over(Window.partitionBy("l").orderBy($"mmiAvg".desc))).alias("rank"),
      authorMaxLabel("ai"),
      $"mmiAvg"
    ).where("rank < 1501")
    println("blueLabelOrder")
    blueLabelOrder.show(1000, false)
    //todo
    /*
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

        val mmiAvg = line(3).toString

        val put = new Put((Label + yesterday + l + rank).getBytes)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(ai))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("l"), Bytes.toBytes(l))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("frcAvg"), Bytes.toBytes(mmiAvg))

        mutator.mutate(put)
      })
      mutator.close()
    })*/

    //    //蓝v 省分类
    //    odsYesterDay.select("ai", "p")
    //      .join(blueAuthorWeek, odsYesterDay("ai") === blueAuthorWeek("ai"), "left_outer")
    //      .select(odsYesterDay("ai"),
    //        (row_number().over(Window.partitionBy("p").orderBy($"frcAvg".desc))).alias("rank"),
    //        odsYesterDay("p"),
    //        $"frcAvg"
    //      ).where("rank < 1001").show(false)
    //
    //
    //    //蓝v 市分类
    //    odsFilterCity.select("ai", "c")
    //      .join(blueAuthorWeek, odsYesterDay("ai") === blueAuthorWeek("ai"), "left_outer")
    //      .select(odsYesterDay("ai"),
    //        (row_number().over(Window.partitionBy("c").orderBy($"frcAvg".desc))).alias("rank"),
    //        odsYesterDay("c"),
    //        $"frcAvg"
    //      ).where("rank < 1001").show(false)


    //行业的不限
    val odsMmiAvg = odsData.groupBy("ai")
      .agg(
      (sum("mmi") / count("ai") * 100).cast(LongType).alias("mmiAvg")
    )

    //val odsfrcOrder: DataFrame = odsfrcSum.select($"ai", row_number().over(Window.orderBy($"frcAvg".desc)).alias("rank"), $"frcAvg").where("rank < 1501")
    val odsMmiOrder: DataFrame = odsMmiAvg
      .select($"ai", row_number().over(Window.orderBy($"mmiAvg".desc)).alias("rank"), $"mmiAvg")
      .where("rank < 1501")

    odsMmiAvg.show(false)

    //todo
    /*
    odsMmiOrder.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf(tableName))
      part.foreach(line => {
        val Label = "A"
        val l = "不限"
        val ai = line(0).toString
        val rank = line(1).toString
        val mmiAvg = line(2).toString
        println(ai + "," + rank)

        val put = new Put((Label + yesterday + l + rank).getBytes)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(ai))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("l"), Bytes.toBytes(l))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("frcAvg"), Bytes.toBytes(mmiAvg))

        mutator.mutate(put)
      })
      mutator.close()
    })*/


    //行业分类
    authorMaxLabel.show(false)
    val authorFinalData: DataFrame = authorMaxLabel.join(odsMmiAvg, authorMaxLabel("ai") === odsMmiAvg("ai"))

    authorFinalData.show(500, false)

    //todo
    /*
    authorFinalData.select(
      authorMaxLabel("l"),
      (row_number().over(Window.partitionBy("l").orderBy($"mmiAvg".desc))).alias("rank"),
      authorMaxLabel("ai"),
      $"mmiAvg"
    ).where("rank < 1501")
      .foreachPartition(part => {
        val connection = ConnectionFactory.createConnection()
        val mutator = connection.getBufferedMutator(TableName.valueOf(tableName))
        part.foreach(line => {
          val Label = "A"
          val l = line(0).toString
          val rank = line(1).toString
          val ai = line(2).toString
          val mmiAvg = line(3).toString

          val put = new Put((Label + yesterday + l + rank).getBytes)
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(ai))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("l"), Bytes.toBytes(l))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("frcAvg"), Bytes.toBytes(mmiAvg))

          mutator.mutate(put)
        })
        mutator.close()
      })*/


    //省分类
    val odsProvinceData = odsYesterDay.select("ai", "p")
      .join(odsMmiAvg, odsYesterDay("ai") === odsMmiAvg("ai"))
      .select(odsYesterDay("ai"),
        (row_number().over(Window.partitionBy("p").orderBy($"mmiAvg".desc))).alias("rank"),
        odsYesterDay("p"),
        $"mmiAvg"
      ).where("rank < 1501")

    odsProvinceData.show(false)

    //todo
    /*
    odsProvinceData
      .foreachPartition(part => {
        val connection = ConnectionFactory.createConnection()
        val mutator = connection.getBufferedMutator(TableName.valueOf(tableName))
        part.foreach(line => {
          val Label = "D"
          val ai = line(0).toString
          val rank = line(1).toString
          val p = line(2).toString
          val mmiAvg = line(3).toString

          val put = new Put((Label + yesterday + p + rank).getBytes)
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(ai))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("p"), Bytes.toBytes(p))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("frcAvg"), Bytes.toBytes(mmiAvg))

          mutator.mutate(put)
        })
        mutator.close()
      })*/

    //市分类
    val odsCityData = odsFilterCity.select("ai", "c")
      .join(odsMmiAvg, odsYesterDay("ai") === odsMmiAvg("ai"))
      .select(odsYesterDay("ai"),
        (row_number().over(Window.partitionBy("c").orderBy($"mmiAvg".desc))).alias("rank"),
        odsYesterDay("c"),
        $"mmiAvg"
      ).where("rank < 1501")

    odsCityData.show(false)

    //todo
    /*
    odsCityData.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf(tableName))
      part.foreach(line => {
        val Label = "D"
        val ai = line(0).toString
        val rank = line(1).toString
        val c = line(2).toString
        val mmiAvg = line(3).toString

        val put = new Put((Label + yesterday + c + rank).getBytes)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(ai))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("p"), Bytes.toBytes(c))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("frcAvg"), Bytes.toBytes(mmiAvg))

        mutator.mutate(put)
      })
      mutator.close()
    })*/
  }
}
