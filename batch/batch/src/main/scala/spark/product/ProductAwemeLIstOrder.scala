package spark.product

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import utils.SparkUtils

/**
 * 抖音商品视频日榜：（参数）包含今日电商榜
 */
object ProductAwemeLIstOrder {
  def main(args: Array[String]): Unit = {
    //取aweme-daily快照
    //取两天的算点赞增量，按播主行业排序，按商品类别排序
    //算当天的实时榜单按点赞总量，按商品类别排序
    val spark = SparkSession
      .builder()
      .appName("ActionOperation")
      .config("spark.yarn.maxAppAttempts", "1")
      //      .master("local[*]")
      .getOrCreate()

    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance
    var beginData = ""
    var endData = ""
    val arg = SparkUtils.functionArgs(args)
    if (arg.equals("今日电商视频日榜")) {
      beginData = dateFormat.format(calendar.getTime)
      calendar.add(Calendar.HOUR_OF_DAY, -24)
      endData = dateFormat.format(calendar.getTime)
    } else {
      calendar.add(Calendar.HOUR_OF_DAY, -24)
      beginData = dateFormat.format(calendar.getTime)
      calendar.add(Calendar.HOUR_OF_DAY, -24)
      endData = dateFormat.format(calendar.getTime)
    }


    println(beginData + "," + endData)


    val sc = spark.sparkContext
    import spark.implicits._
    import org.apache.spark.sql.functions._

    /**
     * 扫描昨天
     */
    val scan = new Scan()
    scan.setRowPrefixFilter(DigestUtils.sha1Hex(beginData).getBytes())
    val scanString = TableMapReduceUtil.convertScanToString(scan)
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.regionsizecalculator.enable", "false") //aliyun hbase 不加有问题
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "dy:aweme-daily")
    hbaseConf.set(TableInputFormat.SCAN, scanString)

    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val yesterDayAwemeData = hbaseRDD.filter(line => {
      if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ps")) != null) {
        true
      } else {
        false
      }
    }).mapPartitions(part => {
      val tuples = part.map(line => {
        (
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ai"))),
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ps"))),
          Bytes.toLong(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("dc")))
        )
      })
      tuples
    }).toDF("ai", "ps", "dc").filter($"ps".notEqual("") && $"ai".notEqual("null"))
    yesterDayAwemeData.show(false)

    /**
     * 扫描前天
     */
    val scan2 = new Scan()
    scan2.setRowPrefixFilter(DigestUtils.sha1Hex(endData).getBytes())
    val scanString2 = TableMapReduceUtil.convertScanToString(scan2)
    val hbaseConf2 = HBaseConfiguration.create()
    hbaseConf2.set("hbase.regionsizecalculator.enable", "false") //aliyun hbase 不加有问题
    hbaseConf2.set(TableInputFormat.INPUT_TABLE, "dy:aweme-daily")
    hbaseConf2.set(TableInputFormat.SCAN, scanString2)

    val hbaseRDD2: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hbaseConf2,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val yesterBeforeAwemeData = hbaseRDD2.filter(line => {
      if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ps")) != null) {
        true
      } else {
        false
      }
    }).mapPartitions(part => {
      val tuples = part.map(line => {
        (
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ai"))),
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ps"))),
          Bytes.toLong(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("dc")))
        )
      })
      tuples
    }).toDF("ai", "ps", "dc").filter($"ps".notEqual("") && $"ai".notEqual("null"))
    yesterBeforeAwemeData.show(false)

    val AwemeTwoDayData = yesterDayAwemeData.join(yesterBeforeAwemeData, yesterDayAwemeData("ai") === yesterBeforeAwemeData("ai"))
      .select(yesterDayAwemeData("*"), (yesterDayAwemeData("dc") - yesterBeforeAwemeData("dc")).alias("chadc"))

    AwemeTwoDayData.show(false)


    if (arg.equals("今日电商视频日榜")) {
      AwemeTwoDayData
        .orderBy(desc("dc"))
        .limit(1501)
        .withColumn("rank", monotonically_increasing_id + 1)
        .foreachPartition(
          part => {
            val connection = ConnectionFactory.createConnection()
            val mutator = connection.getBufferedMutator(TableName.valueOf("dy:product-aweme-rank"))
            part.foreach(line => {
              val title = "AB"
              val aid = line(0).toString
              val dc = line(2).toString.toLong
              val chadc = line(3).toString.toLong
              val rank = line(4).toString.toInt
              val label = "不限"

              val put = new Put((title + beginData + label + rank).getBytes)
              put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chadc"), Bytes.toBytes(chadc))
              put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("dc"), Bytes.toBytes(dc))
              put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
              put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(aid))

              mutator.mutate(put)
            })
            mutator.close()
          })
    } else {
      //全部
      AwemeTwoDayData.orderBy(desc("chadc"))
        .limit(1501)
        .withColumn("rank", monotonically_increasing_id + 1)
        .foreachPartition(
          part => {
            val connection = ConnectionFactory.createConnection()
            val mutator = connection.getBufferedMutator(TableName.valueOf("dy:product-aweme-rank"))
            part.foreach(line => {
              val title = "AB"
              val aid = line(0).toString
              val dc = line(2).toString.toLong
              val chadc = line(3).toString.toLong
              val rank = line(4).toString.toInt
              val label = "不限"

              val put = new Put((title + beginData + label + rank).getBytes)
              put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chadc"), Bytes.toBytes(chadc))
              put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("dc"), Bytes.toBytes(dc))
              put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
              put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(aid))

              mutator.mutate(put)
            })
            mutator.close()
          })
    }

    /**
     * 扫描视频表
     */
    val hbaseConf3 = HBaseConfiguration.create()
    hbaseConf3.set("hbase.regionsizecalculator.enable", "false") //aliyun hbase 不加有问题
    hbaseConf3.set(TableInputFormat.INPUT_TABLE, "dy:aweme")

    val hbaseRDD3: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hbaseConf3,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val awemeData = hbaseRDD3.filter(line => {
      if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ai")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("l")) != null) {
        true
      } else {
        false
      }
    }).mapPartitions(part => {
      val tuples = part.map(line => {
        (
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ai"))),
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("l")))
        )
      })
      tuples
    }).toDF("ai", "l").filter($"ai".notEqual(""))

    awemeData.show(false)

    //获取行业分类
    AwemeTwoDayData.join(awemeData, AwemeTwoDayData("ai") === awemeData("ai"))
      .select(
        AwemeTwoDayData("*"),
        awemeData("l"),
        (row_number() over ((Window.partitionBy("l").orderBy(desc("chadc"))))).alias("rank")
      ).where("rank < 1501").foreachPartition(
      part => {
        val connection = ConnectionFactory.createConnection()
        val mutator = connection.getBufferedMutator(TableName.valueOf("dy:product-aweme-rank"))
        part.foreach(line => {
          val title = "B"
          val aid = line(0).toString
          val dc = line(2).toString.toLong
          val chadc = line(3).toString.toLong
          val label = line(4).toString
          val rank = line(5).toString.toInt

          val put = new Put((title + beginData + label + rank).getBytes)
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chadc"), Bytes.toBytes(chadc))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("dc"), Bytes.toBytes(dc))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(aid))

          mutator.mutate(put)
        })
        mutator.close()
      }
    )

    /**
     * 扫描商品表
     */
    val hbaseConf4 = HBaseConfiguration.create()
    hbaseConf4.set("hbase.regionsizecalculator.enable", "false") //aliyun hbase 不加有问题
    hbaseConf4.set(TableInputFormat.INPUT_TABLE, "dy:promotion")

    val hbaseRDD4: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hbaseConf4,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val productData = hbaseRDD4.filter(line => {
      if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("awi")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("c1")) != null) {
        true
      } else {
        false
      }
    }).mapPartitions(part => {
      val tuples = part.map(line => {
        (
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("awi"))),
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("c1")))
        )
      })
      tuples
    }).toDF("ai", "l").filter($"ai".notEqual("") && $"l".notEqual(""))
      .distinct()

    productData.show(false)


    if (arg.equals("今日电商视频日榜")) {
      //获取商品分类
      val todayProductData = AwemeTwoDayData.join(productData, AwemeTwoDayData("ai") === productData("ai"))
        .select(
          AwemeTwoDayData("*"),
          productData("l"),
          (row_number() over ((Window.partitionBy("l").orderBy(desc("dc"))))).alias("rank")
        ).where("rank < 1501")

      todayProductData.show(false)

      todayProductData.foreachPartition(
        part => {
          val connection = ConnectionFactory.createConnection()
          val mutator = connection.getBufferedMutator(TableName.valueOf("dy:product-aweme-rank"))
          part.foreach(line => {
            val title = "A"
            val aid = line(0).toString
            val dc = line(2).toString.toLong
            val chadc = line(3).toString.toLong
            val label = line(4).toString
            val rank = line(5).toString.toInt

            val put = new Put((title + beginData + label + rank).getBytes)
            put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chadc"), Bytes.toBytes(chadc))
            put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("dc"), Bytes.toBytes(dc))
            put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
            put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(aid))

            mutator.mutate(put)
          })
          mutator.close()
        }
      )
    } else {
      //获取商品分类
      AwemeTwoDayData.join(productData, AwemeTwoDayData("ai") === productData("ai"))
        .select(
          AwemeTwoDayData("*"),
          productData("l"),
          (row_number() over ((Window.partitionBy("l").orderBy(desc("chadc"))))).alias("rank")
        ).where("rank < 1501").foreachPartition(
        part => {
          val connection = ConnectionFactory.createConnection()
          val mutator = connection.getBufferedMutator(TableName.valueOf("dy:product-aweme-rank"))
          part.foreach(line => {
            val title = "A"
            val aid = line(0).toString
            val dc = line(2).toString.toLong
            val chadc = line(3).toString.toLong
            val label = line(4).toString
            val rank = line(5).toString.toInt

            val put = new Put((title + beginData + label + rank).getBytes)
            put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chadc"), Bytes.toBytes(chadc))
            put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("dc"), Bytes.toBytes(dc))
            put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
            put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(aid))

            mutator.mutate(put)
          })
          mutator.close()
        }
      )
    }
  }
}
