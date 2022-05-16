package test

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Calendar

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.types.DoubleType
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import utils.SparkUtils

/**
 * 视频指数
 * 博主指数
 */
object CalIndexTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ActionOperation")
      .config("spark.yarn.maxAppAttempts", "1")
      .config("spark.sql.shuffle.partitions", "300")
      //      .master("local[*]")
      //.config(ConfigurationOptions.ES_NODES, "es-cn-mp91asj1e0007gyzj.elasticsearch.aliyuncs.com")
      //.config(ConfigurationOptions.ES_NODES_WAN_ONLY, "true")
      //.config(ConfigurationOptions.ES_PORT, "9200")
      //.config(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, "elastic")
      //.config(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, "chanmama@123")
      .config("es.batch.size.bytes", "1mb")
      .config("es.batch.size.entries", "10000")
      .getOrCreate()

    val arg = SparkUtils.functionArgs(args)
    var yesterday = ""

    if(arg.equals("p") ){
      val dateFormat = new SimpleDateFormat("yyyyMMdd")
      val calendar = Calendar.getInstance
      calendar.add(Calendar.HOUR_OF_DAY, -24)
      yesterday = dateFormat.format(calendar.getTime)
      calendar.add(Calendar.MONTH, -1)
    }else{
      //指定日期
      yesterday = arg
    }

    val sc = spark.sparkContext
    import spark.implicits._
    import org.apache.spark.sql.functions._

    println(yesterday)

    val scanAw = new Scan()
    scanAw.setRowPrefixFilter(DigestUtils.sha1Hex(yesterday).getBytes())
    val scanAwString = TableMapReduceUtil.convertScanToString(scanAw)
    val hbaseConfAw = HBaseConfiguration.create()
    hbaseConfAw.set("hbase.regionsizecalculator.enable", "false") //aliyun hbase 不加有问题
    hbaseConfAw.set(TableInputFormat.INPUT_TABLE, "dy:aweme-daily")
    hbaseConfAw.set(TableInputFormat.SCAN, scanAwString)

    val hbaseRDDAw: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hbaseConfAw,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val odsData = hbaseRDDAw.filter(line => {
      if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ps")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ct")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ai")) != null) {
        true
      } else {
        false
      }
    }).mapPartitions(part => {
      val tuples = part.map(line => {
        (
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ps"))), //商品id
          Bytes.toLong(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ct"))), //时间
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("a"))), //博主id
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ai"))), //视频id
          Bytes.toLong(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("dc"))), //点赞
          Bytes.toLong(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("cc"))), //评论
          Bytes.toLong(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("sc"))) //转发
        )
      })
      tuples
    }).toDF("ps", "ct", "a", "ai", "dc", "cc", "sc")

    odsData.show(false)

    val scanAu = new Scan()
    scanAu.setRowPrefixFilter(DigestUtils.sha1Hex(yesterday).getBytes())
    val scanAuString = TableMapReduceUtil.convertScanToString(scanAu)
    val hbaseConfAu = HBaseConfiguration.create()
    hbaseConfAu.set("hbase.regionsizecalculator.enable", "false") //aliyun hbase 不加有问题
    hbaseConfAu.set(TableInputFormat.INPUT_TABLE, "dy:author-daily")
    hbaseConfAu.set(TableInputFormat.SCAN, scanAuString)

    val hbaseRDDAu: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hbaseConfAu,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val odsAuthorIndexData = hbaseRDDAu.mapPartitions(part => {
      val tuples = part.map(line => {
        var frc: Long = 0L
        var fc: Long = 0L
        //        var cmt: Long = 0L
        //        var srt: Long = 0L
        if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("frc")) != null) {
          frc = Bytes.toLong(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("frc")))
        }

        if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("fc")) != null) {
          fc = Bytes.toLong(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("fc")))
        }

        (
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ai"))), //博主id
          frc, //总粉丝数
          fc // 点赞数
        )
      })
      tuples
    }).toDF("a", "frc", "fc")

    odsAuthorIndexData.show(false)


    //博主id，视频id，点赞数，评论数，转发数，时间
    val odsAwemeIndexData = odsData.select("a", "ai", "dc", "cc", "sc", "ct").filter($"a".notEqual(""))
    val odsAwemeJoinedAuthor = odsAwemeIndexData.join(odsAuthorIndexData, odsAwemeIndexData("a") === odsAuthorIndexData("a"))
      .select(odsAwemeIndexData("*"), odsAuthorIndexData("fc"))

    //视频传播指数
    val awemeIndexData = odsAwemeJoinedAuthor.select(
      $"a",
      $"ai",
      $"ct",
      round(((($"fc" / 100) * 0.1) + (($"dc" / 10000) * 0.5) + (($"cc" / 1000) * 0.2) + (($"sc" / 100) * 0.2)), 2).alias("awemeIndex")
    ).mapPartitions(part => {
      val tuples = part.map(line => {
        val aweme_id = line(1).toString
        val awemeIndex = Vectors.dense(line(3).toString.toDouble)

        (aweme_id, awemeIndex)
      })
      tuples
    }).toDF("a", "awemeIndex")

    awemeIndexData.show(false)

    val normalizerAweme = new MinMaxScaler()
      .setInputCol("awemeIndex")
      .setOutputCol("normalfeatures")

    val Awememodel = normalizerAweme
      .fit(awemeIndexData)
      .transform(awemeIndexData)
      .orderBy(desc("normalfeatures"))

    Awememodel.show(false)

    val authorIndexData = odsAuthorIndexData.filter($"a".notEqual("") && $"a".notEqual("null")).select($"a",
      round(($"frc") + (($"fc") / 100000), 2).alias("authorIndex"))
      .mapPartitions(part => {
        val tuples = part.map(line => {
          val auhtor_id = line(0).toString
          val authorIndex = Vectors.dense(line(1).toString.toDouble)

          (auhtor_id, authorIndex)
        })
        tuples
      }).toDF("a", "authorIndex")

    authorIndexData.show(false)

    //归一化
    val normalizer1 = new MinMaxScaler()
      .setInputCol("authorIndex")
      .setOutputCol("normalfeatures")
    //      .setP(1.0)

    val Authormodel: DataFrame = normalizer1
      .fit(authorIndexData)
      .transform(authorIndexData)
      .orderBy(desc("normalfeatures"))

    //Vector转double
    val authorFinishData = Authormodel.mapPartitions(part => {
      val tuples = part.map(line => {
        val authorId = line(0).toString
        val normalfeatures = line(2).toString

        (authorId, normalfeatures.substring(1, normalfeatures.length - 1).toDouble)
      })
      tuples
    }).toDF("authorId", "normalfeatures")

    authorFinishData.show(false)


    authorFinishData.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf("dy:author-daily"))

      part.foreach(line => {
        val authorId = line(0).toString
        val authorIndex = line(1).toString.toDouble
        println(authorId + "," + authorIndex)

        val put = new Put((DigestUtils.sha1Hex(yesterday) + authorId).getBytes)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("mmi"), Bytes.toBytes(authorIndex))

        mutator.mutate(put)
      })
      mutator.close()
    })

    val awemeFinishData = Awememodel.mapPartitions(part => {
      val tuples = part.map(line => {
        val awemeId = line(0).toString
        val normalfeatures = line(2).toString

        (awemeId, normalfeatures.substring(1, normalfeatures.length - 1).toDouble)
      })
      tuples
    }).toDF("awemeId", "normalfeatures")
    awemeFinishData.show()

    awemeFinishData.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf("dy:aweme-daily"))

      part.foreach(line => {
        val awemeId = line(0).toString
        val awemeIdIndex = line(1).toString.toDouble
        println(awemeId + "," + awemeIdIndex)

        val put = new Put((DigestUtils.sha1Hex(yesterday)+awemeId).getBytes)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("mmi"), Bytes.toBytes(awemeIdIndex))

        mutator.mutate(put)
      })
      mutator.close()
    })
  }
}

