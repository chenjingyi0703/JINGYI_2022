package spark.product

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Calendar

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.types.DoubleType
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

/**
 * 商品近三十天的关联博主数和视频数
 * 商品总关联商品数和视频数
 * 视频指数
 * 博主指数
 */
object ProductAuthorAndAwemeCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ActionOperation")
      .config("spark.yarn.maxAppAttempts", "1")
      .config("spark.sql.shuffle.partitions", "300")
      //      .master("local[*]")
      .config(ConfigurationOptions.ES_NODES, "es-cn-mp91asj1e0007gyzj.elasticsearch.aliyuncs.com")
      .config(ConfigurationOptions.ES_NODES_WAN_ONLY, "true")
      .config(ConfigurationOptions.ES_PORT, "9200")
      .config(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, "elastic")
      .config(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, "chanmama@123")
      .config("es.batch.size.bytes", "1mb")
      .config("es.batch.size.entries", "10000")
      .getOrCreate()

    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance
    calendar.add(Calendar.HOUR_OF_DAY, -24)
    val yesterdayDate = dateFormat.format(calendar.getTime)
    calendar.add(Calendar.MONTH, -1)
    val monthDate = dateFormat.format(calendar.getTime)

    println(yesterdayDate + "," + monthDate)


    val sc = spark.sparkContext
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.regionsizecalculator.enable", "false")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "dy:aweme")

    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val odsData = hbaseRDD.filter(line => {
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

    //近三十天的关联博主
    val authorDaysData = odsData.filter($"ps".notEqual("") && $"ai".notEqual("") && $"a".notEqual(""))
      .withColumn("ps", explode(split($"ps", ",")))

    val authorAndAwemeDatas = authorDaysData
      .filter(from_unixtime($"ct", "yyyyMMdd").between(monthDate, yesterdayDate))
      .groupBy("ps")
      .agg(
        countDistinct("a").alias("aun"), // 关联博主数
        countDistinct("ai").alias("awn") // 关联视频数
      )
    authorAndAwemeDatas.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf("dy:product"))

      part.foreach(line => {
        val pid = line(0).toString
        val aun = line(1).toString.toLong
        val awn = line(2).toString.toLong
        println(pid + "," + aun + "," + awn)

        val put = new Put(DigestUtils.sha1Hex(pid).getBytes)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("aun"), Bytes.toBytes(aun))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("awn"), Bytes.toBytes(awn))

        mutator.mutate(put)
      })
      mutator.close()
    })

    val genderMap = Map("es.mapping.id" -> "promotion_id")
    authorAndAwemeDatas.write.format("org.elasticsearch.spark.sql")
      .options(genderMap)
      .mode(SaveMode.Append)
      .option("es.write.operation", "upsert")
      .save("products/origin")


    // 商品总数据
    authorDaysData
      .groupBy("ps")
      .agg(
        countDistinct("a").alias("aun"),
        countDistinct("ai").alias("awn")
      ).foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutatorDaily = connection.getBufferedMutator(TableName.valueOf("dy:product-snapshot"))

      val date = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.now())

      part.foreach(line => {
        val pid = line(0).toString
        val aun = line(1).toString.toLong
        val awn = line(2).toString.toLong
        println(pid + "," + aun + "," + awn)

        val putDaily = new Put((DigestUtils.sha1Hex(pid) + date).getBytes)
        putDaily.addColumn(Bytes.toBytes("r"), Bytes.toBytes("aun"), Bytes.toBytes(aun))
        putDaily.addColumn(Bytes.toBytes("r"), Bytes.toBytes("awn"), Bytes.toBytes(awn))

        mutatorDaily.mutate(putDaily)
      })
      mutatorDaily.close()
    })


    val hbaseConf2 = HBaseConfiguration.create()
    hbaseConf2.set("hbase.regionsizecalculator.enable", "false")
    hbaseConf2.set(TableInputFormat.INPUT_TABLE, "dy:author")

    val hbaseRDD2: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hbaseConf2,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val odsAuthorIndexData = hbaseRDD2.mapPartitions(part => {
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

        //        if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("cmt")) != null) {
        //          cmt = Bytes.toLong(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("cmt")))
        //        }
        //
        //        if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("srt")) != null) {
        //          srt = Bytes.toLong(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("srt")))
        //        }

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

    //博主传播指数
    //(粉丝数/1w)%100 * 0.5 + (近10个视频平均点赞数/1w)%100 * 0.5 +(近10个视频平均评论数/1000)%100 * 0.2 + (近10个视频平均分享数/100)%100 * 0.1
    //    val odsAwemeJoinedAuthorMapData = odsAwemeJoinedAuthor.select($"*",
    //      (row_number() over (Window.partitionBy(odsAwemeIndexData("a")).orderBy("ct"))).alias("rank")
    //    ).where("rank<=10")
    //      .select($"*", (max("rank") over (Window.partitionBy("a"))).alias("maxRank"))
    //
    //    odsAwemeJoinedAuthorMapData.show(false)
    //
    //    val authorIndexData = odsAwemeJoinedAuthorMapData
    //      .select($"*",
    //        round((((sum("dc") over (Window.partitionBy("a"))) / $"maxRank" / 1000) * 0.3)
    //          + (((sum("cc") over (Window.partitionBy("a"))) / $"maxRank" / 1000) * 0.1)
    //          + (((sum("sc") over (Window.partitionBy("a"))) / $"maxRank" / 10) * 0.1)
    //          + (($"fc" / 100000) * 0.5), 2).alias("authorIndex"))
    //      .select($"a", $"authorIndex")
    //      .distinct()
    //      .mapPartitions(part => {
    //        val tuples = part.map(line => {
    //          val auhtor_id = line(0).toString
    //          val authorIndex = Vectors.dense(line(1).toString.toDouble)
    //
    //          (auhtor_id, authorIndex)
    //        })
    //        tuples
    //      }).toDF("a", "authorIndex")
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

    authorFinishData.select($"authorId".alias("author_id"), $"normalfeatures".alias("mm_index"))
      .write
      .format("org.elasticsearch.spark.sql")
      .options(Map("es.mapping.id" -> "author_id"))
      .mode(SaveMode.Append)
      .option("es.write.operation", "upsert")
      .save("authors/origin")

    authorFinishData.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf("dy:author"))

      part.foreach(line => {
        val authorId = line(0).toString
        val authorIndex = line(1).toString.toDouble
        println(authorId + "," + authorIndex)

        val put = new Put(DigestUtils.sha1Hex(authorId).getBytes)
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

    awemeFinishData.select($"awemeId".alias("aweme_id"), $"normalfeatures".alias("mm_index"))
      .write
      .format("org.elasticsearch.spark.sql")
      .options(Map("es.mapping.id" -> "aweme_id"))
      .mode(SaveMode.Append)
      .option("es.write.operation", "upsert")
      .save("awemes/origin")

    awemeFinishData.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf("dy:aweme"))

      part.foreach(line => {
        val awemeId = line(0).toString
        val awemeIdIndex = line(1).toString.toDouble
        println(awemeId + "," + awemeIdIndex)

        val put = new Put(DigestUtils.sha1Hex(awemeId).getBytes)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("mmi"), Bytes.toBytes(awemeIdIndex))

        mutator.mutate(put)
      })
      mutator.close()
    })

    //        Awememodel.select($"a".alias("aweme_id"), $"normalfeatures".alias("mm_index"))
    //          .write
    //          .format("org.elasticsearch.spark.sql")
    //          .options(Map("es.mapping.id" -> "aweme_id"))
    //          .mode(SaveMode.Append)
    //          .option("es.write.operation", "upsert")
    //          .save("awemes/origin")

  }
}
