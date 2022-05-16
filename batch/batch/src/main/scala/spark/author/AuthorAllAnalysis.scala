package spark.author

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import utils.SparkUtils

/**
 * 用户画像
 */
object AuthorAllAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ActionOperation")
      .config("spark.yarn.maxAppAttempts", "1")
      .config("spark.sql.shuffle.partitions", "500")
      .config(ConfigurationOptions.ES_NODES, "es-cn-mp91asj1e0007gyzj.elasticsearch.aliyuncs.com")
      .config(ConfigurationOptions.ES_NODES_WAN_ONLY, "true")
      .config(ConfigurationOptions.ES_PORT, "9200")
      .config(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, "elastic")
      .config(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, "chanmama@123")
      .config("es.batch.size.bytes", "1mb")
      .config("es.batch.size.entries", "10000")
      //      .master("local")
      .getOrCreate()

    val sc = spark.sparkContext

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.regionsizecalculator.enable", "false") //aliyun hbase 不加有问题
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "dy:follower-transient")

    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.
      newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
        classOf[ImmutableBytesWritable],
        classOf[Result])

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val odsData: DataFrame = hbaseRDD.mapPartitions(part => {
      val tuples: Iterator[(String, String)] = part.map(r => {
        (
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ai"))), //播主
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("fi"))) //播主粉丝
        )
      })
      tuples
    }).toDF("author_id", "follower_id").repartition(24)

    //原始数据
    odsData.show(false)

    //连表
    val AuthorIdAndFollowerDataFrame: DataFrame = odsData.mapPartitions(part => {
      val connection = ConnectionFactory.createConnection()
      val table = connection.getTable(TableName.valueOf("dy:follower-author"))
      val AuthorIdAndFollowerInfo = part.map(line => {
        val authorId = line(0).toString
        val followerId = line(1).toString

        val get = new Get(Bytes.toBytes(DigestUtils.sha1Hex(followerId)))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ct"))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("p"))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("b"))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("c"))
        get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("g"))

        val result = table.get(get)

        val country = Bytes.toString(result.getValue(Bytes.toBytes("r"), Bytes.toBytes("ct")))
        val province = Bytes.toString(result.getValue(Bytes.toBytes("r"), Bytes.toBytes("p")))
        val city = Bytes.toString(result.getValue(Bytes.toBytes("r"), Bytes.toBytes("c")))
        val birthday = Bytes.toString(result.getValue(Bytes.toBytes("r"), Bytes.toBytes("b")))
        var gender = 4
        if (result.getValue(Bytes.toBytes("r"), Bytes.toBytes("g")) != null) {
          gender = Bytes.toInt(result.getValue(Bytes.toBytes("r"), Bytes.toBytes("g")))
        }

        (authorId, followerId, country, province, city, birthday, gender)
      })
      table.close()
      AuthorIdAndFollowerInfo
    }).toDF("authorId", "followerId", "country", "province", "city", "birthday", "gender").filter($"authorId".notEqual(""))

    AuthorIdAndFollowerDataFrame.cache().show()
    /*
    AuthorIdAndFollowerDataFrame
      .cache()
      .show(false)
    */
    val genderMap = Map("es.mapping.id" -> "author_id")

    val followerInChina = AuthorIdAndFollowerDataFrame.filter($"country".equalTo("中国"))
    followerInChina.cache().show()

    //省份
    //地域分布
    val followerProvinceCount: DataFrame = followerInChina.filter($"province".notEqual("") && ($"province".isNotNull))
      .groupBy("authorId", "province")
      .count()

    //地域分布拼接入库
    followerProvinceCount
      .select($"authorId", concat_ws("|", $"province", $"count") as ("info"))
      .groupBy("authorId")
      .agg(concat_ws("||", collect_set($"info")) as ("info"))
      .foreachPartition(part => {
        val connection = ConnectionFactory.createConnection()
        val mutator = connection.getBufferedMutator(TableName.valueOf("dy:author"))
        part.foreach(line => {
          val authorId = line(0).toString
          val info = line(1).toString

          val put = new Put(DigestUtils.sha1Hex(authorId).getBytes)
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("prov"), Bytes.toBytes(info))

          mutator.mutate(put)
        })
        mutator.close()
      })

    //地域最大分布
    val followerProvinceMax = followerProvinceCount.groupBy("authorId")
      .agg(max(struct("count", "province")) as "max")
      .select("authorId", "max.*")

    followerProvinceMax.show(false)

    //最大分布入es
    followerProvinceMax.select($"authorId".alias("author_id"), $"province".alias("province_type"))
      .write
      .format("org.elasticsearch.spark.sql")
      .options(genderMap)
      .mode(SaveMode.Append)
      .option("es.write.operation", "upsert")
      .save("authors/origin")


    //城市
    //城市分布
    val followerCityCount: DataFrame = followerInChina.filter($"city".notEqual("") && ($"city".isNotNull))
      .groupBy("authorId", "city")
      .count()

    //城市分布最大
    val followerCityMax: DataFrame = followerCityCount.groupBy("authorId")
      .agg(max(struct("count", "city")) as "max")
      .select("authorId", "max.*")

    followerCityMax.show(false)

    //最大入es
    followerCityMax.select($"authorId".alias("author_id"), $"city".alias("city_type"))
      .write
      .format("org.elasticsearch.spark.sql")
      .options(genderMap)
      .mode(SaveMode.Append)
      .option("es.write.operation", "upsert")
      .save("authors/origin")

    //地域分布拼接入库
    followerCityCount
      .select($"authorId", concat_ws("|", $"city", $"count") as ("info"))
      .groupBy("authorId")
      .agg(concat_ws("||", collect_set($"info")) as ("info"))
      .foreachPartition(part => {
        val connection = ConnectionFactory.createConnection()
        val mutator = connection.getBufferedMutator(TableName.valueOf("dy:author"))
        part.foreach(line => {
          val authorId = line(0).toString
          val info = line(1).toString

          val put = new Put(DigestUtils.sha1Hex(authorId).getBytes)
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("city"), Bytes.toBytes(info))

          mutator.mutate(put)
        })
        mutator.close()
      })

    //过滤生日
    val authorWithBirthday = AuthorIdAndFollowerDataFrame.filter($"birthday".notEqual("")
      && ($"birthday".isNotNull) && ($"birthday".notEqual("1993-01-01")))

    authorWithBirthday.cache().show(500)


    //星座
    //注册udf
    val date2Constellation = spark.udf.register("date2Constellation", SparkUtils.date2Constellation(_: String))

    //星座分布
    val authorBirthAnal = authorWithBirthday
      .select($"authorId", date2Constellation($"birthday").as("constellation"))
      .groupBy("authorId", "constellation")
      .count()

    authorBirthAnal.show(false)

    //星座最大
    val authorBirthMax = authorBirthAnal.groupBy("authorId")
      .agg(max(struct("count", "constellation")) as "max")
      .select("authorId", "max.*")

    authorBirthMax.show(false)

    //入es
    authorBirthMax.select($"authorId".alias("author_id"), $"constellation".alias("constellation_type"))
      .write
      .format("org.elasticsearch.spark.sql")
      .options(genderMap)
      .mode(SaveMode.Append)
      .option("es.write.operation", "upsert")
      .save("authors/origin")

    //星座拼接入es
    authorBirthAnal
      .select($"authorId", concat_ws("|", $"constellation", $"count") as ("info"))
      .groupBy("authorId")
      .agg(concat_ws("||", collect_set($"info")) as ("info"))
      .foreachPartition(part => {
        val connection = ConnectionFactory.createConnection()
        val mutator = connection.getBufferedMutator(TableName.valueOf("dy:author"))
        part.foreach(line => {
          val authorId = line(0).toString
          val info = line(1).toString

          val put = new Put(DigestUtils.sha1Hex(authorId).getBytes)
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("cons"), Bytes.toBytes(info))

          mutator.mutate(put)
        })
        mutator.close()
      })

    //性别
    val authorCount = AuthorIdAndFollowerDataFrame.filter($"gender".notEqual(4))
      .groupBy("authorId", "gender")
      .count()

    //最大性别
    val authorCountMax = authorCount.groupBy("authorId")
      .agg(max(struct("count", "gender")) as "max")
      .select("authorId", "max.*")

    authorCountMax.show(false)
    authorCountMax.select($"authorId".alias("author_id"), $"gender".alias("gender_type"))
      .write
      .format("org.elasticsearch.spark.sql")
      .options(genderMap)
      .mode(SaveMode.Append)
      .option("es.write.operation", "upsert")
      .save("authors/origin")

    authorCount
      .foreachPartition(part => {
        val connection = ConnectionFactory.createConnection()
        val mutator = connection.getBufferedMutator(TableName.valueOf("dy:author"))
        val mutatorSnapShot = connection.getBufferedMutator(TableName.valueOf("dy:author-snapshot"))
        val date = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.now())

        part.foreach(line => {
          val authorId = line(0).toString
          val gender = line(1).toString
          val count = line(2).toString.toLong

          val put = new Put(DigestUtils.sha1Hex(authorId).getBytes)
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("g" + gender), Bytes.toBytes(count))

          val putSnapShot = new Put((DigestUtils.sha1Hex(authorId) + date).getBytes)
          putSnapShot.addColumn(Bytes.toBytes("r"), Bytes.toBytes("g" + gender), Bytes.toBytes(count))

          mutator.mutate(put)
          mutatorSnapShot.mutate(putSnapShot)
        })
        mutator.close()
        mutatorSnapShot.close()
      })

    //年龄
    val authorAgeAnal: DataFrame = authorWithBirthday.select($"authorId", floor(datediff(current_date(), $"birthday") / 365).as("age"))
      .withColumn("ageTag", when($"age" >= 6 && $"age" < 18, 1)
        .when($"age" >= 18 && $"age" < 25, 2)
        .when($"age" >= 25 && $"age" < 31, 3)
        .when($"age" >= 31 && $"age" < 36, 4)
        .when($"age" >= 36 && $"age" < 41, 5)
        .when($"age" >= 41, 6))
      .groupBy("authorId", "ageTag")
      .count()
      .filter($"ageTag".isNotNull)

    authorAgeAnal.show(100, false)

    val authorAgeMax = authorAgeAnal.groupBy("authorId")
      .agg(max(struct("count", "ageTag")) as "max")
      .select("authorId", "max.*")

    authorAgeMax.show(100, false)

    authorAgeMax.select($"authorId".alias("author_id"), $"ageTag".alias("age_type"))
      .write
      .format("org.elasticsearch.spark.sql")
      .options(genderMap)
      .mode(SaveMode.Append)
      .option("es.write.operation", "upsert")
      .save("authors/origin")

    authorAgeAnal
      .select($"authorId", concat_ws("|", $"ageTag", $"count") as ("info"))
      .groupBy("authorId")
      .agg(concat_ws("||", collect_set($"info")) as ("info"))
      .foreachPartition(part => {
        val connection = ConnectionFactory.createConnection()
        val mutator = connection.getBufferedMutator(TableName.valueOf("dy:author"))
        val mutatorSnapShot = connection.getBufferedMutator(TableName.valueOf("dy:author-snapshot"))
        val date = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.now())

        part.foreach(line => {
          val authorId = line(0).toString
          val info = line(1).toString

          println(authorId + "," + info)
          val put = new Put(DigestUtils.sha1Hex(authorId).getBytes)
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ageT"), Bytes.toBytes(info))

          val putSnapShot = new Put((DigestUtils.sha1Hex(authorId) + date).getBytes)
          putSnapShot.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ageT"), Bytes.toBytes(info))

          mutator.mutate(put)
          mutatorSnapShot.mutate(putSnapShot)
        })
        mutator.close()
        mutatorSnapShot.close()
      })
  }
}
