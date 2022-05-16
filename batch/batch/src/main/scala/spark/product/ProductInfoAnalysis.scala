package spark.product

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

/**
 * 抖音商品画像
 */
object ProductInfoAnalysis {
  def main(args: Array[String]): Unit = {
    //读aweme表
    //取性别和年龄还有省份地市
    //性别和年龄直接sum
    //省份行专列，然后切割，然后sum 然后拼接
    //存入商品表

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

    val sc = spark.sparkContext
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.regionsizecalculator.enable", "false")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "dy:aweme")

    //    val testData ="20191015"

    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val odsAwemeData = hbaseRDD.filter(line => {
      if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("s1")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("s2")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("province")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("city")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ps")) != null
      ) {
        true
      } else {
        false
      }
    }).mapPartitions(part => {
      val tuples = part.map(line => {
        var a1: Long = 0
        if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("a1")) != null) {
          a1 = Bytes.toLong(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("a1")))
        }
        var a2: Long = 0
        if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("a2")) != null) {
          a2 = Bytes.toLong(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("a2")))
        }
        var a3: Long = 0
        if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("a3")) != null) {
          a3 = Bytes.toLong(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("a3")))
        }
        var a4: Long = 0
        if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("a4")) != null) {
          a4 = Bytes.toLong(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("a4")))
        }
        var a5: Long = 0
        if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("a5")) != null) {
          a5 = Bytes.toLong(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("a5")))
        }
        var a6: Long = 0
        if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("a6")) != null) {
          a6 = Bytes.toLong(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("a6")))
        }

        (
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ps"))), //商品id
          Bytes.toLong(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("s1"))), //男
          Bytes.toLong(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("s2"))), //女
          a1,
          a2,
          a3,
          a4,
          a5,
          a6,
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("province"))), //省份
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("city"))) //城市
        )
      })
      tuples
    }).toDF("ps", "male", "female", "a1", "a2", "a3", "a4", "a5", "a6", "city", "province")
      .filter($"ps".notEqual(""))

    val genderMap = Map("es.mapping.id" -> "promotion_id")

    val odsMapData = odsAwemeData.withColumn("ps", explode(split($"ps", ",")))

    odsMapData.cache().show(false)

    //城市
    val productCityData = odsMapData.select($"ps", $"city")
      .withColumn("city", explode(split($"city", "\\|\\|")))
      .withColumn("splitcity", split(col("city"), "\\|"))
      .select($"ps",
        col("splitcity").getItem(0).as("city"),
        col("splitcity").getItem(1).as("cityNum").cast(LongType)
      ).groupBy("ps", "city").agg(sum("cityNum").alias("citySum"))

    productCityData.show(false)

    productCityData.select($"ps", concat_ws("|", $"city", $"citySum") as ("info"))
      .groupBy("ps")
      .agg(concat_ws("||", collect_set($"info")) as ("info")).foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf("dy:product"))
      part.foreach(line => {
        val pid = line(0).toString
        val info = line(1).toString

        val put = new Put(DigestUtils.sha1Hex(pid).getBytes)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("city"), Bytes.toBytes(info))

        mutator.mutate(put)
      })
      mutator.close()
    })

    //最大城市
    val productMaxCityData = productCityData.groupBy("ps")
      .agg(max(struct("citySum", "city")) as "max")
      .select("ps", "max.*")
      .select($"ps".alias("promotion_id"), $"city".alias("city_type"))

    productMaxCityData.show(false)

    productMaxCityData.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf("dy:product"))
      val mutatorDaily = connection.getBufferedMutator(TableName.valueOf("dy:product-daily"))
      val date = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.now())
      //val date = "20191026"
      part.foreach(line => {
        val pid = line(0).toString
        val info = line(1).toString

        val put = new Put(DigestUtils.sha1Hex(pid).getBytes)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("cityM"), Bytes.toBytes(info))

        val putDaily = new Put((DigestUtils.sha1Hex(date) + pid).getBytes)
        putDaily.addColumn(Bytes.toBytes("r"), Bytes.toBytes("cityM"), Bytes.toBytes(info))

        mutator.mutate(put)
        mutatorDaily.mutate(putDaily)
      })
      mutator.close()
      mutatorDaily.close()
    })
    //      .write
    //      .format("org.elasticsearch.spark.sql")
    //      .options(genderMap)
    //      .mode(SaveMode.Append)
    //      .option("es.write.operation", "upsert")
    //      .save("products/origin")

    //省份
    val productProvinceData = odsMapData.select($"ps", $"province")
      .withColumn("province", explode(split($"province", "\\|\\|")))
      .withColumn("splitprovince", split(col("province"), "\\|"))
      .select(
        $"ps",
        col("splitprovince").getItem(0).as("province"),
        col("splitprovince").getItem(1).as("provinceNum").cast(LongType)
      ).groupBy("ps", "province").agg(sum("provinceNum").alias("provinceSum"))

    productProvinceData.show(false)

    productProvinceData.select($"ps", concat_ws("|", $"province", $"provinceSum") as ("info"))
      .groupBy("ps")
      .agg(concat_ws("||", collect_set($"info")) as ("info"))
      .foreachPartition(part => {
        val connection = ConnectionFactory.createConnection()
        val mutator = connection.getBufferedMutator(TableName.valueOf("dy:product"))
        part.foreach(line => {
          val pid = line(0).toString
          val info = line(1).toString

          val put = new Put(DigestUtils.sha1Hex(pid).getBytes)
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("prov"), Bytes.toBytes(info))

          mutator.mutate(put)
        })
        mutator.close()
      })

    //最大省份
    val productMaxProvince = productProvinceData.groupBy("ps")
      .agg(max(struct("provinceSum", "province")) as "max")
      .select("ps", "max.*")
      .select($"ps".alias("promotion_id"), $"province".alias("province_type"))

    //    productMaxProvince
    //      .write
    //      .format("org.elasticsearch.spark.sql")
    //      .options(genderMap)
    //      .mode(SaveMode.Append)
    //      .option("es.write.operation", "upsert")
    //      .save("products/origin")


    productMaxProvince.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf("dy:product"))
      val mutatorDaily = connection.getBufferedMutator(TableName.valueOf("dy:product-daily"))
      val date = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.now())
      //val date = "20191026"
      part.foreach(line => {
        val pid = line(0).toString
        val info = line(1).toString

        val put = new Put(DigestUtils.sha1Hex(pid).getBytes)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("provM"), Bytes.toBytes(info))

        val putDaily = new Put((DigestUtils.sha1Hex(date) + pid).getBytes)
        putDaily.addColumn(Bytes.toBytes("r"), Bytes.toBytes("provM"), Bytes.toBytes(info))

        mutator.mutate(put)
        mutatorDaily.mutate(putDaily)
      })
      mutator.close()
      mutatorDaily.close()
    })

    //年龄性别
    val productGenderAndAge = odsMapData.groupBy("ps")
      .agg(
        sum("male").alias("maleSum"),
        sum("female").alias("femaleSum"),
        sum("a1").alias("a1Sum"),
        sum("a2").alias("a2Sum"),
        sum("a3").alias("a3Sum"),
        sum("a4").alias("a4Sum"),
        sum("a5").alias("a5Sum"),
        sum("a6").alias("a6Sum")
      )

    productGenderAndAge.show(false)

    productGenderAndAge.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf("dy:product"))
      part.foreach(line => {
        val pid = line(0).toString
        val male = line(1).toString.toLong
        val female = line(2).toString.toLong
        val a1 = line(3).toString.toLong
        val a2 = line(4).toString.toLong
        val a3 = line(5).toString.toLong
        val a4 = line(6).toString.toLong
        val a5 = line(7).toString.toLong
        val a6 = line(8).toString.toLong

        val put = new Put(DigestUtils.sha1Hex(pid).getBytes)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("g1"), Bytes.toBytes(male))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("g2"), Bytes.toBytes(female))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a1"), Bytes.toBytes(a1))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a2"), Bytes.toBytes(a2))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a3"), Bytes.toBytes(a3))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a4"), Bytes.toBytes(a4))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a5"), Bytes.toBytes(a5))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a6"), Bytes.toBytes(a6))

        mutator.mutate(put)
      })
      mutator.close()
    })

    productGenderAndAge.createOrReplaceTempView("productGenderAndAge")

    //    spark.sql("select `ps`,stack(2, 'maleSum', `maleSum`, 'femaleSum', `femaleSum`) as (`gender`, `num` )from  productGenderAndAge")
    //      .groupBy("ps")
    //      .agg(max(struct("num", "gender")) as "max")
    //      .select("ps", "max.*")
    //      .show(false)

    //最大年龄
    val productAgeMax = spark.sql("select `ps`,stack(6, 'a1Sum', `a1Sum`, 'a2Sum', `a2Sum`, 'a3Sum', `a3Sum`, 'a4Sum', `a4Sum`, 'a5Sum', `a5Sum`, 'a6Sum', `a6Sum`) as (`age`, `num` )from  productGenderAndAge")
      .groupBy("ps")
      .agg(max(struct("num", "age")) as "max")
      .select("ps", "max.*")
      .select($"ps".alias("promotion_id"), $"age".alias("age_type"))

    //    productAgeMax
    //      .write
    //      .format("org.elasticsearch.spark.sql")
    //      .options(genderMap)
    //      .mode(SaveMode.Append)
    //      .option("es.write.operation", "upsert")
    //      .save("products/origin")

    productAgeMax.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf("dy:product"))
      val mutatorDaily = connection.getBufferedMutator(TableName.valueOf("dy:product-daily"))
      val date = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.now())
      //val date = "20191026"
      part.foreach(line => {
        val pid = line(0).toString
        val info = line(1).toString

        val put = new Put(DigestUtils.sha1Hex(pid).getBytes)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ageM"), Bytes.toBytes(info))

        val putDaily = new Put((DigestUtils.sha1Hex(date) + pid).getBytes)
        putDaily.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ageM"), Bytes.toBytes(info))

        mutator.mutate(put)
        mutatorDaily.mutate(putDaily)
      })
      mutator.close()
      mutatorDaily.close()
    })

  }
}
