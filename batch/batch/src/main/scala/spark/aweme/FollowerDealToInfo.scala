package spark.aweme

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import utils.SparkUtils

/**
 * 视频画像：省市分布，年龄分布（参数：包含热门视频）
 */
object FollowerDealToInfo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ActionOperation")
      .config("spark.yarn.maxAppAttempts", "1")
      .config("spark.sql.shuffle.partitions", "500")
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
    val genderMap = Map("es.mapping.id" -> "aweme_id")

    val arg = SparkUtils.functionArgs(args)

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.regionsizecalculator.enable", "false")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, arg)
    //    hbaseConf.set(TableInputFormat.SCAN_BATCHSIZE, "2000")


    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val hbaseDataFrame: DataFrame = hbaseRDD.filter(line => {
      if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ai")) != null) {
        true
      } else {
        false
      }
    }).mapPartitions(part => {
      val tuple = part.map(r => {
        (
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ai"))),
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ui"))),
          Bytes.toString(r._2.getRow
          )
        )
      })
      tuple
    }).toDF("aid", "fensiId", "commentId")

    val rddData: RDD[(String, String, String)] = hbaseDataFrame.rdd
      .mapPartitions(part => {
        val connection = ConnectionFactory.createConnection()
        val table = connection.getTable(TableName.valueOf("dy:comment-author"))

        val data = part.map(line => {
          val id = line(0).toString
          val fensi = line(1).toString
          val commentId = line(2).toString
          var fensiInfo = ""

          val get = new Get(Bytes.toBytes(DigestUtils.sha1Hex(fensi)))
          get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("p"))
          get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("b"))
          get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("c"))

          val result = table.get(get)
          val cellScanner = result.cellScanner //遍历单元格
          fensiInfo += fensi + ","
          while (cellScanner.advance) { //hasNext
            val cell = cellScanner.current //Next
            if (new String(CellUtil.cloneValue(cell), "utf-8").equals("")) {
              fensiInfo += "未知" + ","
            } else {
              fensiInfo += new String(CellUtil.cloneValue(cell), "utf-8") + ","
            }
          }
          fensiInfo = fensiInfo.substring(0, fensiInfo.length - 1)
          (id, fensiInfo, commentId)
        })
        data
      })

    rddData.take(100)

    val getProvinceAndAge: RDD[(String, String, String)] = rddData.filter(line => line._2.split(",").length > 1)

    val odsData: DataFrame = getProvinceAndAge.mapPartitions(part => {
      part.map(line => {
        //        println(line)
        val aid = line._1
        val uidInfo = line._2
        val commentId = line._3
        val arrInfo = uidInfo.split(",")
        //        println(arrInfo.length)
        val fensiId = arrInfo(0)
        val b = arrInfo(1)
        val p = arrInfo(2)
        val c = arrInfo(3)
        (aid, fensiId, b, p, c, commentId)
      })
    }).toDF("aweme_id", "fensiId", "b", "p", "c", "commentId")
      .filter($"aweme_id".notEqual(""))
      .repartition(24)

    odsData.cache().show(false)
    odsData.createOrReplaceTempView("tmp")


    /**
     * 城市
     */

    val cityCount = spark.sql(
      "select dim.aweme_id,dim.c as c,count(1) as num " +
        "from(select aweme_id,c from tmp where c !='未知')dim " +
        "group by dim.aweme_id,dim.c")

    cityCount.show(false)

    val cityCountMax = cityCount.groupBy("aweme_id")
      .agg(max(struct("num", "c")) as "max")
      .select("aweme_id", "max.*")

    cityCountMax.select($"aweme_id", $"c".alias("city"))
      .write
      .format("org.elasticsearch.spark.sql")
      .options(genderMap)
      .mode(SaveMode.Append)
      .option("es.write.operation", "upsert")
      .save("awemes/origin")

    val cityConcat: DataFrame = cityCount
      .select($"aweme_id", concat_ws("|", $"c", $"num") as ("info"))
      .groupBy("aweme_id")
      .agg(concat_ws("||", collect_set($"info")) as ("info"))

    cityConcat.show(false)

    cityConcat.rdd.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf("dy:aweme"))
      while (part.hasNext) {
        val arrInfo = part.next.toString().split(",")
        val authorId: String = arrInfo(0).substring(1, arrInfo(0).length)
        val city: String = arrInfo(1).substring(0, arrInfo(1).length - 1)
        val put = new Put(DigestUtils.sha1Hex(authorId).getBytes)
        val columnTagName = "city"

        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes(columnTagName), Bytes.toBytes(city))

        mutator.mutate(put)
      }
      mutator.close()
    })

    /**
     * 省份
     */
    val provinceCount: DataFrame = spark.sql(
      "select dim.aweme_id,dim.p,count(1) as num " +
        "from(select aweme_id,p from tmp where p !='未知')dim " +
        "group by dim.aweme_id,dim.p"
    )

    val provinceCountMax = provinceCount.groupBy("aweme_id")
      .agg(max(struct("num", "p")) as "max")
      .select("aweme_id", "max.*")

    provinceCountMax.select($"aweme_id", $"p".alias("province"))
      .write
      .format("org.elasticsearch.spark.sql")
      .options(genderMap)
      .mode(SaveMode.Append)
      .option("es.write.operation", "upsert")
      .save("awemes/origin")

    val provinceConcat: DataFrame = provinceCount.select($"aweme_id", concat_ws("|", $"p", $"num") as ("info"))
      .groupBy("aweme_id")
      .agg(concat_ws("||", collect_set($"info")) as ("info"))

    provinceConcat.show(false)

    provinceConcat.rdd.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf("dy:aweme"))
      while (part.hasNext) {
        val arrInfo = part.next.toString().split(",")
        val authorId: String = arrInfo(0).substring(1, arrInfo(0).length)
        val provinceCount: String = arrInfo(1).substring(0, arrInfo(1).length - 1)
        val put = new Put(DigestUtils.sha1Hex(authorId).getBytes)
        val columnTagName = "province"

        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes(columnTagName), Bytes.toBytes(provinceCount))

        mutator.mutate(put)
      }
      mutator.close()
    })

    /**
     * 年龄
     */
    val ageCount = spark.sql(
      "select dis.aweme_id,floor(datediff(current_date,dis.b) / 365) as age,count(1) as num " +
        "from(select aweme_id,b from tmp where b != '未知') as dis " +
        "group by dis.aweme_id, dis.b"
    )

    ageCount.show(false)

    val ageTag: DataFrame = ageCount.withColumn("ageTag",
      when($"age" >= 6 && $"age" < 18, 1)
        .when($"age" >= 18 && $"age" < 25, 2)
        .when($"age" >= 25 && $"age" < 31, 3)
        .when($"age" >= 31 && $"age" < 36, 4)
        .when($"age" >= 36 && $"age" < 41, 5)
        .when($"age" >= 41, 6)
    )

    val ageMax = ageTag.groupBy("aweme_id")
      .agg(max(struct("num", "ageTag")) as "max")
      .select("aweme_id", "max.*")

    ageMax.show()


    ageMax.select($"aweme_id", $"ageTag".alias("age_type"))
      .write
      .format("org.elasticsearch.spark.sql")
      .options(genderMap)
      .mode(SaveMode.Append)
      .option("es.write.operation", "upsert")
      .save("awemes/origin")

    ageTag.rdd.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf("dy:aweme"))
      while (part.hasNext) {
        val arrInfo = part.next.toString().split(",")
        val authorId: String = arrInfo(0).substring(1, arrInfo(0).length)
        val ageCount = arrInfo(2).toLong
        val ageTag = arrInfo(3)
        val put = new Put(DigestUtils.sha1Hex(authorId).getBytes)
        val columnTagName = "a" + ageTag.substring(0, ageTag.length - 1)

        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes(columnTagName), Bytes.toBytes(ageCount))

        mutator.mutate(put)
      }
      mutator.close()
    })
    spark.stop()
  }
}
