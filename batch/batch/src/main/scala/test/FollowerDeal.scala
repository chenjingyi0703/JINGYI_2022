package test

import java.util

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object FollowerDeal {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ActionOperation")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val hbaseConf = HBaseConfiguration.create()
    //    val scan = new Scan()
    //    scan.setLimit(2000)
    //    val proto = ProtobufUtil.toScan(scan)
    //    val scanToString = Base64.encodeBytes(proto.toByteArray())
    hbaseConf.set("hbase.regionsizecalculator.enable", "false")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "dy:comment")
    //    hbaseConf.set(TableInputFormat.SCAN_BATCHSIZE, "2000")
    //    hbaseConf.set(TableInputFormat.SCAN,scan)
    //
    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val hbaseData = hbaseRDD.map(r => (
      Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("aui"))),
      Bytes.toLong(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ct"))),
      //      Bytes.toString(r._2.getRow),
      Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ui")))
    )).toDF("aui", "ct", "ui").where($"aui".isNotNull)

    hbaseData.select($"aui", from_unixtime($"ct", "HH").as("hour"), $"ui")
      .groupBy("aui", "hour")
      .count()
      .show(false)

    hbaseData.select($"aui", dayofweek(from_unixtime($"ct")).as("day"))
      .groupBy("aui", "day")
      .count()
      .show(false)
    //      .agg()


    //      .foreach(println)


    //    hbaseData.foreachPartition(part => {
    //      val connection = ConnectionFactory.createConnection()
    //      val table = connection.getTable(TableName.valueOf("dy:comment-author"))
    //      part.foreach(line => {
    //        val aid = line._1
    //        val uidList = line._2.toList
    //        var fensiInfo = ""
    //        val gets = new util.ArrayList[Get]()
    //
    //        println(uidList)
    //
    //        for (uid <- uidList) {
    //          val get = new Get(Bytes.toBytes(DigestUtils.sha1Hex(uid)))
    //          get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("p"))
    //          get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("b"))
    //          get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("c"))
    //          gets.add(get)
    //        }
    //
    //        val results: Array[Result] = table.get(gets)
    //
    //        for (result <- results) {
    //          val uid = result.getRow()
    //          val p = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("p"))
    //          val b = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("b"))
    //          val c = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("c"))
    //
    //          val info = Bytes.toString(uid) + "," + Bytes.toString(p) + "," + Bytes.toString(b) + "," + Bytes.toString(c) + "||"
    //          fensiInfo += info
    //          println(info)
    //        }
    //        (aid, fensiInfo)
    //      })
    //    })
    //
    //    rddData.foreach(println)
    //
    //    val resultData: RDD[(String, String)] = rddData.map(v => (v._1, v._2.split(",\\|\\|")))
    //      .map(v => {
    //        v._2.map(x => (v._1, x))
    //      })
    //      .flatMap(x => x)
    //      .filter(_._2.split(",").length > 2)

    //    val getProvinceAndAge: RDD[(String, String)] = rddData.filter(line => line._2.split(",").length > 1)
    //    //    getProvinceAndAge.foreach(println)
    //    val odsData: DataFrame = getProvinceAndAge.mapPartitions(part => {
    //      part.map(line => {
    //        //        println(line)
    //        val aid = line._1
    //        val uidInfo = line._2
    //        val arrInfo = uidInfo.split(",")
    //        //        println(arrInfo.length)
    //        val fensiId = arrInfo(0)
    //        val b = arrInfo(1)
    //        val p = arrInfo(2)
    //        val c = arrInfo(3)
    //        (aid, fensiId, b, p, c)
    //      })
    //    }).toDF("aweme_id", "fensiId", "b", "p", "c").repartition(24)
    //
    //    odsData.createOrReplaceTempView("tmp")
    //
    //    val ageCount = spark.sql("select aweme_id,floor(datediff(current_date,tmp.b) / 365) as age,count(1) as num from tmp group by aweme_id,b")
    //      .where($"age".isNotNull)
    //
    //    val ageTag: DataFrame = ageCount.withColumn("ageTag",
    //      when($"age" >= 6 && $"age" < 18, 1)
    //        .when($"age" >= 18 && $"age" < 25, 2)
    //        .when($"age" >= 25 && $"age" < 31, 3)
    //        .when($"age" >= 31 && $"age" < 36, 4)
    //        .when($"age" >= 36 && $"age" < 41, 5)
    //        .when($"age" >= 41, 6)
    //    )
    //
    //    val ageMax = ageTag.groupBy("aweme_id")
    //      .agg(max(struct("num", "ageTag")) as "max")
    //      .select("aweme_id", "max.*")
    //
    //    ageMax.show()
    //
    //    val genderMap = Map("es.mapping.id" -> "aweme_id")
    //
    //    ageMax.select($"aweme_id", $"ageTag".alias("age_type"))
    //      .write
    //      .format("org.elasticsearch.spark.sql")
    //      .options(genderMap)
    //      .mode(SaveMode.Append)
    //      .option("es.write.operation", "upsert")
    //      .save("awemes/origin")
    //
    //    ageTag.rdd.foreachPartition(part => {
    //      val connection = ConnectionFactory.createConnection()
    //      val mutator = connection.getBufferedMutator(TableName.valueOf("dy:aweme"))
    //      while (part.hasNext) {
    //        val arrInfo = part.next.toString().split(",")
    //        val authorId: String = arrInfo(0).substring(1, arrInfo(0).length)
    //        val ageCount = arrInfo(2).toLong
    //        val ageTag = arrInfo(3)
    //        val put = new Put(DigestUtils.sha1Hex(authorId).getBytes)
    //        val columnTagName = "a" + ageTag.substring(0, ageTag.length - 1)
    //
    //        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes(columnTagName), Bytes.toBytes(ageCount))
    //
    //        mutator.mutate(put)
    //      }
    //      mutator.close()
    //    })


    //    val resultData: RDD[(String, String)] = rddData.map(v => (v._1, v._2.split("\\|\\|")))
    //      .map(v => {
    //        v._2.map(x => (v._1, x))
    //      })
    //      .flatMap(x => x)
    //      .filter(_._2.split(",").length > 2)

  }
}
