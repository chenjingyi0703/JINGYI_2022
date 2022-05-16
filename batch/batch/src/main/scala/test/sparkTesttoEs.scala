package test

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

object sparkTesttoEs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ActionOperation")
      .config(ConfigurationOptions.ES_NODES, "es-cn-mp91asj1e0007gyzj.elasticsearch.aliyuncs.com")
      .config(ConfigurationOptions.ES_NODES_WAN_ONLY, "true")
      .config(ConfigurationOptions.ES_PORT, "9200")
      .config(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, "elastic")
      .config(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, "chanmama@123")
      .config("es.batch.size.bytes", "1mb")
      .config("es.batch.size.entries", "10000")
      .getOrCreate()


    val sc = spark.sparkContext

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.regionsizecalculator.enable", "false")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "dy:comment")

    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val hbaseDataFrame: DataFrame = hbaseRDD.map(r => (
      Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ai"))),
      Bytes.toInt(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("g"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("b")))
    )).toDF("awemeID", "gender", "birthday").repartition(24)


    hbaseDataFrame.show(false)
    println("~~~~~~~~~~~~~~~~~"+hbaseDataFrame.count())
//    hbaseDataFrame.rdd.foreachPartition(part => {
//      val connection = ConnectionFactory.createConnection()
//      val mutator = connection.getBufferedMutator(TableName.valueOf("test2"))
//      println("-------------当前时间时间：" + current_timestamp() + "------------------")
//      part.foreach(line => {
//        val arrInfo = part.next.toString().split(",")
//
//        val authorId: String = arrInfo(0).substring(1, arrInfo(0).length)
//        val gender = arrInfo(1)
//        val num = arrInfo(2).substring(0, arrInfo(2).length - 1).toLong
//        val put = new Put(DigestUtils.sha1Hex(authorId).getBytes)
//        val columnTagName = "s" + gender
//
//        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes(columnTagName), Bytes.toBytes(num))
//        mutator.mutate(put)
//
//      })
//
//      println("-------------当前结束时间：" + current_timestamp() + "------------------")
//      mutator.close()
//
//      //      while (part.hasNext) {
//      //        val arrInfo = part.next.toString().split(",")
//      //
//      //        val authorId: String = arrInfo(0).substring(1, arrInfo(0).length)
//      //        val gender = arrInfo(1)
//      //        val num = arrInfo(2).substring(0, arrInfo(2).length - 1).toLong
//      //        val put = new Put(DigestUtils.sha1Hex(authorId).getBytes)
//      //        val columnTagName = "s" + gender
//      //
//      //        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes(columnTagName), Bytes.toBytes(num))
//      //
//      //        mutator.mutate(put)
//    }
//      //      mutator.close()
//      //    }
//    )

    spark.stop()
  }
}
