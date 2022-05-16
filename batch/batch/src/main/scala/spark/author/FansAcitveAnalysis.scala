package spark.author

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

/**
 * 粉丝活跃时间
 */
object FansAcitveAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ActionOperation")
      .config("spark.yarn.maxAppAttempts", "1")
      .config("spark.sql.shuffle.partitions", "250")
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
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "dy:comment-transient")

    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.
      newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
        classOf[ImmutableBytesWritable],
        classOf[Result])

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val odsData: DataFrame = hbaseRDD.filter(line => {
      if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("aui")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ct")) != null) {
        true
      } else {
        false
      }
    }).mapPartitions(part => {
      val tuples: Iterator[(String, Long)] = part.map(r => {
        (
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("aui"))),
          Bytes.toLong(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ct")))
          //          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ui")))
        )
      })
      tuples
    }).toDF("aui", "ct").filter($"aui".notEqual(""))

    odsData.repartition(24).cache().show(false)

    //活跃小时
    odsData.select($"aui", from_unixtime($"ct", "HH").as("hour"))
      .groupBy("aui", "hour")
      .count()
      .select($"aui", concat_ws("|", $"hour", $"count") as ("info"))
      .groupBy("aui")
      .agg(concat_ws("||", collect_set($"info")) as ("info"))
      .foreachPartition(part => {
        val connection = ConnectionFactory.createConnection()
        val mutator = connection.getBufferedMutator(TableName.valueOf("dy:author"))
        part.foreach(line => {
          val authorId = line(0).toString
          val info = line(1).toString

          val put = new Put(DigestUtils.sha1Hex(authorId).getBytes)
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("acth"), Bytes.toBytes(info))

          mutator.mutate(put)
        })
        mutator.close()
      })

    //活跃天
    odsData.select($"aui", dayofweek(from_unixtime($"ct")).as("day"))
      .groupBy("aui", "day")
      .count()
      .select($"aui", concat_ws("|", $"day", $"count") as ("info"))
      .groupBy("aui")
      .agg(concat_ws("||", collect_set($"info")) as ("info"))
      .foreachPartition(part => {
        val connection = ConnectionFactory.createConnection()
        val mutator = connection.getBufferedMutator(TableName.valueOf("dy:author"))
        part.foreach(line => {
          val authorId = line(0).toString
          val info = line(1).toString

          val put = new Put(DigestUtils.sha1Hex(authorId).getBytes)
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("actd"), Bytes.toBytes(info))

          mutator.mutate(put)
        })
        mutator.close()
      })
  }
}
