package spark.author

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

/**
 * 用户的平均点赞，平均评论，平均分享
 */
object AuthorInfoAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ActionOperation")
      .config("spark.yarn.maxAppAttempts", "1")
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
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "dy:aweme")

    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.
      newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
        classOf[ImmutableBytesWritable],
        classOf[Result])

    import spark.implicits._

    val hbaseDataFrame: DataFrame = hbaseRDD.filter(line => {
      if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("cc")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("sc")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("dc")) != null) {
        true
      } else {
        false
      }
    }).map(r => (
      Bytes.toString(r._2.getRow),
      Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("a"))),
      Bytes.toLong(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("cc"))),
      Bytes.toLong(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("sc"))),
      Bytes.toLong(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("dc")))
    )).toDF("awemeId", "authorId", "comments", "shares", "diggs")

    hbaseDataFrame.createOrReplaceTempView("tmp")
    hbaseDataFrame.show(100, false)

    val dataGrouped: DataFrame = spark.sql(
      "select authorId,commentsTotal,sharesTotal,diggsTotal,cast(diggsTotal/awemeCount as long) as diggsAvg ,cast(commentsTotal / awemeCount as long) as commentsAvg, cast(sharesTotal/ awemeCount as long) as sharesAvg " +
        "from(select authorId,sum(comments)as commentsTotal,sum(shares)as sharesTotal,sum(diggs)as diggsTotal,count(awemeId)as awemeCount " +
        "from tmp " +
        "group by tmp.authorId)"
    ).filter($"authorId".notEqual(""))

    dataGrouped.show(100, false)

    //    val p = dataGrouped.selectExpr("authorId", "cast(diggsAvg as long) diggsAvg", "cast(commentsAvg as long) commentsAvg", "cast(sharesAvg as long) sharesAvg")

    val genderMap = Map("es.mapping.id" -> "author_id")


    dataGrouped.select($"authorId".as("author_id"), $"commentsTotal".as("total_comment"), $"sharesTotal".as("total_share"))
      .write
      .format("org.elasticsearch.spark.sql")
      .options(genderMap)
      .mode(SaveMode.Append)
      .option("es.write.operation", "upsert")
      .save("authors/origin")

    dataGrouped.rdd.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf("dy:author"))
      val mutatorSnapShot = connection.getBufferedMutator(TableName.valueOf("dy:author-snapshot"))
      val date = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.now())

      while (part.hasNext) {
        val arrInfo = part.next.toString().split(",")
        val authorId: String = arrInfo(0).substring(1, arrInfo(0).length)
        val commentsTotal = arrInfo(1).toLong
        val sharesTotal = arrInfo(2).toLong
        val diggsTotal = arrInfo(3).toLong
        val diggsAvg = arrInfo(4).toLong
        val commentsAvg = arrInfo(5).toLong
        val sharesAvg = arrInfo(6).substring(0, arrInfo(6).length - 1).toLong

        val put = new Put(DigestUtils.sha1Hex(authorId).getBytes)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("da"), Bytes.toBytes(diggsAvg))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ca"), Bytes.toBytes(commentsAvg))
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("sa"), Bytes.toBytes(sharesAvg))
        mutator.mutate(put)

        val putSnapShot = new Put((DigestUtils.sha1Hex(authorId) + date).getBytes)
        putSnapShot.addColumn(Bytes.toBytes("r"), Bytes.toBytes("cmt"), Bytes.toBytes(commentsTotal))
        putSnapShot.addColumn(Bytes.toBytes("r"), Bytes.toBytes("srt"), Bytes.toBytes(sharesTotal))
        putSnapShot.addColumn(Bytes.toBytes("r"), Bytes.toBytes("dgt"), Bytes.toBytes(diggsTotal))
        putSnapShot.addColumn(Bytes.toBytes("r"), Bytes.toBytes("da"), Bytes.toBytes(diggsAvg))
        putSnapShot.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ca"), Bytes.toBytes(commentsAvg))
        putSnapShot.addColumn(Bytes.toBytes("r"), Bytes.toBytes("sa"), Bytes.toBytes(sharesAvg))
        mutatorSnapShot.mutate(putSnapShot)
      }
      mutator.close()
      mutatorSnapShot.close()
    })

    spark.stop()
  }
}
