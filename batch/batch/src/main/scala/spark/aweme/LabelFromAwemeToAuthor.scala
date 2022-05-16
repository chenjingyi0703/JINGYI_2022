package spark.aweme

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

/**
 * 行业标签，博主分类
 */
object LabelFromAwemeToAuthor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ActionOperation")
      .config("spark.yarn.maxAppAttempts", "1")
      //      .master("local")
      .config(ConfigurationOptions.ES_NODES, "es-cn-mp91asj1e0007gyzj.elasticsearch.aliyuncs.com")
      .config(ConfigurationOptions.ES_NODES_WAN_ONLY, "true")
      .config(ConfigurationOptions.ES_PORT, "9200")
      .config(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, "elastic")
      .config(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, "chanmama@123")
      .config("es.batch.size.bytes", "1mb")
      .config("es.batch.size.entries", "10000")
      .getOrCreate()

    val sc = spark.sparkContext
    import org.apache.spark.sql.functions._
    import spark.implicits._

//    val testDate = "20191023"


    /**
     * 扫描视频表
     */
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.regionsizecalculator.enable", "false") //aliyun hbase 不加有问题
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "dy:aweme")

    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.
      newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
        classOf[ImmutableBytesWritable],
        classOf[Result])

    val odsData: DataFrame = hbaseRDD.mapPartitions(part => {
      val tuples: Iterator[(String, String)] = part.map(r => {
        (
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("a"))), //博主id
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("l"))) //标签
        )
      })
      tuples
    }).toDF("author_id", "label")
      .filter($"label".isNotNull && $"author_id".notEqual(""))

    odsData.createOrReplaceTempView("tmp")

    /**
     * 扫描博主表
     */
    val hbaseConf2 = HBaseConfiguration.create()
    hbaseConf2.set("hbase.regionsizecalculator.enable", "false") //aliyun hbase 不加有问题
    hbaseConf2.set(TableInputFormat.INPUT_TABLE, "dy:author")

    val hbaseRDD2: RDD[(ImmutableBytesWritable, Result)] = sc.
      newAPIHadoopRDD(hbaseConf2, classOf[TableInputFormat],
        classOf[ImmutableBytesWritable],
        classOf[Result])

    val odsAuthorData: DataFrame = hbaseRDD2.mapPartitions(part => {
      val tuples = part.map(r => {
        (
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ai"))), //博主id
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ml"))), //手动标签
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("l1"))), //抖音明星榜
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("l2"))), //抖音官方认证
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("l3"))), //昵称
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("l4"))) //标签
        )
      })
      tuples
    }).toDF("author_id", "ml", "l1", "l2", "l3", "l4")
      .filter($"author_id".notEqual(""))

    odsAuthorData.show(false)

    //分组count
    val labelMax: DataFrame = spark.sql("select author_id,label,count(1)as num from tmp group by author_id,label")
    labelMax.show(false)

    //    val _label: UserDefinedFunction = udf {
    //      (author: String, label: String) =>
    //        val authorTable = ConnectionFactory.createConnection().getTable(TableName.valueOf("dy:author"))
    //        val get = new Get(DigestUtils.sha1Hex(author).getBytes)
    //        get.addColumn("r".getBytes, "ml".getBytes)
    //        get.addColumn("r".getBytes, "l1".getBytes)
    //        get.addColumn("r".getBytes, "l2".getBytes)
    //        get.addColumn("r".getBytes, "l3".getBytes)
    //        get.addColumn("r".getBytes, "l4".getBytes)
    //        val result = authorTable.get(get)
    //        val aml = Bytes.toString(result.getValue("r".getBytes, "ml".getBytes))
    //        val al1 = Bytes.toString(result.getValue("r".getBytes, "l1".getBytes))
    //        val al2 = Bytes.toString(result.getValue("r".getBytes, "l2".getBytes))
    //        val al3 = Bytes.toString(result.getValue("r".getBytes, "l3".getBytes))
    //        val al4 = Bytes.toString(result.getValue("r".getBytes, "l4".getBytes))
    //        authorTable.close()
    //        var l = al4
    //        if (StringUtils.isNotBlank(al3)) l = al3
    //        if (StringUtils.isNotBlank(al2)) l = al2
    //        if (StringUtils.isNotBlank(al1)) l = al1
    //        if (StringUtils.isNotBlank(aml)) l = aml
    //        if (StringUtils.isBlank(l)) l = label
    //        l
    //    }
    //取最大
    val labelFinishData = labelMax.groupBy("author_id")
      .agg(max(struct("num", "label")) as "max")
      .select("author_id", "max.*")
    //      .withColumn("label", _label($"author_id", $"label"))

    labelFinishData.show(false)

    val odsAwemeAndAuthor = labelFinishData.join(odsAuthorData, labelFinishData("author_id") === odsAuthorData("author_id"))
      .select(labelFinishData("author_id"), labelFinishData("label"), odsAuthorData("ml"), odsAuthorData("l1"), odsAuthorData("l2"), odsAuthorData("l3"), odsAuthorData("l4"))

    odsAwemeAndAuthor.show(false)

    val odsMapData = odsAwemeAndAuthor.mapPartitions(part => {
      val tuples = part.map(line => {
        val authorId = line(0).toString
        val lable = line(1).toString

        val ml = if (line(2) == null) "" else line(2).toString
        val l1 = if (line(3) == null) "" else line(3).toString
        val l2 = if (line(4) == null) "" else line(4).toString
        val l3 = if (line(5) == null) "" else line(5).toString
        val l4 = if (line(6) == null) "" else line(6).toString

        var l = lable
        if (StringUtils.isNotBlank(l4)) l = l4
        if (StringUtils.isNotBlank(l3)) l = l3
        if (StringUtils.isNotBlank(l2)) l = l2
        if (StringUtils.isNotBlank(l1)) l = l1
        if (StringUtils.isNotBlank(ml)) l = ml

        println(authorId + "," + l)

        (authorId, l)
      })
      tuples
    }).toDF("author_id", "label")

    odsMapData.show(false)

    odsMapData.rdd.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf("dy:author-daily"))
      val mutatorAuthor = connection.getBufferedMutator(TableName.valueOf("dy:author"))

      val date = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.now())

      while (part.hasNext) {
        val arrInfo = part.next.toString().split(",")
        val authorId: String = arrInfo(0).substring(1, arrInfo(0).length)
        val lable = arrInfo(1).substring(0, arrInfo(1).length - 1)

        println(date + "," + authorId + "," + lable)

        val put = new Put((DigestUtils.sha1Hex(date) + authorId).getBytes)
        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("l"), Bytes.toBytes(lable))
        mutator.mutate(put)

        val putAuthor = new Put((DigestUtils.sha1Hex(authorId)).getBytes)
        putAuthor.addColumn(Bytes.toBytes("r"), Bytes.toBytes("l"), Bytes.toBytes(lable))
        mutatorAuthor.mutate(putAuthor)
      }
      mutator.close()
      mutatorAuthor.close()
    })


    odsMapData.where($"author_id".isNotNull).select($"author_id", $"label")
      .write
      .format("org.elasticsearch.spark.sql")
      .options(Map("es.mapping.id" -> "author_id"))
      .mode(SaveMode.Append)
      .option("es.write.operation", "upsert")
      .save("authors/origin")

    spark.stop()
  }
}

