package spark.aweme

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import utils.SparkUtils

/**
 * 视频画像：性别分布（参数：包含热门视频）
 */
object VideoInfoAnalysis {
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

    val arg = SparkUtils.functionArgs(args)

    val sc = spark.sparkContext

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.regionsizecalculator.enable", "false")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, arg)

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
    }).map(r => {
      var g: Int = 0
      if (r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("g")) != null) {
        g = Bytes.toInt(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("g")))
      }
      (
        Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ai"))),
        g,
        Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("b")))
      )
    }
    ).toDF("aweme_id", "gender", "birthday")
      .filter($"aweme_id".notEqual(""))
      .repartition(24)

    hbaseDataFrame.cache().show(false)
    hbaseDataFrame.createOrReplaceTempView("tmp")

    //性别分布
    val genderCount = spark.sql(
      "select aweme_id,gender,count(*) as num " +
        "from tmp where gender='1' or gender='0' or gender = '2' " +
        "group by aweme_id, gender "
    )
    genderCount.show()

    //最大性别
    val genderMax = genderCount.groupBy("aweme_id")
      .agg(max(struct("num", "gender")) as "max")
      .select("aweme_id", "max.*")

    genderMax.show()

    /**
     * 存入es
     */
    val genderMap = Map("es.mapping.id" -> "aweme_id")
    genderMax.select($"aweme_id", $"gender".alias("gender_type"))
      .write
      .format("org.elasticsearch.spark.sql")
      .options(genderMap)
      .mode(SaveMode.Append)
      .option("es.write.operation", "upsert")
      .save("awemes/origin")

    /**
     * 存入hbase
     */

    genderCount.rdd.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      val mutator = connection.getBufferedMutator(TableName.valueOf("dy:aweme"))
      while (part.hasNext) {
        val arrInfo = part.next.toString().split(",")

        val authorId: String = arrInfo(0).substring(1, arrInfo(0).length)
        val gender = arrInfo(1)
        val num = arrInfo(2).substring(0, arrInfo(2).length - 1).toLong
        val put = new Put(DigestUtils.sha1Hex(authorId).getBytes)
        val columnTagName = "s" + gender

        put.addColumn(Bytes.toBytes("r"), Bytes.toBytes(columnTagName), Bytes.toBytes(num))

        mutator.mutate(put)
      }
      mutator.close()
    })

    spark.stop()
    //    spark.sql("SELECT from_unixtime(0, 'yyyy-MM-dd HH:mm:ss')").show()ld-uf6w1y03mb950v52e-proxy-hbaseue.hbaseue.rds.aliyuncs.com:30020
    //    spark.sql("select if(datediff(CURRENT_DATE,CONCAT(substr(CURRENT_DATE,0,4),substr('1993-09-12',5,7)))>=0,(substr(CURRENT_DATE,0,4) - substr('1993-09-12',0,4)),(substr(CURRENT_DATE,0,4) - substr('1993-09-12',0,4)-1))").show(false)
  }
}
