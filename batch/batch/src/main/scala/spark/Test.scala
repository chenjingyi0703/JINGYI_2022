package spark

import java.time.{LocalDate, LocalDateTime}

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ActionOperation")
      .config("spark.yarn.maxAppAttempts", "1")
      .master("local")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    val scan = new Scan()
    scan.setRowPrefixFilter(DigestUtils.sha1Hex("20190919").getBytes())
    val scanString = TableMapReduceUtil.convertScanToString(scan)

    //    val job = Job.getInstance()
    //    TableMapReduceUtil.initTableMapperJob("dy:author-daily", scan, classOf[IdentityTableMapper], null, null, job)
    //    val jobConf = new JobConf(job.getConfiguration)
    //    SparkHadoopUtil.get.addCredentials(jobConf)

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.regionsizecalculator.enable", "false") //aliyun hbase 不加有问题
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "dy:author-daily") /**/
    hbaseConf.set(TableInputFormat.SCAN, scanString)

    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.
      newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
        classOf[ImmutableBytesWritable],
        classOf[Result])

    val odsData: DataFrame = hbaseRDD.mapPartitions(part => {
      val tuples: Iterator[(Long, Long, Long, Long, Long)] = part.map(r => {
        (
          Bytes.toLong(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ac"))), //视频数
          Bytes.toLong(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("dc"))),
          Bytes.toLong(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("fc"))), //点赞数
          Bytes.toLong(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("fic"))), //关注总数
          Bytes.toLong(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("frc"))) //粉丝总数
        )
      })
      tuples
    }).toDF("ac", "dc", "fc", "fic", "frc")
    odsData.createOrReplaceTempView("tmp")

    spark.sql("select * from tmp order by frc desc").show(false)
  }
}
