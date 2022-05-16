package spark.author.grow.hbase

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author LJK
 * @date 2019/10/29 5:07 下午
 */
object HBaseAuthorGrowHelper {

  /**
   * dy:author-daily获取一天数据
   *
   * @param day
   * @param ss
   * @return
   */
  def getOneDayOfAuthor(day: String, ss: SparkSession): DataFrame = {
    val scan = new Scan()
    scan.setRowPrefixFilter(DigestUtils.sha1Hex(day).getBytes())
    val scanString = TableMapReduceUtil.convertScanToString(scan)
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.regionsizecalculator.enable", "false") //aliyun hbase 不加有问题
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "dy:author-daily")
    hbaseConf.set(TableInputFormat.SCAN, scanString)

    val hbaseRDD = ss.sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    import ss.implicits._
    hbaseRDD.filter(line => {
      if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("fc")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("frc")) != null) {
        true
      } else {
        false
      }
    }).mapPartitions(part => {
      val tuples: Iterator[(String, String, String, Long, Long, String)] = part.map(r => {
        (
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ai"))), //博主ID
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("nn"))), // 昵称
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("a"))), // 头像
          Bytes.toLong(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("fc"))), //总点赞数
          Bytes.toLong(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("frc"))), // 总粉丝数
          Bytes.toString(r._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("l"))) // 标签
        )
      })
      tuples
    }).toDF("ai", "nn", "a", "fc", "frc", "l")
      .filter($"ai".notEqual("") && $"l".notEqual(""))
  }
}
