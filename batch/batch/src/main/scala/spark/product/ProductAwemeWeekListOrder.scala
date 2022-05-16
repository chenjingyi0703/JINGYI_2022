package spark.product

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import utils.SparkUtils

/**
 * 抖音电商视频榜周榜
 */
//product-aweme-week-rank
object ProductAwemeWeekListOrder {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ActionOperation")
      .config("spark.yarn.maxAppAttempts", "1")
      //      .master("local[*]")
      .getOrCreate()


    val sc = spark.sparkContext
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val date = SparkUtils.judgeDurationTime(1)
    // todo
    val yesterdayDate = "20191027"//date._1
    val testData = yesterdayDate//"20191027"
    // todo
    val day7ago = "20191021" //date._2
    println(yesterdayDate + "," + day7ago)
    //    val timeLabel = "week"

    val hbaseConf2 = HBaseConfiguration.create()
    hbaseConf2.set("hbase.regionsizecalculator.enable", "false") //aliyun hbase 不加有问题
    hbaseConf2.set(TableInputFormat.INPUT_TABLE, "dy:aweme-daily")

    val hbaseRDD2: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hbaseConf2,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val AwemeTwoDayData = hbaseRDD2.filter(line => {
      if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ps")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("dt")) != null
      ) {
        true
      } else {
        false
      }
    }).mapPartitions(part => {
      val tuples = part.map(line => {
        (
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ai"))),
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ps"))),
          Bytes.toLong(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("dc"))),
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("dt")))
        )
      })
      tuples
    }).toDF("ai", "ps", "dc", "d").filter($"ps".notEqual("") && $"d".between(day7ago, yesterdayDate))
      //    && $"d".between("20191012", "20191018")
      .select(
        $"ai",
        $"ps",
        (min("dc") over (Window.partitionBy("ai"))).alias("mindc"),
        (max("dc") over (Window.partitionBy("ai"))).alias("maxdc"))
      .select($"ai",
        $"ps",
        $"maxdc",
        ($"maxdc" - $"mindc").alias("chadc")
      ).distinct().repartition(24)

    //AwemeTwoDayData.show(false)

    //全部
    /*
    AwemeTwoDayData.orderBy(desc("chadc"))
      .limit(1501)
      .withColumn("rank", monotonically_increasing_id + 1)
      .foreachPartition(
        part => {
          val connection = ConnectionFactory.createConnection()
          val mutator = connection.getBufferedMutator(TableName.valueOf("dy:product-aweme-week-rank"))
          part.foreach(line => {
            val title = "AB"
            val aid = line(0).toString
            val dc = line(2).toString.toLong
            val chadc = line(3).toString.toLong
            val rank = line(4).toString.toInt
            val label = "不限"

            val put = new Put((title + testData + label + rank).getBytes)
            put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chadc"), Bytes.toBytes(chadc))
            put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("dc"), Bytes.toBytes(dc))
            put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
            put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(aid))

            mutator.mutate(put)
          })
          mutator.close()
        })
      */
    /**
     * 扫描视频表
     */
    val hbaseConf3 = HBaseConfiguration.create()
    hbaseConf3.set("hbase.regionsizecalculator.enable", "false") //aliyun hbase 不加有问题
    hbaseConf3.set(TableInputFormat.INPUT_TABLE, "dy:aweme")

    val hbaseRDD3: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hbaseConf3,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val awemeData = hbaseRDD3.filter(line => {
      if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ai")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("l")) != null) {
        true
      } else {
        false
      }
    }).mapPartitions(part => {
      val tuples = part.map(line => {
        (
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ai"))),
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("l")))
        )
      })
      tuples
    }).toDF("ai", "l").filter($"ai".notEqual(""))

    //awemeData.show(false)

    //获取行业分类
    val AwemeJoinData = AwemeTwoDayData.join(awemeData, AwemeTwoDayData("ai") === awemeData("ai"))
    //AwemeJoinData.show(false)

    val resData = AwemeJoinData.distinct()
      .select(
        AwemeTwoDayData("*"),
        awemeData("l"),
        (row_number() over ((Window.partitionBy("l").orderBy(desc("chadc"))))).alias("rank")
      ).where("rank < 1501")
    /*
    resData.foreachPartition(
        part => {
          val connection = ConnectionFactory.createConnection()
          val mutator = connection.getBufferedMutator(TableName.valueOf("dy:product-aweme-week-rank"))
          part.foreach(line => {
            val title = "B"
            val aid = line(0).toString
            val dc = line(2).toString.toLong
            val chadc = line(3).toString.toLong
            val label = line(4).toString
            val rank = line(5).toString.toInt

            val put = new Put((title + testData + label + rank).getBytes)
            put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chadc"), Bytes.toBytes(chadc))
            put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("dc"), Bytes.toBytes(dc))
            put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
            put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(aid))

            mutator.mutate(put)
          })
          mutator.close()
        }
      )
    */
    resData.filter($"ps".equalTo("563892788604")).show()
    resData.filter($"ai".equalTo("6750261977454595335")).show()

    /**
     * 扫描商品表
     */
      /*
    val hbaseConf4 = HBaseConfiguration.create()
    hbaseConf4.set("hbase.regionsizecalculator.enable", "false") //aliyun hbase 不加有问题
    hbaseConf4.set(TableInputFormat.INPUT_TABLE, "dy:promotion")

    val hbaseRDD4: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hbaseConf4,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val productData = hbaseRDD4.filter(line => {
      if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("awi")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("c1")) != null) {
        true
      } else {
        false
      }
    }).mapPartitions(part => {
      val tuples = part.map(line => {
        (
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("awi"))),
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("c1")))
        )
      })
      tuples
    }).toDF("ai", "l").filter($"ai".notEqual("") && $"l".notEqual(""))

    productData.show(false)



    //获取商品分类
    val productJoinData = AwemeTwoDayData.join(productData, AwemeTwoDayData("ai") === productData("ai"))
      .distinct()
      .select(
        AwemeTwoDayData("*"),
        productData("l"),
        (row_number() over ((Window.partitionBy("l").orderBy(desc("chadc"))))).alias("rank")
      ).where("rank < 1501")

    productJoinData.show(false)

    productJoinData.foreachPartition(
      part => {
        val connection = ConnectionFactory.createConnection()
        val mutator = connection.getBufferedMutator(TableName.valueOf("dy:product-aweme-week-rank"))
        part.foreach(line => {
          val title = "A"
          val aid = line(0).toString
          val dc = line(2).toString.toLong
          val chadc = line(3).toString.toLong
          val label = line(4).toString
          val rank = line(5).toString.toInt

          val put = new Put((title + testData + label + rank).getBytes)
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chadc"), Bytes.toBytes(chadc))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("dc"), Bytes.toBytes(dc))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(aid))

          mutator.mutate(put)
        })
        mutator.close()
      }
    )
    */
  }
}
