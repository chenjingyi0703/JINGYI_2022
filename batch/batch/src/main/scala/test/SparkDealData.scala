package test

import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Mutation, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkDealData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ActionOperation")
      .master("local")
      .getOrCreate()

    val sc = spark.sparkContext

    val hbaseConf = HBaseConfiguration.create()
    // 将HBase底层Connection实现替换成HBase增强版专用的AliHBaseUEConnection
//    hbaseConf.set("hbase.client.connection.impl", AliHBaseUEConnection.class.getName());
//    // 集群的连接地址(注意公网地址和VPC内网地址)
//    hbaseConf.set("hbase.client.endpoint", "HOST:PORT");
//    // 设置用户名密码，默认root:root，可根据实际情况调整
//    hbaseConf.set("hbase.client.username", "root")
//    hbaseConf.set("hbase.client.password", "root")
    hbaseConf.set("hbase.zookeeper.quorum", "localhost")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "comment")
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "dimData2")
    //    hbaseConf.set(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.OUTPUT_TABLE, "dimData")
    hbaseConf.setClass("mapreduce.job.outputformat.class", classOf[org.apache.hadoop.hbase.mapreduce.TableOutputFormat[String]],
      classOf[org.apache.hadoop.mapreduce.OutputFormat[String, Mutation]])

    //读取评论表数据：视频id：fensiid,pinglunId
    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    import spark.implicits._

    val hbaseDataFrame: DataFrame = hbaseRDD.map(r => (
      Bytes.toString(r._2.getRow),
      Bytes.toString(r._2.getValue(Bytes.toBytes("t1"), Bytes.toBytes("id")))
    )).toDF("id", "fensi")

    val rddData: RDD[(String, String)] = hbaseDataFrame.rdd.mapPartitions(part => {
      val connection = ConnectionFactory.createConnection()
      val table = connection.getTable(TableName.valueOf("person"))

      val data = part.map(line => {
        val id = line(0).toString
        val fensi = line(1).toString
        val arrfensiId = fensi.split(",")
        var fensiInfo = ""

        for (i <- arrfensiId) {
          val get = new Get(Bytes.toBytes(i))
          get.addColumn(Bytes.toBytes("t1"), Bytes.toBytes("province"))
          get.addColumn(Bytes.toBytes("t1"), Bytes.toBytes("age"))
          val result = table.get(get)
          val cellScanner = result.cellScanner //遍历单元格
          fensiInfo += i + ","
          while (cellScanner.advance) { //hasNext
            val cell = cellScanner.current //Next
            fensiInfo += new String(CellUtil.cloneValue(cell), "utf-8") + ","
          }
          fensiInfo = fensiInfo.substring(0, fensiInfo.length - 1) + "||"
        }
        (id, fensiInfo)
      })
      data
    })

    //行转列
    val resultData: RDD[(String, String)] = rddData.map(v => (v._1, v._2.split("\\|\\|")))
      .map(v => {
        v._2.map(x => (v._1, x))
      })
      .flatMap(x => x)
      .filter(_._2.split(",").length > 2)

    resultData.foreach(println)

    //    resultData.toDF.write.options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5")).format("org.apache.spark.sql.execution.datasources.hbase").save()

    //存入hbase
//    resultData.mapPartitions(part => {
//      part.map(line => {
//        val rowKey = line._1
//        val info = line._2
//        var put = new Put(Bytes.toBytes(rowKey))
//        put.addImmutable(Bytes.toBytes("c"), Bytes.toBytes("i"), Bytes.toBytes(info))
//        (new ImmutableBytesWritable(), put)
//      })
//    }).saveAsNewAPIHadoopDataset(hbaseConf)

    //    val rddDataToDF: DataFrame = rddData.toDF("id","info").select("info")
    //    rddDataToDF.show(false)
    //    rddDataToDF.select("info")

    //        rddData.foreach(println)

    //    val bulkLoader = new LoadIncrementalHFiles(hbaseConf)
    //    bulkLoader.doBulkLoad(new Path("hdfs://localhost:8020/tmp/hbase"),table)
  }
}
