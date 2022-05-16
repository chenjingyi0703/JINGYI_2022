package test

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ActionOperation")
      .master("local")
      .config("es.index.auto.create", "true")
      .config("es.nodes", "127.0.0.1")
      .config("es.port", "9200")
      .getOrCreate()

    val sc = spark.sparkContext

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "localhost")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "person")

    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    import spark.implicits._

    val hbaseDataFrame: DataFrame = hbaseRDD.map(r => (
      Bytes.toString(r._2.getRow),
      Bytes.toString(r._2.getValue(Bytes.toBytes("t1"), Bytes.toBytes("province"))),
      Bytes.toString(r._2.getValue(Bytes.toBytes("t1"), Bytes.toBytes("age")))
    )).toDF("id", "province", "age")

    //    hbaseRDD.foreach({ case (_,result) =>
    //      val key = Bytes.toString(result.getRow)
    //      val province = Bytes.toString(result.getValue("t1".getBytes,"province".getBytes))
    //      val age = Bytes.toString(result.getValue("t1".getBytes,"age".getBytes))
    //      println("Row key:"+key+" Province:"+province+" Age:"+age)
    //    })

    //    print( hbaseDataFrame.count())
    //    hbaseDataFrame.show(2000, false)
    hbaseDataFrame.createOrReplaceTempView("tmp")
    val resultData: DataFrame = spark.sql("select id,province,count(age)over(partition by age) as count from tmp ")
    resultData.show(false)
    //    hbaseDataFrame.rdd.saveToEs("")

    val esmap = Map("es.mapping.id" -> "id")
    resultData.write.format("org.elasticsearch.spark.sql")
      .mode(SaveMode.Append)
      .options(esmap)
      .save("bmps/order")

  }
}
