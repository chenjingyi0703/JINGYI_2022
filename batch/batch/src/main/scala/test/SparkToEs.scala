package test

import org.apache.spark.sql.SparkSession

object SparkToEs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkSqlDemo1")
      .master("local")
      .config("es.index.auto.create", "true")
      .config("es.nodes", "127.0.0.1")
      .config("es.port", "9200")
      .getOrCreate()

//    val data: DataFrame = spark.read.json("/Users/dn49/Desktop/视频.json")

//    data.rdd.foreachPartition(part => {
//      part.foreach(line => {
//      })
//    })

//    result.foreachRDD(rdd => {
//      rdd.map(line => {
//        val time = (System.currentTimeMillis() / 60000) * 60
//        Map("hostIp" -> line._1.hostIp,
//          "remoteIp" -> line._1.remoteIp,
//          "callTimes" -> line._2.toLong,
//          "time" -> time)
//      }).saveToEs("jsf_node_speed/infoType")
//    })

    //写入5.50之后
//    val esmap = Map("es.mapping.id" -> "aweme_id")
//    data.write.format("org.elasticsearch.spark.sql").options(esmap).save("bmps/order")


    //查询
    val query = """{"query":{"match":{"id": "row1"}}}"""
    val readDf = spark.read.format("org.elasticsearch.spark.sql")
      .option("query",query)
      .load(s"bmps/order")

    readDf.show(false)
  }
}
