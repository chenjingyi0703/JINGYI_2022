package test

import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DoubleType, LongType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import utils.SparkUtils
import org.apache.spark.mllib.linalg.Vector

object SparkExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder() //构建sparkContext，这里用到了java里的构造器设计模式
      .appName("SparkSqlDemo1")
      .master("local")
      .getOrCreate()


    //    spark.sql("select to_date('20190920')")

    //    val rdd1 = spark.sparkContext.parallelize(List(("tom", "1"), ("jerry", "3"), ("kitty", "2"), ("tom", "2"), ("tom", "3")))
    //
    //    rdd1.groupByKey().mapPartitions(part => {
    //      part.map(line => {
    //        val value = line._2.toList
    //        for (i <- value) {
    //          println(i)
    //        }
    //      })
    //    }).foreach(println)
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val testData: DataFrame = spark.sparkContext.parallelize(List(
      ("a", "20190925", 11, 22000000, 33),
      ("b", "20190924", 22, 22, 33),
      ("c", "20190923", 33, 11, 11),
      ("d", "20190922", 44, 0, 0),
      ("e", "20190921", 55, 111, 11),
      ("a", "20190918", 11, 110000000, 0),
      ("a", "20190930", 22, 0, 0),
      ("h", "20190920", 11, 0, 0),
      ("h", "", 1, 251, 61),
      ("i", null, 22, 124, 66)
    )).toDF("ui", "ct", "ai", "cm", "sh")

    val data: DataFrame = testData.mapPartitions(part => {
      val tuples = part.map(line => {
        val auhtor_id = line(0).toString
        val authorIndex = Vectors.dense(line(4).toString.toLong)

        (auhtor_id, authorIndex)
      })
      tuples
    }).toDF("a", "cm")
    //
    data.printSchema()
    val normalizer1 = new MinMaxScaler()
      .setInputCol("cm")
      .setOutputCol("normalfeatures")
    //
    val Authormodel: DataFrame = normalizer1
      .fit(data)
      .transform(data)
    Authormodel.show(false)

    val assembler = new VectorAssembler()
      .setInputCols(Array("val1", "val2"))
      .setOutputCol("vectorCol")

    Authormodel.select($"normalfeatures")

    Authormodel.map(line => {
      val a = line(0).toString
      val normalfeatures = line(2).toString
      (a, normalfeatures.substring(1, normalfeatures.length - 1))
    }).toDF("a", "sadasdasda").show(false)

    val testDF = spark.sparkContext.parallelize(List(Vectors.dense(5D, 6D, 7D), Vectors.dense(8D, 9D, 10D), Vectors.dense(11D, 12D, 13D))).map(Tuple1(_)).toDF("scaledFeatures")
    testDF.show(false)

    //    val testData14: DataFrame = spark.sparkContext.parallelize(List(
    //      ("a", "20190925", 1232313, 22000000, 33),
    //      ("b", "20190924", 11223, 22, 33),
    //      ("c", "20190923", 33, 11, 11),
    //      ("d", "20190922", 44, 0, 0),
    //      ("e", "20190921", 55, 111, 11),
    //      ("a", "20190918", 11, 110000000, 0),
    //      ("a", "20190930", 22, 0, 0),
    //      ("h", "20190920", 11, 0, 0),
    //      ("h", "", 1, 251, 61),
    //      ("i", null, 22, 124, 66)
    //    )).toDF("ui", "ct", "ai", "cm", "sh")


    //    testData14.join(testData, testData("ui") === testData14("ui"))
    //      .select(testData14("*"), testData("ai"))
    //      .show(false)
    //
    //
    //    testData.select(($"cm" - $"sh").alias("chac")).show(false)
    //
    //    testData.filter($"ct".notEqual("null") && $"ct".notEqual("")).select($"*", (min("cm") over (Window.partitionBy("ui"))).alias("min"),
    //      (max("cm") over (Window.partitionBy("ui"))).alias("max"))
    //      //      .select($"*").where($"ct".equalTo($"min") or ($"ct".equalTo($"max")))
    //      .show()
    //    testData.select($"*", row_number() over (Window.orderBy("sh")))
    //      .withColumn("id", monotonically_increasing_id + 1).show(false)
    //
    //    testData.select($"*", max("cm") over (Window.partitionBy("ui"))).show(false)
    //
    //    testData.createOrReplaceTempView("tmp")
    //    spark.sql("select `ui`,stack(3, 'ai', `ai`, 'cm', `cm`, 'sh', `sh`) as (`age`, `num` )from  tmp").show(100, false)


    //    '''select `科目`,
    //    stack(4, '张三', `张三`, '王五', `王五`, '李雷', `李雷`, '宫九', `宫九`) as (`姓名`, `分数` )
    //    from  v_pivot
    //      '''
    //    testData.groupBy("ui")
    //      .pivot("ai")
    //      .show(false)

    //    testData.withColumn("t", $"cm" - $"sh").show()
    //    //    testData.select(($"cm" - $"sh").alias("t")).show()
    //    testData.filter($"ct".between("20190920", "20190925")).show(false)


    //    testData.groupBy("ui")
    //      .agg(
    //        (sum("ai") / 7).cast(LongType),
    //        (sum("cm") / 7).cast(LongType),
    //        (sum("sh") / 7).cast(LongType)
    //      )
    //      .show(false)
    //    testData.filter($"ct".notEqual("") && ($"ct".isNotNull)).show(false)
    //    testData.filter($"ui".equalTo("a")).show(false)

    //    val testData2: DataFrame = spark.sparkContext.parallelize(List(
    //      ("a", "2019-09-08", "博主A"),
    //      ("b", "2019-09-08", "博主B"),
    //      ("c", "2019-09-08", "博主A"),
    //      ("d", "2019-09-08", "博主A"),
    //      ("e", "2019-09-08", "博主A"),
    //      ("f", "2019-09-08", "博主A"),
    //      ("g", "2019-09-08", "博主A"),
    //      ("h", "2019-09-08", "博主A"),
    //      ("h", "2019-09-08", "博主A"),
    //      ("i", "2019-09-08", "博主A")
    //    )).toDF("ui", "ct", "ai")

    //    testData2.select($"ui", (row_number() over (Window.partitionBy("ui"))).alias("rank")).where("rank<2")

    //    testData.withColumn("textNew", explode(split($"fensi", ",")))
    //      .where("fensi != ''")
    //
    //    val date2Constellation = spark.udf.register("date2Constellation", SparkUtils.date2Constellation(_: String))
    //    testData2.select(date2Constellation($"ct")).show(false)
    //    spark.sql("select date2Constellation('2019-09-08') as Constellation").show(false)

    //    spark.sql("select from_unixtime(1568207899, 'HH')").show(false)
    //    testData.where($"ct".notEqual("")).select($"ai", from_unixtime($"ct", "HH").as("hour"), $"ui")
    //      .groupBy("ai", "hour")
    //      .count()
    //      .show(false)
    //
    //
    //    testData.where($"ct".notEqual("")).select($"ai", dayofweek(from_unixtime($"ct")).as("day"))
    //      .groupBy("ai", "day")
    //      .count()
    //      .show()

    //    val rdd1 = spark.sparkContext.parallelize(List(
    //      ("tom", "黑龙江", "哈尔滨", 11),
    //      ("ack", "黑龙江", "哈尔滨", 99),
    //      ("jerry", "黑龙江", "大庆", 12),
    //      ("ted", "黑龙江", "大庆", 100),
    //      ("ity", "辽宁", "沈阳", 13),
    //      ("kit", "辽宁", "沈阳", 99),
    //      ("kitty", "辽宁", "大连", 12),
    //      ("chen", "内蒙古", "呼和浩特", 23),
    //      ("cen", "内蒙古", "包头", 230),
    //      ("chn", "内蒙古", "包头", 130),
    //      ("lee", "澳门", "澳门", 2325),
    //      ("xe", "澳门", "澳门", 223),
    //      ("lalala", "", "", 222)
    //    )
    //    )
    //
    //    val data = rdd1.toDF("name", "p", "c", "num")
    //    data.createOrReplaceTempView("tmp")
    //    data.where("p != ''").select("*").show()
    //
    //    val dataOrder = spark.sql("select rank() over(partition by c order by num desc) as t,* from tmp")
    //
    //    dataOrder.show()
    //
    //    dataOrder
    //      .select($"c", concat_ws(",", $"t", $"name") as ("info"))
    //      .groupBy("c")
    //      .agg(concat_ws("||", collect_list($"info")) as ("info"))
    //      .show(false)

    //      .concat(concat(name,' '),id)
    //    spark.sql("select id+cast(num as String) as g from tmp").show()
    //    val cityConcat: DataFrame = data
    //      .select($"name", concat_ws(",", $"id", $"num") as ("info"))
    //      .groupBy("name")
    //      .agg(concat_ws("||", collect_set($"info")))

    //        .agg(concat($"name",$"id")

    //    cityConcat.show(false)
    //    val data: RDD[String] = spark.sparkContext.textFile("/Users/dn49/Desktop/视频.txt")
    //    val data: DataFrame = spark.read.json("/Users/dn49/Desktop/视频.json")
    //    print(data.count())
    //    data.printSchema()
    //    //导入spark的隐私转换
    //    import spark.implicits._
    //    //导入spark Sql的functions
    //    import org.apache.spark.sql.functions._
    //    data.show(false)
    //    data.select("data.aweme_cover","data.author_id")
    //      .groupBy("author_id")
    //      .count()
    //      .show(false)


    //    val originPlayVideoTags =  spark.sparkContext.parallelize(List(("zjf","1,2,3")))
    //    val a = originPlayVideoTags.collect()
    //    for (_a <- a){
    //      println(_a)
    //    }
    //    val b = originPlayVideoTags
    //      .map(v=> (v._1,v._2.split(",")))
    //      .map(v=>{
    //        v._2.map(x=>(v._1,x))
    //      })
    //      .flatMap(x=>x)
    //    for (_b <- b.collect()){println(_b)}

  }
}
