package test

import java.time.LocalDateTime
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.ml.feature.{MinMaxScaler, MinMaxScalerModel, Normalizer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object test {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[4]").appName("NOrmalize").getOrCreate()
    val df = sparkSession.createDataFrame(Seq((1, Vectors.dense(10000)),
      (2, Vectors.dense(9500)), (3, Vectors.dense(9000))))
      .toDF("id", "features")
    //Normalizer的作用范围是每一行，使每一个行向量的范数变换为一个单位范数
    val normalizer1 = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("normalfeatures")
    //      .setP(1.0)
    val model = normalizer1.fit(df).transform(df)
    model.show(false)
  }
}
