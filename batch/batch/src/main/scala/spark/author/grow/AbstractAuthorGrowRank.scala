package spark.author.grow

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import spark.author.grow.hbase.HBaseAuthorGrowHelper
import utils.HBaseUtils

/**
 * @author LJK
 * @date 2019/10/29 10:54 上午
 */
abstract class AbstractAuthorGrowRank extends Serializable {

  val spark: SparkSession

  import org.apache.spark.sql.functions._
  import spark.implicits._

  // 日榜是昨天和前天、周榜是最大粉丝那天和最小粉丝那天、月榜和周榜一样
  val endDate: String
  val startDate: String
  val hbaseTableName: String

  val growIndex: UserDefinedFunction = udf {
    (followCount: Long, incrementFrc: Long, incrementFc: Long) => {
      Vectors.dense(followCount / 1000000 + incrementFrc / 10000 * 0.5 + incrementFc / 10000 * 0.5)
    }
  }

  /**
   * 成长榜入库HBase
   */
  def importAuthorGrowUpDF(): Unit = {
    val twoDayHBaseDF = this.getTwoDayHBaseDF
    // 算出粉丝增量和点赞增量以及成长指数
    val incrementTwoTimeData = twoDayHBaseDF
      .withColumn("increment_frc", $"frc" - $"byfrc")
      .withColumn("increment_fc", $"fc" - $"byfc")
      //      .withColumn("grow_index", $"frc" / 10000 * 0.5 + $"increment_frc" / 10000 * 0.25 + $"increment_fc" / 10000 * 0.25)
      .withColumn("grow_index", growIndex($"frc", $"increment_frc", $"increment_fc"))
      .filter($"increment_frc" > 0)
      .filter($"increment_fc" > 0)
      .select("ai", "nn", "a", "fc", "frc", "l", "byfrc", "byfc", "increment_frc", "increment_fc", "grow_index")

    val authorGrowModel: DataFrame = growIndex(incrementTwoTimeData)

    authorGrowModel.persist(StorageLevel.MEMORY_ONLY_SER)

    authorGrowModel.show()
    this.unlimitGrowUpImportHBase(authorGrowModel)
    this.labelGrowUpImportHBase(authorGrowModel)

    authorGrowModel.unpersist()
  }


  /**
   * 从HBase捞出两天数据
   *
   * @return
   */
  private def getTwoDayHBaseDF: DataFrame = {
    val yDF = HBaseAuthorGrowHelper.getOneDayOfAuthor(endDate, spark)
    val byDF = HBaseAuthorGrowHelper.getOneDayOfAuthor(startDate, spark)

    yDF.join(byDF, yDF("ai") === byDF("ai"))
      .select(
        yDF("*"),
        byDF("frc").alias("byfrc"),
        byDF("fc").alias("byfc"))
  }


  /**
   * 成长指数归一化
   *
   * @param incrementTwoTimeData
   * @return
   */
  private def growIndex(incrementTwoTimeData: DataFrame) = {

    val normalizerAuthor = new MinMaxScaler()
      .setInputCol("grow_index")
      .setOutputCol("normalfeatures")

    val authorGrowModel = normalizerAuthor
      .fit(incrementTwoTimeData)
      .transform(incrementTwoTimeData)
    authorGrowModel
  }

  /**
   * 不限成长排名入库HBase
   *
   * @param authorGrowUpDF
   */
  private def unlimitGrowUpImportHBase(authorGrowUpDF: DataFrame): Unit = {

    // 不限成长排名，根据成长指数排名
    val authorUnlimitRank: Dataset[Row] = authorGrowUpDF.select(
      (row_number() over Window.orderBy($"normalfeatures".desc)).alias("rank"),
      $"*")
      .where($"rank" <= 1500)

    authorUnlimitRank.show()
    authorUnlimitRank.foreachPartition(
      parts => {
        val connection = HBaseUtils.getConnection
        val mutator = connection.getBufferedMutator(TableName.valueOf(hbaseTableName))
        parts.foreach(row => {
          val authorId: String = row.getAs("ai")
          val nickName: String = row.getAs("nn")
          val avatar: String = row.getAs("a")
          val followerCount: Long = row.getAs("frc")
          val increFollowerCount: Long = row.getAs("increment_frc")
          val increFavoritedCount: Long = row.getAs("increment_fc")
          val rank: Int = row.getAs("rank")

          val put = new Put(Bytes.toBytes("G" + endDate + "不限" + rank))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(authorId))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("nn"), Bytes.toBytes(nickName))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a"), Bytes.toBytes(avatar))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("frc"), Bytes.toBytes(followerCount))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("inc_frc"), Bytes.toBytes(increFollowerCount))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("inc_fc"), Bytes.toBytes(increFavoritedCount))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))

          mutator.mutate(put)
        })
        mutator.close()
      }
    )
  }


  /**
   * 标签排名入库
   *
   * @param authorGrowUpDF
   */
  private def labelGrowUpImportHBase(authorGrowUpDF: DataFrame): Unit = {

    // 标签排名
    val authorLabelRank: Dataset[Row] = authorGrowUpDF.select(
      (row_number() over Window.partitionBy("l").orderBy($"normalfeatures".desc)).alias("rank"),
      $"*")
      .where($"rank" <= 1500)

    authorLabelRank.show()

    authorLabelRank.foreachPartition(
      parts => {
        val connection = ConnectionFactory.createConnection()
        val mutator = connection.getBufferedMutator(TableName.valueOf(hbaseTableName))
        parts.foreach(row => {
          val authorId: String = row.getAs("ai")
          val nickName: String = row.getAs("nn")
          val avatar: String = row.getAs("a")
          val followerCount: Long = row.getAs("frc")
          val increFollowerCount: Long = row.getAs("increment_frc")
          val increFavoritedCount: Long = row.getAs("increment_fc")
          val rank: Int = row.getAs("rank")
          val label: String = row.getAs("l")

          val put = new Put(Bytes.toBytes("G" + endDate + label + rank))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"), Bytes.toBytes(authorId))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("nn"), Bytes.toBytes(nickName))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a"), Bytes.toBytes(avatar))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("frc"), Bytes.toBytes(followerCount))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("inc_frc"), Bytes.toBytes(increFollowerCount))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("inc_fc"), Bytes.toBytes(increFavoritedCount))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rank))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("l"), Bytes.toBytes(label))

          mutator.mutate(put)
        })
        mutator.close()
      }
    )
  }


}
