package spark.author.grow.impl

import org.apache.spark.sql.SparkSession
import spark.author.grow.AbstractAuthorGrowRank
import utils.DateUtils

/**
 * @author LJK
 * @date 2019/10/29 4:31 下午
 */
class AuthorGrowDayRank extends AbstractAuthorGrowRank {
  override val spark: SparkSession = SparkSession
    .builder()
    .appName("博主成长榜日榜")
    .config("spark.yarn.maxAppAttempts", "1")
//    .master("local[*]")
    .getOrCreate()

  override val endDate: String = DateUtils.getYesterday
  override val startDate: String = DateUtils.getBeforeYesterday
  override val hbaseTableName: String = "dy:author-rank"
}

object AuthorGrowDayRank {
  def main(args: Array[String]): Unit = {
    val growDayRank = new AuthorGrowDayRank
    growDayRank.importAuthorGrowUpDF()
    growDayRank.spark.stop()
  }
}
