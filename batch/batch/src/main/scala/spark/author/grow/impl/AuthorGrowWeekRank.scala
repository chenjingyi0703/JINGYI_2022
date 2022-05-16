package spark.author.grow.impl

import org.apache.spark.sql.SparkSession
import spark.author.grow.AbstractAuthorGrowRank
import utils.DateUtils

/**
 * @author LJK
 * @date 2019/10/30 11:25 上午
 */
class AuthorGrowWeekRank extends AbstractAuthorGrowRank {

  override val spark: SparkSession = SparkSession
    .builder()
    .appName("博主成长榜周榜")
    .config("spark.yarn.maxAppAttempts", "1")
//    .master("local[*]")
    .getOrCreate()

  override val endDate: String = DateUtils.getWeekEnd
  override val startDate: String = DateUtils.getWeekStart
  override val hbaseTableName: String = "dy:author-week-rank"
}

object AuthorGrowWeekRank {
  def main(args: Array[String]): Unit = {
    val growWeekRank = new AuthorGrowWeekRank
    growWeekRank.importAuthorGrowUpDF()
    growWeekRank.spark.stop()
  }
}
