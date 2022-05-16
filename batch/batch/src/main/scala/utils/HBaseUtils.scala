package utils

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.util.ShutdownHookManager
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @author LJK
 * @date 2019/10/29 4:50 下午
 */
object HBaseUtils {
  private val instance: Connection = ConnectionFactory.createConnection()
  ShutdownHookManager.get().addShutdownHook(new Runnable {
    override def run(): Unit = {
      instance.close()
    }
  }, 1)

  def getConnection: Connection = instance
}
