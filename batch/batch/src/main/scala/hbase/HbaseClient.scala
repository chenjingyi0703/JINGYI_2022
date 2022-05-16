package hbase


import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Result, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.CellScanner
import org.apache.hadoop.hbase.CellUtil

object HbaseClient {
  def main(args: Array[String]): Unit = {

    /**
     * 批量查
     */

    val configuration = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection()
    val table = connection.getTable(TableName.valueOf("test:ljk"))
    val scan = new Scan()
    //        scan.withStartRow("D20190923".getBytes())
    //    scan.withStopRow("D20190925".getBytes())
//    DigestUtils.sha1Hex("20191013")
    scan.setRowPrefixFilter(("A201910").getBytes())
    scan.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ai"))
    scan.addColumn(Bytes.toBytes("r"), Bytes.toBytes("l"))
    scan.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"))
    scan.addColumn(Bytes.toBytes("r"), Bytes.toBytes("frcAvg"))/*
    scan.addColumn(Bytes.toBytes("r"), Bytes.toBytes("nn"))
    scan.addColumn(Bytes.toBytes("r"), Bytes.toBytes("cmt"))
    scan.addColumn(Bytes.toBytes("r"), Bytes.toBytes("srt"))
    scan.addColumn(Bytes.toBytes("r"), Bytes.toBytes("pf"))
    scan.addColumn(Bytes.toBytes("r"), Bytes.toBytes("l"))
    scan.addColumn(Bytes.toBytes("r"), Bytes.toBytes("cityM"))
    scan.addColumn(Bytes.toBytes("r"), Bytes.toBytes("ageM"))
  */
    val result = table.getScanner(scan)


    val it: util.Iterator[Result] = result.iterator()
    println("ls")
    while (it.hasNext) {

      val next: Result = it.next()
      val row = next.getRow
      val rank = Bytes.toString(next.getValue(Bytes.toBytes("r"), Bytes.toBytes("c1")))

      //      if (next.getValue(Bytes.toBytes("r"), Bytes.toBytes("d")) != null) {
      //        println(Bytes.toString(next.getValue(Bytes.toBytes("r"), Bytes.toBytes("d"))))
      //      }
      //      val aid = Bytes.toString(next.getValue(Bytes.toBytes("r"), Bytes.toBytes("d")))
      //      val fc = Bytes.toLong(next.getValue(Bytes.toBytes("r"), Bytes.toBytes("fc")))
      var cmt: Long = 0L

      //      val nn = Bytes.toLong(next.getValue(Bytes.toBytes("r"), Bytes.toBytes("c")))
      val a = Bytes.toString(next.getValue(Bytes.toBytes("r"), Bytes.toBytes("ai")))
      val a1 = Bytes.toString(next.getValue(Bytes.toBytes("r"), Bytes.toBytes("l")))
      val a2 = Bytes.toString(next.getValue(Bytes.toBytes("r"), Bytes.toBytes("rk")))
      val a3 = Bytes.toString(next.getValue(Bytes.toBytes("r"), Bytes.toBytes("frcAvg")))
      if (next.getValue(Bytes.toBytes("r"), Bytes.toBytes("s")) != null) {
        cmt = Bytes.toInt(next.getValue(Bytes.toBytes("r"), Bytes.toBytes("d")))
      }

      //      val frc = Bytes.toLong(next.getValue(Bytes.toBytes("r"), Bytes.toBytes("frc")))
      println(Bytes.toString(row) + "," + rank + "," + a1 + "," + a ++ "," + a2)
    }


    /**
     * get过滤
     */
    //    val configuration = HBaseConfiguration.create()
    //    val connection = ConnectionFactory.createConnection()
    //    val table = connection.getTable(TableName.valueOf("dy:author-snapshot"))
    //
    //    val get = new Get(Bytes.toBytes(DigestUtils.sha1Hex("65246789056") + "20190928"))
    //    get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("nn"))
    //    get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a"))
    //    get.addColumn(Bytes.toBytes("r"), Bytes.toBytes("actd"))
    //    //dy:author-snapshot
    //    val result = table.get(get)
    //    val province = Bytes.toString(result.getValue(Bytes.toBytes("r"), Bytes.toBytes("prov")))
    //    val city = Bytes.toString(result.getValue(Bytes.toBytes("r"), Bytes.toBytes("nn")))
    //
    //    println(city)
    //
    //    val row = Bytes.toString(result.getRow())
    //    val cmt = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("province"))
    //    val srt = result.getValue(Bytes.toBytes("r"), Bytes.toBytes("province"))
    //
    //    println(Bytes.toString(cmt))
    //
    //    val cellScanner = result.cellScanner //遍历单元格
    //    while (cellScanner.advance) { //hasNext
    //      val cell = cellScanner.current //Next
    //      println(row + ":" + new String(CellUtil.cloneFamily(cell), "utf-8") + ":" + new String(CellUtil.cloneQualifier(cell), "utf-8") + "=" + new String(CellUtil.cloneValue(cell), "utf-8"))
    //    }
  }
}
