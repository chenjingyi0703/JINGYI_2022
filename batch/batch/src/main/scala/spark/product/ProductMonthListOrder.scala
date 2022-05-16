package spark.product

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import utils.SparkUtils


/**
 * 抖音商品月榜
 * author: hubf
 * 2019-10-25
 */
object ProductMonthListOrder {
 //todo 发布前记得改结果表名
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ActionOperation")
      .config("spark.yarn.maxAppAttempts", "1")
      //      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //获取上个月的数据
    val dateArr: Array[String]  = SparkUtils.datesOfMonth(1)
    val yesterdayDate = dateArr(dateArr.length - 1)
    val scan0 = new Scan()
    scan0.setRowPrefixFilter(DigestUtils.sha1Hex(dateArr(0)).getBytes())
    val hbaseConf = SparkUtils.hbaseConfSet(scan0, "dy:product-daily")

    var hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.
      newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
        classOf[ImmutableBytesWritable],classOf[Result])

    for(i <- 8 to 9){
    //for(i <- 1 to dateArr.length - 1){
      val scan = new Scan()
      scan.setRowPrefixFilter(DigestUtils.sha1Hex(dateArr(i)).getBytes())
      val hbaseConf = SparkUtils.hbaseConfSet(scan, "dy:product-daily")

      val tmpRDD: RDD[(ImmutableBytesWritable, Result)] = sc.
        newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
          classOf[ImmutableBytesWritable],classOf[Result])
      hbaseRDD = hbaseRDD.union(tmpRDD)
    }

    val odsdata = hbaseRDD.filter(line => {
      if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("c1")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("pf")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("d")) != null
      ) {
        true
      } else {
        false
      }
    }).mapPartitions(part => {
      val tuples = part.map(line => {
        var s: Long = -1L
        var c: Long = -1L
        //        var pf: String = "未知来源"
        if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("s")) != null) {
          s = Bytes.toLong(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("s")))
        }

        if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("c")) != null) {
          c = Bytes.toLong(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("c")))
        }
        (
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("pi"))), //商品id
          s, //销量
          c, //浏览量
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("c1"))), //商品分类
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("pf"))), //商品来源分类
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("d"))) //商品日期
        )
      })
      tuples
    }).toDF("pi", "s", "c", "l", "pf", "d")

    /*
    //日期格式
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance()

    //上月
    val m = calendar.get(Calendar.MONTH)
    calendar.add(Calendar.MONTH, -1);

    //日期不为1则减至1
    while(calendar.get(Calendar.DAY_OF_MONTH) != 1){
      val dateNum = calendar.get(Calendar.DAY_OF_MONTH)
      calendar.add(Calendar.DAY_OF_MONTH, -1)
      //println(dateNum)
    }


    //扫描一个月的数据
    while(calendar.get(Calendar.MONTH) < m){
      //扫描数据
      val date = dateFormat.format(calendar.getTime())
      val scan = new Scan()
      scan.setRowPrefixFilter(DigestUtils.sha1Hex(date).getBytes())
      val scanString = TableMapReduceUtil.convertScanToString(scan)
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set("hbase.regionsizecalculator.enable", "false") //aliyun hbase 不加有问题
      hbaseConf.set(TableInputFormat.INPUT_TABLE, "dy:product-daily")
      hbaseConf.set(TableInputFormat.SCAN, scanString)

      val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.
        newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
          classOf[ImmutableBytesWritable],
          classOf[Result])
      calendar.add(Calendar.DAY_OF_MONTH, 1)
    }
    */
    val odsMonthMapData = odsdata.filter($"s".notEqual(-1) && $"c".notEqual(-1)).select(
      $"pi",
      $"pf",
      $"l",
      (min("s") over (Window.partitionBy("pi"))).alias("mins"),
      (max("s") over (Window.partitionBy("pi"))).alias("maxs"),
      (min("c") over (Window.partitionBy("pi"))).alias("minc"),
      (max("c") over (Window.partitionBy("pi"))).alias("maxc"))
      .distinct()
      .filter($"l".notEqual(""))
      .select($"pi",
        $"pf",
        $"l",
        ($"maxs" - $"mins").alias("chas"),
        ($"maxc" - $"minc").alias("chac")
      ).repartition(24)

    //odsMonthMapData.show(false)


    val hbaseConf2 = HBaseConfiguration.create()
    hbaseConf2.set("hbase.regionsizecalculator.enable", "false") //aliyun hbase 不加有问题
    hbaseConf2.set(TableInputFormat.INPUT_TABLE, "dy:product")

    val hbaseRDD2: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hbaseConf2,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val productData = hbaseRDD2.filter(line => {
      if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("pi")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("prov")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("city")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("age")) != null
      ) {
        true
      } else {
        false
      }
    }).mapPartitions(part => {
      val tuples = part.map(line => {
        (
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("pi"))), //商品id
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("prov"))), //省份
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("city"))), //城市
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("age"))) //年龄
        )
      })
      tuples
    }).toDF("pi", "prov", "city", "age")

    val odsJoinData = odsMonthMapData.join(productData, odsMonthMapData("pi") === productData("pi"))
      .select(odsMonthMapData("*"), productData("prov"), productData("city"), productData("age"))


    //浏览增量榜单，行业榜单
    val chacOrderList = odsJoinData
      .select($"pi",
        $"l",
        $"prov",
        $"city",
        $"age",
        (row_number() over (Window.orderBy($"chac".desc))).alias("rankc"),
        (row_number() over (Window.orderBy($"chas".desc))).alias("ranks"),
        (row_number() over (Window.partitionBy("l").orderBy($"chac".desc))).alias("ranklc"),
        (row_number() over (Window.partitionBy("l").orderBy($"chas".desc))).alias("rankls"),
        (row_number() over (Window.partitionBy("pf").orderBy($"chac".desc))).alias("rankpfc"),
        (row_number() over (Window.partitionBy("pf").orderBy($"chas".desc))).alias("rankpfs"),
        (row_number() over (Window.partitionBy("l", "pf").orderBy($"chac".desc))).alias("ranklcpf"),
        (row_number() over (Window.partitionBy("l", "pf").orderBy($"chas".desc))).alias("ranklspf"),
        $"chac",
        $"chas",
        $"pf"
      )

    //chacOrderList.show(false)

    chacOrderList.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      //val mutator = connection.getBufferedMutator(TableName.valueOf("dy:product-month-rank"))
      val mutator = connection.getBufferedMutator(TableName.valueOf("test:ljk"))
      part.foreach(line => {
        val title = "A"
        val pid = line(0).toString
        val label = line(1).toString
        val prov = line(2).toString
        val city = line(3).toString
        val age = line(4).toString
        val rankc = line(5).toString.toInt
        val ranks = line(6).toString.toInt
        val ranklc = line(7).toString.toInt
        val rankls = line(8).toString.toInt
        val rankpfc = line(9).toString.toInt
        val rankpfs = line(10).toString.toInt
        val ranklcpf = line(11).toString.toInt
        val ranklspf = line(12).toString.toInt
        val chac = line(13).toString.toLong
        val chas = line(14).toString.toLong
        val pflabel = line(15).toString


        //浏览量全部
        if (rankc <= 10000) {
          val put = new Put((title + yesterdayDate + "不限c" + rankc).getBytes)
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("pi"), Bytes.toBytes(pid))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rankc))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chac"), Bytes.toBytes(chac))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chas"), Bytes.toBytes(chas))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("prov"), Bytes.toBytes(prov))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("city"), Bytes.toBytes(city))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("age"), Bytes.toBytes(age))
          mutator.mutate(put)
        }

        //销量全部
        if (ranks <= 10000) {
          val put2 = new Put((title + yesterdayDate + "不限s" + ranks).getBytes)
          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("pi"), Bytes.toBytes(pid))
          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(ranks))
          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chac"), Bytes.toBytes(chac))
          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chas"), Bytes.toBytes(chas))

          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("prov"), Bytes.toBytes(prov))
          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("city"), Bytes.toBytes(city))
          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("age"), Bytes.toBytes(age))

          mutator.mutate(put2)
        }

        //浏览量行业
        if (ranklc <= 10000) {
          val put3 = new Put((title + yesterdayDate + label + "c" + ranklc).getBytes)
          put3.addColumn(Bytes.toBytes("r"), Bytes.toBytes("pi"), Bytes.toBytes(pid))
          put3.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(ranklc))
          put3.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chac"), Bytes.toBytes(chac))
          put3.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chas"), Bytes.toBytes(chas))

          put3.addColumn(Bytes.toBytes("r"), Bytes.toBytes("prov"), Bytes.toBytes(prov))
          put3.addColumn(Bytes.toBytes("r"), Bytes.toBytes("city"), Bytes.toBytes(city))
          put3.addColumn(Bytes.toBytes("r"), Bytes.toBytes("age"), Bytes.toBytes(age))

          mutator.mutate(put3)
        }

        //销量行业
        if (rankls <= 10000) {
          val put4 = new Put((title + yesterdayDate + label + "s" + rankls).getBytes)
          put4.addColumn(Bytes.toBytes("r"), Bytes.toBytes("pi"), Bytes.toBytes(pid))
          put4.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rankls))
          put4.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chac"), Bytes.toBytes(chac))
          put4.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chas"), Bytes.toBytes(chas))

          put4.addColumn(Bytes.toBytes("r"), Bytes.toBytes("prov"), Bytes.toBytes(prov))
          put4.addColumn(Bytes.toBytes("r"), Bytes.toBytes("city"), Bytes.toBytes(city))
          put4.addColumn(Bytes.toBytes("r"), Bytes.toBytes("age"), Bytes.toBytes(age))

          mutator.mutate(put4)
        }

        //浏览量来源
        if (rankpfc <= 10000) {
          val put5 = new Put((title + yesterdayDate + pflabel + "c" + rankpfc).getBytes)
          put5.addColumn(Bytes.toBytes("r"), Bytes.toBytes("pi"), Bytes.toBytes(pid))
          put5.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rankpfc))
          put5.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chac"), Bytes.toBytes(chac))
          put5.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chas"), Bytes.toBytes(chas))

          put5.addColumn(Bytes.toBytes("r"), Bytes.toBytes("prov"), Bytes.toBytes(prov))
          put5.addColumn(Bytes.toBytes("r"), Bytes.toBytes("city"), Bytes.toBytes(city))
          put5.addColumn(Bytes.toBytes("r"), Bytes.toBytes("age"), Bytes.toBytes(age))

          mutator.mutate(put5)
        }

        //销量来源
        if (rankpfs <= 10000) {
          val put6 = new Put((title + yesterdayDate + pflabel + "s" + rankpfs).getBytes)
          put6.addColumn(Bytes.toBytes("r"), Bytes.toBytes("pi"), Bytes.toBytes(pid))
          put6.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rankpfs))
          put6.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chac"), Bytes.toBytes(chac))
          put6.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chas"), Bytes.toBytes(chas))

          put6.addColumn(Bytes.toBytes("r"), Bytes.toBytes("prov"), Bytes.toBytes(prov))
          put6.addColumn(Bytes.toBytes("r"), Bytes.toBytes("city"), Bytes.toBytes(city))
          put6.addColumn(Bytes.toBytes("r"), Bytes.toBytes("age"), Bytes.toBytes(age))

          mutator.mutate(put6)
        }

        //浏览量行业来源
        if (ranklcpf <= 10000) {
          val put7 = new Put((title + yesterdayDate + label + pflabel + "c" + ranklcpf).getBytes)
          put7.addColumn(Bytes.toBytes("r"), Bytes.toBytes("pi"), Bytes.toBytes(pid))
          put7.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(ranklcpf))
          put7.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chac"), Bytes.toBytes(chac))
          put7.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chas"), Bytes.toBytes(chas))

          put7.addColumn(Bytes.toBytes("r"), Bytes.toBytes("prov"), Bytes.toBytes(prov))
          put7.addColumn(Bytes.toBytes("r"), Bytes.toBytes("city"), Bytes.toBytes(city))
          put7.addColumn(Bytes.toBytes("r"), Bytes.toBytes("age"), Bytes.toBytes(age))

          mutator.mutate(put7)
        }

        //销量行业来源
        if (ranklspf <= 10000) {
          val put8 = new Put((title + yesterdayDate + label + pflabel + "s" + ranklspf).getBytes)
          put8.addColumn(Bytes.toBytes("r"), Bytes.toBytes("pi"), Bytes.toBytes(pid))
          put8.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(ranklspf))
          put8.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chac"), Bytes.toBytes(chac))
          put8.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chas"), Bytes.toBytes(chas))

          put8.addColumn(Bytes.toBytes("r"), Bytes.toBytes("prov"), Bytes.toBytes(prov))
          put8.addColumn(Bytes.toBytes("r"), Bytes.toBytes("city"), Bytes.toBytes(city))
          put8.addColumn(Bytes.toBytes("r"), Bytes.toBytes("age"), Bytes.toBytes(age))

          mutator.mutate(put8)
        }
      })
      mutator.close()
    })

    /**
     * 扫描视频表的数据
     */
    val hbaseAwemeConf = HBaseConfiguration.create()
    hbaseAwemeConf.set("hbase.regionsizecalculator.enable", "false")
    hbaseAwemeConf.set(TableInputFormat.INPUT_TABLE, "dy:aweme")

    val hbaseAwemeRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hbaseAwemeConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val awemeData = hbaseAwemeRDD.filter(line => {
      if (line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("s1")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("s2")) != null
        && line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ps")) != null
      ) {
        true
      } else {
        false
      }
    }).mapPartitions(part => {
      val tuples = part.map(line => {
        (
          Bytes.toString(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("ps"))), //商品id
          Bytes.toLong(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("s1"))), //性别男
          Bytes.toLong(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("s2"))) //性别女
          //          Bytes.toLong(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("city"))), //city
          //          Bytes.toLong(line._2.getValue(Bytes.toBytes("r"), Bytes.toBytes("province"))) //province
        )
      })
      tuples
    }).toDF("pi", "male", "female")
      .filter($"pi".notEqual(""))

    //切割行转列打标签
    val awemeMapData = awemeData.filter($"pi".notEqual(""))
      .withColumn("pi", explode(split($"pi", ",")))
      .groupBy("pi")
      .agg(sum($"male").alias("maleSum"), sum($"female").alias("femaleSum"))
      .select($"*", ($"maleSum" > $"femaleSum").alias("maleMax"))

    //awemeMapData.show(false)

    //连表
    val producetJoinAwemeData = odsJoinData.join(awemeMapData, odsJoinData("pi") === awemeMapData("pi"))
      .select(odsJoinData("*"), awemeMapData("maleSum"), awemeMapData("femaleSum"), awemeMapData("maleMax"))

    //producetJoinAwemeData.show(false)

    //过滤出男性多的商品
    val maleMaxProduct = producetJoinAwemeData.filter($"maleMax".equalTo(true))
    val maleProductOrder = maleMaxProduct.select($"pi",
      $"l",
      $"prov",
      $"city",
      $"age",
      (row_number() over (Window.orderBy($"chac".desc))).alias("rankc"),
      (row_number() over (Window.orderBy($"chas".desc))).alias("ranks"),
      (row_number() over (Window.partitionBy("pf").orderBy($"chac".desc))).alias("rankpfc"),
      (row_number() over (Window.partitionBy("pf").orderBy($"chas".desc))).alias("rankpfs"),
      //      (row_number() over (Window.partitionBy("l").orderBy($"chac".desc))).alias("ranklc"),
      //      (row_number() over (Window.partitionBy("l").orderBy($"chas".desc))).alias("rankls"),
      $"chac",
      $"chas",
      $"pf"
    )

    //maleProductOrder.show(false)

    maleProductOrder.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      //val mutator = connection.getBufferedMutator(TableName.valueOf("dy:product-month-rank"))
      val mutator = connection.getBufferedMutator(TableName.valueOf("test:ljk"))
      part.foreach(line => {
        val title = "B"
        val pid = line(0).toString
        val label = line(1).toString
        val prov = line(2).toString
        val city = line(3).toString
        val age = line(4).toString
        val rankc = line(5).toString.toInt
        val ranks = line(6).toString.toInt
        val rankpfc = line(7).toString.toInt
        val rankpfs = line(8).toString.toInt
        val chac = line(9).toString.toLong
        val chas = line(10).toString.toLong
        val pflabel = line(11).toString

        if (rankc <= 10000) {
          val put = new Put((title + yesterdayDate + "不限c" + rankc).getBytes)
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("pi"), Bytes.toBytes(pid))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rankc))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chac"), Bytes.toBytes(chac))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chas"), Bytes.toBytes(chas))

          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("prov"), Bytes.toBytes(prov))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("city"), Bytes.toBytes(city))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("age"), Bytes.toBytes(age))

          mutator.mutate(put)
        }

        if (ranks <= 10000) {
          val put2 = new Put((title + yesterdayDate + "不限s" + ranks).getBytes)
          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("pi"), Bytes.toBytes(pid))
          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(ranks))
          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chac"), Bytes.toBytes(chac))
          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chas"), Bytes.toBytes(chas))

          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("prov"), Bytes.toBytes(prov))
          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("city"), Bytes.toBytes(city))
          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("age"), Bytes.toBytes(age))

          mutator.mutate(put2)
        }

        if (rankpfc <= 10000) {
          val put3 = new Put((title + yesterdayDate + pflabel + "c" + rankpfc).getBytes)
          put3.addColumn(Bytes.toBytes("r"), Bytes.toBytes("pi"), Bytes.toBytes(pid))
          put3.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rankpfc))
          put3.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chac"), Bytes.toBytes(chac))
          put3.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chas"), Bytes.toBytes(chas))

          put3.addColumn(Bytes.toBytes("r"), Bytes.toBytes("prov"), Bytes.toBytes(prov))
          put3.addColumn(Bytes.toBytes("r"), Bytes.toBytes("city"), Bytes.toBytes(city))
          put3.addColumn(Bytes.toBytes("r"), Bytes.toBytes("age"), Bytes.toBytes(age))

          mutator.mutate(put3)
        }

        if (rankpfs <= 10000) {
          val put4 = new Put((title + yesterdayDate + pflabel + "s" + rankpfs).getBytes)
          put4.addColumn(Bytes.toBytes("r"), Bytes.toBytes("pi"), Bytes.toBytes(pid))
          put4.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rankpfs))
          put4.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chac"), Bytes.toBytes(chac))
          put4.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chas"), Bytes.toBytes(chas))

          put4.addColumn(Bytes.toBytes("r"), Bytes.toBytes("prov"), Bytes.toBytes(prov))
          put4.addColumn(Bytes.toBytes("r"), Bytes.toBytes("city"), Bytes.toBytes(city))
          put4.addColumn(Bytes.toBytes("r"), Bytes.toBytes("age"), Bytes.toBytes(age))

          mutator.mutate(put4)
        }
      })
      mutator.close()
    })


    //过滤女性多的商品
    val femaleMaxProduct = producetJoinAwemeData.filter($"maleMax".equalTo(false))
    val femaleProductOrder = femaleMaxProduct.select($"pi",
      $"l",
      $"prov",
      $"city",
      $"age",
      (row_number() over (Window.orderBy($"chac".desc))).alias("rankc"),
      (row_number() over (Window.orderBy($"chas".desc))).alias("ranks"),
      (row_number() over (Window.partitionBy("pf").orderBy($"chac".desc))).alias("rankpfc"),
      (row_number() over (Window.partitionBy("pf").orderBy($"chas".desc))).alias("rankpfs"),
      //      (row_number() over (Window.partitionBy("l").orderBy($"chac".desc))).alias("ranklc"),
      //      (row_number() over (Window.partitionBy("l").orderBy($"chas".desc))).alias("rankls"),
      $"chac",
      $"chas",
      $"pf"
    )

    //femaleProductOrder.show(false)

    femaleProductOrder.foreachPartition(part => {
      val connection = ConnectionFactory.createConnection()
      //val mutator = connection.getBufferedMutator(TableName.valueOf("dy:product-month-rank"))
      val mutator = connection.getBufferedMutator(TableName.valueOf("test:ljk"))
      part.foreach(line => {
        //todo CX改回C
        val title = "CX"
        val pid = line(0).toString
        val label = line(1).toString
        val prov = line(2).toString
        val city = line(3).toString
        val age = line(4).toString
        val rankc = line(5).toString.toInt
        val ranks = line(6).toString.toInt
        val rankpfc = line(7).toString.toInt
        val rankpfs = line(8).toString.toInt
        val chac = line(9).toString.toLong
        val chas = line(10).toString.toLong
        val pflabel = line(11).toString

        if (rankc <= 10000) {
          val put = new Put((title + yesterdayDate + "不限c" + rankc).getBytes)
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("pi"), Bytes.toBytes(pid))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rankc))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chac"), Bytes.toBytes(chac))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chas"), Bytes.toBytes(chas))

          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("prov"), Bytes.toBytes(prov))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("city"), Bytes.toBytes(city))
          put.addColumn(Bytes.toBytes("r"), Bytes.toBytes("age"), Bytes.toBytes(age))

          mutator.mutate(put)
        }

        if (ranks <= 10000) {
          val put2 = new Put((title + yesterdayDate + "不限s" + ranks).getBytes)
          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("pi"), Bytes.toBytes(pid))
          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(ranks))
          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chac"), Bytes.toBytes(chac))
          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chas"), Bytes.toBytes(chas))

          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("prov"), Bytes.toBytes(prov))
          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("city"), Bytes.toBytes(city))
          put2.addColumn(Bytes.toBytes("r"), Bytes.toBytes("age"), Bytes.toBytes(age))

          mutator.mutate(put2)
        }

        if (rankpfc <= 10000) {
          val put3 = new Put((title + yesterdayDate + pflabel + "c" + rankpfc).getBytes)
          put3.addColumn(Bytes.toBytes("r"), Bytes.toBytes("pi"), Bytes.toBytes(pid))
          put3.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rankpfc))
          put3.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chac"), Bytes.toBytes(chac))
          put3.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chas"), Bytes.toBytes(chas))

          put3.addColumn(Bytes.toBytes("r"), Bytes.toBytes("prov"), Bytes.toBytes(prov))
          put3.addColumn(Bytes.toBytes("r"), Bytes.toBytes("city"), Bytes.toBytes(city))
          put3.addColumn(Bytes.toBytes("r"), Bytes.toBytes("age"), Bytes.toBytes(age))

          mutator.mutate(put3)
        }

        if (rankpfs <= 10000) {
          val put4 = new Put((title + yesterdayDate + pflabel + "s" + rankpfs).getBytes)
          put4.addColumn(Bytes.toBytes("r"), Bytes.toBytes("pi"), Bytes.toBytes(pid))
          put4.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rk"), Bytes.toBytes(rankpfs))
          put4.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chac"), Bytes.toBytes(chac))
          put4.addColumn(Bytes.toBytes("r"), Bytes.toBytes("chas"), Bytes.toBytes(chas))

          put4.addColumn(Bytes.toBytes("r"), Bytes.toBytes("prov"), Bytes.toBytes(prov))
          put4.addColumn(Bytes.toBytes("r"), Bytes.toBytes("city"), Bytes.toBytes(city))
          put4.addColumn(Bytes.toBytes("r"), Bytes.toBytes("age"), Bytes.toBytes(age))

          mutator.mutate(put4)
        }
      })
      mutator.close()
    })

    spark.stop()
  }

}
