package xiangmu.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis
import xiangmu.bin.LogBean
import xiangmu.utils.{RedisUtil, Spark_XiangMu_RddYeWuGJ}

object Spark_XiangMu_RedisYeWu {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder().config(conf).appName("Log2Parquet").master("local[1]").getOrCreate()

    var sc = spark.sparkContext



    val lj: RDD[String] = sc.textFile("E:\\大华教育大数据课件\\练习题\\互联网广告\\互联网广告第一天\\123.log")
    val qie: RDD[LogBean] = lj.map(_.split(",", -1)).filter(_.length >= 85).map(LogBean(_)).filter(x => {
      !x.appid.isEmpty
    })
    qie.mapPartitions(it=>{
      val jedis: Jedis = RedisUtil.getJedis
      val ss: Iterator[(String, List[Double])] = it.map(x => {
        var appname: String = x.appname
        jedis.get(x.appid)
        if (appname == "" || appname.isEmpty) {
          if (jedis.get(x.appid) != null) {
            appname = jedis.get(x.appid)
          } else {
            appname = "不知道"
          }
        }
        val yewu: List[Double] = Spark_XiangMu_RddYeWuGJ.qqs(x.requestmode, x.processnode)
        val cyjjs: List[Double] = Spark_XiangMu_RddYeWuGJ.cyjjs(x.adplatformproviderid, x.iseffective, x.isbilling, x.isbid, x.iswin, x.adorderid)
        val ggzss: List[Double] = Spark_XiangMu_RddYeWuGJ.ggzss(x.requestmode, x.iseffective)
        val mjzss: List[Double] = Spark_XiangMu_RddYeWuGJ.mjzss(x.requestmode, x.iseffective, x.isbilling)
        val ggxfs: List[Double] = Spark_XiangMu_RddYeWuGJ.ggxfs(x.adplatformproviderid, x.iseffective, x.isbilling, x.iswin, x.winprice, x.adpayment)
        (appname, yewu ++ cyjjs ++ ggzss ++ mjzss ++ ggxfs)
      })
       ss
    }).reduceByKey((x,y)=>{
      x.zip(y).map(t=>t._1+t._2)
    }).foreach(println)

  }
}
