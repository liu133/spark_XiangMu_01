package xiangmu.sql

import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import xiangmu.bin.LogBean
import xiangmu.utils.Spark_XiangMu_RddYeWuGJ

object Spark_XiangMu_GuangBo {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder().config(conf).appName("Log2Parquet").master("local[1]").getOrCreate()

    var sc = spark.sparkContext

    import spark.implicits._
    val map: Map[String, String] = sc.textFile("E:\\大华教育大数据课件\\练习题\\互联网广告\\互联网广告第一天\\app_mappings.txt").map(line => {
      val arr: Array[String] = line.split("[:]", -1)
      (arr(0), arr(1))
    }).collect().toMap
    val guang: Broadcast[Map[String, String]] = sc.broadcast(map)
    val lj: RDD[String] = sc.textFile("E:\\大华教育大数据课件\\练习题\\互联网广告\\互联网广告第一天\\123.log")
    val qie: RDD[LogBean] = lj.map(_.split(",", -1)).filter(_.length >= 85).map(LogBean(_)).filter(x => {
      !x.appid.isEmpty
    })
    qie.map(ll=>{
      var appname: String = ll.appname
      if (appname==""||appname.isEmpty){
        appname=guang.value.getOrElse(ll.appid,"不明确")
      }

      val yewu: List[Double] = Spark_XiangMu_RddYeWuGJ.qqs(ll.requestmode, ll.processnode)

      (appname,yewu)
    }).reduceByKey((x,y)=>{
      x.zip(y).map(t=>t._1+t._2)
    }).foreach(println)



  }
}
