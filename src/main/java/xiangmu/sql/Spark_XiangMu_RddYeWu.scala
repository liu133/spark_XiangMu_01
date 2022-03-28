package xiangmu.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import xiangmu.utils.{LogSchema, Spark_XiangMu_RddYeWuGJ}

object Spark_XiangMu_RddYeWu {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder().config(conf).appName("Log2Parquet").master("local[1]").getOrCreate()

    var sc = spark.sparkContext

    import spark.implicits._

    val df: DataFrame = spark.read.format("csv")
      //结构函数给他一个结构
      .schema(LogSchema.structType)
      .load("E:\\大华教育大数据课件\\练习题\\互联网广告\\互联网广告第一天\\123.log")
      .filter(_.length>=85)
    val dimRDD: Dataset[((String, String), List[Double])] =
      df.map(row => {
        val adplatformproviderid: Int = row.getAs[Int]("adplatformproviderid")
        val requestMode: Int = row.getAs[Int]("requestmode")
        val processNode: Int = row.getAs[Int]("processnode")
        val iseffective: Int = row.getAs[Int]("iseffective")
        val isbilling: Int = row.getAs[Int]("isbilling")
        val isbid: Int = row.getAs[Int]("isbid")
        val iswin: Int = row.getAs[Int]("iswin")
        val adorderid: Int = row.getAs[Int]("adorderid")
        val winprice: Double = row.getAs[Double]("winprice")
        val adpayment: Double = row.getAs[Double]("adpayment")
        val province: String = row.getAs[String]("provincename")
        val cityname: String = row.getAs[String]("cityname")
        val appname: String = row.getAs[String]("appname")


        val qqs: List[Double] = Spark_XiangMu_RddYeWuGJ.qqs(requestMode, processNode)
        val cyjjs: List[Double] = Spark_XiangMu_RddYeWuGJ.cyjjs(adplatformproviderid, iseffective, isbilling, isbid, iswin, adorderid)
        val ggzss: List[Double] = Spark_XiangMu_RddYeWuGJ.ggzss(requestMode, iseffective)
        val mjzss: List[Double] = Spark_XiangMu_RddYeWuGJ.mjzss(requestMode, iseffective, isbilling)
        val ggxfs: List[Double] = Spark_XiangMu_RddYeWuGJ.ggxfs(adplatformproviderid, iseffective, isbilling, iswin, winprice, adpayment)
        ((province, cityname), qqs ++ cyjjs ++ ggzss ++ mjzss ++ ggxfs)
      })
        dimRDD.rdd.reduceByKey((list1,list2)=>{
          list1.zip(list2).map(t=>t._1+t._2)
        }).foreach(println)


  }
}
