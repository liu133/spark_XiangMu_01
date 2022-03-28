package xiangmu.sql

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import xiangmu.utils.LogSchema

import java.util.Properties

object spark_XiangMu_YeWu {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder().config(conf).appName("Log2Parquet").master("local[1]").getOrCreate()

    var sc = spark.sparkContext

    import spark.implicits._

    val lj: DataFrame = spark.read.format("csv")
      //结构函数给他一个结构
      .schema(LogSchema.structType)
      .load("E:\\大华教育大数据课件\\练习题\\互联网广告\\互联网广告第一天\\123.log")
      .filter(_.length>=85)
    lj.createTempView("log")

        val  sql02=
          """
            |select
            |provincename,cityname,
            |sum(case when requestmode =1 and processnode >=1 then 1 else 0 end) as ysqq,
            |sum(case when requestmode =1 and processnode >=2 then 1 else 0 end) as yxqq,
            |sum(case when requestmode =1 and processnode =3 then 1 else 0 end) as ggqq,
            |sum(case when adplatformproviderid >=100000 and iseffective =1 and isbilling=1 and isbid=1 and adorderid!=0 then 1 else 0 end) as cyjjs,
            |sum(case when adplatformproviderid >=100000 and iseffective =1 and isbilling=1 and iswin=1 then 1 else 0 end) as jjcgs,
            |sum(case when requestmode =2 and iseffective =1 then 1 else 0 end )as zss,
            |sum(case when requestmode =3 and iseffective =1 then 1 else 0 end )as djs,
            |sum(case when requestmode =2 and iseffective =1 and isbilling = 1 then 1 else 0 end )as mjzss,
            |sum(case when requestmode =3 and iseffective =1 and isbilling = 1 then 1 else 0 end )as mjdjs,
            |sum(case when iseffective =1 and isbilling = 1 and iswin =1  then (winprice*1.0)/1000 else 0 end )as xiaofei,
            |sum(case when iseffective =1 and isbilling = 1 and iswin =1  then (adpayment*1.0)/1000 else 0 end )as chengben
            |from log
            |group by provincename,cityname
            |""".stripMargin
    val frame: DataFrame = spark.sql(sql02)
    val lo: Config = ConfigFactory.load()
    val pro = new Properties()
    pro.setProperty("user",lo.getString("jdbc.user"))
    pro.setProperty("driver",lo.getString("jdbc.driver"))
    pro.setProperty("password",lo.getString("jdbc.password"))
    frame.write.mode(SaveMode.Overwrite).jdbc(lo.getString("jdbc.url"),lo.getString("jdbc.tableName2"),pro)
    spark.stop()
    sc.stop()
  }
}
