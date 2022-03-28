package xiangmu.sql

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import xiangmu.utils.LogSchema

import java.util.Properties

object Spark_XiangMu_Mysql {
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
    // 编写sql语句。
    var sql = "select provincename,cityname,count(*) as pccount from log group by provincename,cityname"
    val resDF: DataFrame = spark.sql(sql)
    val lo: Config = ConfigFactory.load()
    val pro = new Properties()
    pro.setProperty("user",lo.getString("jdbc.user"))
    pro.setProperty("driver",lo.getString("jdbc.driver"))
    pro.setProperty("password",lo.getString("jdbc.password"))
    resDF.write.mode(SaveMode.Overwrite).jdbc(lo.getString("jdbc.url"),lo.getString("jdbc.tableName"),pro)
    spark.stop()
    sc.stop()



  }
}
