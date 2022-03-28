package xiangmu.sql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import xiangmu.utils.LogSchema

import scala.reflect.io.Path

object Spark_XiangMu_GuangGao {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder().config(conf).appName("Log2Parquet").master("local[1]").getOrCreate()

    var sc = spark.sparkContext

    import spark.implicits._

    val lj: RDD[String] = sc.textFile("D:\\DaShuJu\\spark_XiangMu_01\\src\\main\\java\\xiangmu\\utils\\zidaun.txt")
    val dataFrame: DataFrame = spark.read.format("csv")
      //结构函数给他一个结构
      .schema(LogSchema.structType)
      .load("D:\\大华教育大数据课件\\练习题\\互联网广告\\互联网广告第一天\\123.log")
      .filter(_.length>=85)
      dataFrame.createTempView("log")
    var sql = "select provincename,cityname,count(*) as pccount from log group by provincename,cityname"
    val resDF: DataFrame = spark.sql(sql)
    //hadoop配置引用的是hadoop的文件配置
    val hadooppei: Configuration = sc.hadoopConfiguration
    // 文件系统对象用来进行删除系统文件的
    val xt: FileSystem = FileSystem.get(hadooppei)
    val shuchu = new fs.Path("D:\\大华教育大数据课件\\本地测试\\地区的排序设置\\ceshi")
    if (xt.exists(shuchu)){
      xt.delete(shuchu,true)
    }
//    resDF.coalesce(1).write.partitionBy("provincename","cityname").json("D:\\大华教育大数据课件\\本地测试\\地区的排序设置\\ceshi")
    resDF.coalesce(1).write.json("D:\\大华教育大数据课件\\本地测试\\地区的排序设置\\ceshi02")
    spark.stop()
    sc.stop()


  }
}
