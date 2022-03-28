package xiangmu.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import xiangmu.bin.LogBean

object Spark_XiangMu_GuangGao02 {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 将自定义对象进行kryo序列化
    conf.registerKryoClasses(Array(classOf[LogBean]))
    val spark = SparkSession.builder().config(conf).appName("Spark_XiangMu_GuangGao02").master("local[1]").getOrCreate()
    var sc = spark.sparkContext
    import spark.implicits._
    val shuru: RDD[Array[String]] = sc.textFile("D:\\大华教育大数据课件\\练习题\\互联网广告\\互联网广告第一天\\123.log").map(_.split(",", -1)).filter(_.length >= 85)
    //实现bin的类
    val bin: RDD[LogBean] = shuru.map(LogBean(_))
    val df: DataFrame = spark.createDataFrame(bin)
    df.write.parquet("D:\\大华教育大数据课件\\本地测试\\地区的排序设置\\ceshi03")
    spark.stop()
    sc.stop()
  }
}
