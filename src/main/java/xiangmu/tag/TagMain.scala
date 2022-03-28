package xiangmu.tag

import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import xiangmu.utils.TagUtil

import java.util.UUID

object TagMain {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder().config(conf).appName("Log2Parquet").master("local[1]").getOrCreate()
    var sc = spark.sparkContext
    import spark.implicits._
    val df: DataFrame = spark.read.parquet("E:\\大华教育大数据课件\\练习题\\互联网广告\\互联网广告第一天\\output1")
    //    df.createTempView("lo")
    //    var  sql="select provincename,long,lat from lo"
    //    spark.sql(sql).show()
    val map: Map[String, String] = sc.textFile("E:\\大华教育大数据课件\\练习题\\互联网广告\\互联网广告第一天\\app_mappings.txt").map(line => {
      val strs: Array[String] = line.split("[:]", -1)
      (strs(0), strs(1))
    }).collect().toMap
    val mapGB: Broadcast[Map[String, String]] = sc.broadcast(map)

    val stopword: Map[String, Int] = sc.textFile("E:\\大华教育大数据课件\\练习题\\互联网广告\\互联网广告第一天\\stopword.txt")
      .map((_, 0)).collect().toMap
    val stopwordGB: Broadcast[Map[String, Int]] = sc.broadcast(stopword)


    val TagDS: Dataset[(String, List[(String, Int)])] = df.where(TagUtil.tagUserIdFilterParam).map(row => {
      val stringToInt: Map[String, Int] = TagDistrict.aa(row)
      val stringToInt1: Map[String, Int] = TagPc.aa(row)
      val stringToInt2: Map[String, Int] = TagAds.aa(row)
      val stringToInt3: Map[String, Int] = TagApp.aa(row,mapGB.value)
      val stringToInt4: Map[String, Int] = TagDistrict.aa(row)
      val stringToInt5: Map[String, Int] = TagKey.aa(row, stopwordGB.value)

      if (TagUtil.getUserId(row).size > 0) {
        (TagUtil.getUserId(row)(0), (stringToInt ++ stringToInt1++stringToInt2++stringToInt3++stringToInt4++stringToInt5).toList)
      } else {
        (UUID.randomUUID().toString.substring(0, 6), (stringToInt ++ stringToInt1++stringToInt2++stringToInt3++stringToInt4++stringToInt5).toList)
      }
    })

    TagDS.rdd.reduceByKey((list1, list2) => {
      (list1 ++ list2).groupBy(_._1).mapValues(_.foldLeft(0)(_ + _._2)).toList
    })
//      .foreach(println)
          .saveAsTextFile("E:\\大华教育大数据课件\\本地测试\\广告测试\\ce2")
  }
}
