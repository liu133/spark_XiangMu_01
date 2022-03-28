package xiangmu.sql

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.HashMap
import scala.collection.mutable

object spark_XiangMu_RddSuan {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder().config(conf).appName("Log2Parquet").getOrCreate()

    var sc = spark.sparkContext


    val line: RDD[String] = sc.textFile("hdfs://192.168.84.143:8020/guang/123.log")
    val field: RDD[Array[String]] = line.map(_.split(",", -1))
    val proCityRDD: RDD[((String, String), Int)] = field.filter(_.length >= 85).map(arr => {
      var pro = arr(24)
      var city = arr(25)
      ((pro, city), 1)
    })
    //    proCityRDD.foreach(println)

    // 降维。
    val reduceRDD: RDD[((String, String), Int)] = proCityRDD.reduceByKey(_ + _)


    reduceRDD.cache()
    val num: Int = reduceRDD.map(x => {
      x._1._1
    }).distinct().count().toInt

    reduceRDD.sortBy(_._2).coalesce(1).partitionBy(new mappartition(num)).saveAsTextFile("hdfs://192.168.84.143:8020/guang/gao/")

    sc.stop()
  }

  class mappartition(val count: Int) extends Partitioner {
    private val map = mutable.Map[String, Int]()
    private var sm = -1

    override def numPartitions: Int = count

    override def getPartition(key: Any): Int = {
      val value: String = key.toString
//      println(s"value $value")
      val str: String = value.substring(1, value.indexOf(","))
      if (map.contains(str)) {
        map.getOrElse(str, sm)
      } else {
        sm = sm + 1
        map.put(str, sm)
        sm
      }

    }
  }
}
