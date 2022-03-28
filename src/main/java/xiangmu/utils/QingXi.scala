package xiangmu.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object QingXi {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf()
      .setMaster("local[1]")
      .setAppName("shuffle"))
    val sj: RDD[String] = sc.textFile("D:\\DaShuJu\\spark_XiangMu_01\\src\\main\\java\\xiangmu\\utils\\zidaun.txt")
  var  sum=0
    sj.map(x=>{
      val qie: Array[String] = x.split(":")
    print(s"_c${sum} ${qie(0)} :String,")
    sum=sum+1
    }).foreach(println)

  }
}
