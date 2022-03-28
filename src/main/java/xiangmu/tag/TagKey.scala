package xiangmu.tag

import org.apache.spark.sql.Row

object TagKey extends TagTrait {
  override def aa(ar: Any*): Map[String, Int] = {
    var map: Map[String, Int] = Map[String, Int]()
    val row: Row = ar(0).asInstanceOf[Row]
    val row2: Map[String, Int] = ar(1).asInstanceOf[Map[String, Int]]
    val keywords: String = row.getAs[String]("keywords")
    val str: Array[String] = keywords.split("\\|")
    str.filter(skk=>skk.length >= 3 &&
      !row2.contains(skk)&&
      skk.length>=8).foreach(skk=>map+="kk"+skk->1)



    map


  }
}
