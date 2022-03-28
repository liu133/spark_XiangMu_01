package xiangmu.tag

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagApp extends TagTrait {
  override def aa(ar: Any*): Map[String, Int] = {
    var map: Map[String, Int] = Map[String, Int]()
    val row: Row = ar(0).asInstanceOf[Row]
    val row2: Map[String, String] = ar(1).asInstanceOf[Map[String, String]]

    val appname: String= row.getAs[String]("appname")
    val appid: String = row.getAs[String]("appid")
    val adplatformproviderid: Int = row.getAs[Int]("adplatformproviderid")
    if (StringUtils.isEmpty(appname)){
      row2.contains("appid")match{
        case true=> map += "app"+row2.getOrElse("appid","未知")->1
      }
    }else{
      map+="app"+appname->1
    }
    map+="CN"+adplatformproviderid->1
    map

  }
}
