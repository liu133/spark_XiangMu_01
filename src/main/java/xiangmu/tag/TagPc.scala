package xiangmu.tag

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagPc extends TagTrait {
  override def aa(ar: Any*): Map[String, Int] = {
    var map=Map[String,Int]()
    val ro: Row = ar(0).asInstanceOf[Row]
    val provincename: String = ro.getAs[String]("provincename")
    val cityname: String = ro.getAs[String]("cityname")
    if(StringUtils.isNotEmpty("provincename")){
      map+="zp"+provincename->1
    }else if(StringUtils.isNotEmpty("cityname")) {
      map+="zp"+cityname->1
    }
  map
  }
}
