package xiangmu.tag

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagAds extends TagTrait {
  override def aa(ar: Any*): Map[String, Int] = {
    var map=Map[String,Int]()
    val row: Row = ar(0).asInstanceOf[Row]
    val adspacetype: Int = row.getAs[Int]("adspacetype")
    if (adspacetype>9){
      map+="LC"+adspacetype->1
    }else{
      map+="LC0"+adspacetype->1
    }
    val adspacetypename: String = row.getAs[String]("adspacetypename")
    if(StringUtils.isNotEmpty(adspacetypename)){
      map+="LN"+adspacetypename->1
    }
    map


  }
}
