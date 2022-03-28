package xiangmu.tag

import org.apache.spark.sql.Row
import xiangmu.toos.SNTools

object TagDistrict extends TagTrait {
  override def aa(ar: Any*): Map[String, Int] = {
    var  map=Map[String,Int]()
    val row: Row = ar(0).asInstanceOf[Row]
    var long: String = row.getAs[String]("long")
    var lat: String = row.getAs[String]("lat")
    if(lat.toDouble > 3 && lat.toDouble < 54 && long.toDouble > 73 && long.toDouble < 136){
      //苏州街,中关村,万泉河	116.3161200000	39.9850750000
//      if (lat.toDouble > 3 && lat.toDouble < 54 && long.toDouble > 73 && long.toDouble < 136) {
//        println(str)
//        val quan: String = BaiDuRegionAPI.getBusiness(str)
//        resultMap += "Q" + quan -> 1
//      }
    val str: String = SNTools.getBusiness((lat + "," + long))
      print(str+"\t"+long+"\t"+lat)
    map+="NC"+ str ->1
    }else{

    }



    map
  }
}
