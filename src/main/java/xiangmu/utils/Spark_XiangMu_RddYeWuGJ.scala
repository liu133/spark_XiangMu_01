package xiangmu.utils

object Spark_XiangMu_RddYeWuGJ {
  /**
   * 用来做业务的工具类,对应sql包里面的业务类
   */
  def qqs(requestMode: Int, processNode: Int) = {
    if (requestMode == 1 && processNode >= 1) {
      List[Double](1, 0, 0)
    } else if (requestMode == 1 && processNode >= 2) {
      List[Double](1, 1, 0)
    } else if (requestMode == 1 && processNode >= 3) {
      List[Double](1, 1, 1)
    } else {
      List[Double](0, 0, 0)
    }
  }
    def cyjjs(adid: Int, ecTive: Int, Bill: Int, bid: Int, isWin: Int, orderId: Int) = {
      if (ecTive == 1 && Bill == 1 && bid == 1 && orderId != 0 && adid >= 100000) {
        List[Double](1, 0)
      } else if (ecTive == 1 && Bill == 1 && adid >= 100000 && isWin == 1) {
        List[Double](1, 1)
      } else {
        List[Double](0, 0)
      }
    }

    def ggzss(rMode: Int, ecTive: Int) = {
      if (rMode == 2 && ecTive == 1) {
        List[Double](1, 0)
      } else if (rMode == 3 && ecTive == 1) {
        List[Double](1, 1)
      } else {
        List[Double](0, 0)
      }
    }

    def mjzss(rMode: Int, ecTive: Int, Bill: Int): List[Double] = {
      if (rMode == 2 && ecTive == 1 && Bill == 1) {
        List[Double](1, 0)
      } else if (rMode == 3 && ecTive == 1 && Bill == 1) {
        List[Double](1, 1)
      } else {
        List[Double](0, 0)
      }
    }

    def ggxfs(adid: Int, ecTive: Int, Bill: Int, isWin: Int, winPrice: Double, adPatyment: Double): List[Double] = {
      if (ecTive == 1 && Bill == 1 && isWin == 1 && adid >= 100000) {
        List[Double](winPrice * 1.0 / 1000, adPatyment * 1.0 / 1000)
      } else {
        List[Double](0, 0)
      }
    }

}
