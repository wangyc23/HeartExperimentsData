package main.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object showTheSizeOfSimDOfOut {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("showSizeOfSimD")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("~/checkPoint1")

    val pathRoot = "E:/Experiments/DataSetAll/"

    def readSimD(Type: String, cfg: String, DS: String, round: Int, nameOfPro: String): RDD[String] = {
      val pathOfSimD = pathRoot + Type + cfg + DS + "evaluation/round" + round + "/TestingInvocations/simD" + nameOfPro
      sc.textFile(pathOfSimD)
    }


    val Type = Seq("Focus1/", "Focus2/")
    val cfg = Seq("1.1/", "1.2/", "2.1/", "2.2/")
    val DS = Seq("SH_S/", "SH_L/", "MV_S/", "MV_L/")
    val listOfProsAll = pathRoot + "listOfPros/"
    val i = Type.head
    val j = cfg(3)
    val k = DS(3)
    val r = 1 to 10
    val numberOfRound = r.head
    for (numberOfRound <- r) {
      println(s"This is the information of $i $j $k round $numberOfRound ")


      //val nameOfPro = "g_jwi-2.2.3.jar.txt"

      val listOfPro = sc.textFile(listOfProsAll + "Out/" + i + j + k + "round" + numberOfRound).collect()
      //val listOfProFiltered = sc.textFile(listOfProsAll + "Filtered/" + i + j + k + "round" + numberOfRound).collect()

      for (nameOfPro <- listOfPro) {
        //从simD中读取信息
        val rawSimD = readSimD(i, j, k, numberOfRound, nameOfPro)
        println("the size of simD is %d".format(rawSimD.count()))
      }
    }
  }
}
