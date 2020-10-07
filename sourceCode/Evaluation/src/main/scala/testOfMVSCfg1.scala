package main.scala

import org.apache.spark.sql.SparkSession

object testOfMVSCfg1 {


  case class Para(rank: Int, iteNum: Int, lambda: Double, alpha: Int, seed: Int)

  case class FinalResult(SRAt1: Double, SRAt3: Double, SRAt5: Double, PAt1: Double, PAt3: Double, PAt5: Double, RAt1: Double, RAt3: Double, RAt5: Double, MRRAt5: Double, nLiAt5: Double)


  case class metricsWithPara(SRAt1: Double, SRAt3: Double, SRAt5: Double, MRRAt5:Double, NDCGAt5: Double, rank: Int, iteNum: Int, lambda: Double, alpha: Int, seed: Int, repID: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("MVLCfg0")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("~/checkPoint")
    import spark.implicits._

    val pathRoot = "/home/cr/DataOfFocus/DataSetAll/"
    val pathRootNew = "/home/cr/Data/DataOfMF/"
    val pathRootForWrite = pathRootNew + "FinalResultsOfMFInClusterLevelTop5/" //cut off value == 5
    val pathRootForRead = pathRootNew + "searchWithParaPool/"
    val pathRootForParaPool = pathRootNew + "paraPool/"

    val pathRootForList = pathRootNew + "listOfPros/"


    val Type = Seq("Focus1/", "Focus2/")
    val cfg = Seq("1.1/", "1.2/", "2.1/", "2.2/")
    val DS = Seq("SH_S/", "SH_L/", "MV_S/", "MV_L/")


    val i = Type.head
    val j = cfg(1)
    val k = DS(2)


      val paraPool = sc.textFile(pathRootForParaPool).map(line => {
        val lineArray = line.split(",")
        Para(lineArray.head.toInt, lineArray(1).toInt, lineArray(2).toDouble, lineArray(3).toInt, lineArray(4).toInt)
      })





    def getFinalResult(pathOfGT: String, A: Array[String]): FinalResult = {
      val GT = sc.textFile(pathOfGT).map(line => {
        val lineArray = line.split("#")
        lineArray(1)
      })
      var countOfPosition = 1
      val sizeOfGT = GT.count().toInt
      //println(s"This is the size of GT $sizeOfGT")
      val arrayOfGT = GT.collect()
      //val sizeOfIntersection = sc.parallelize(recList.take(20)).intersection(GT).count()
      //val sizeOfIntersection = A.intersect(GT.collect()).length
      // println(s"This is sizeOfIntersection : $sizeOfIntersection")
      val range = 1 to sizeOfGT
      val arrayOfIdealGain = range.toArray.map(line => 1.0 / Math.pow(2, line - 1.0))

      var successRateAt1 = 0.0
      var successRateAt3 = 0.0
      var successRateAt5 = 0.0


      val hitAt1 = A.take(1).intersect(arrayOfGT).length
      val hitAt3 = A.take(3).intersect(arrayOfGT).length
      val hitAt5 = A.take(5).intersect(arrayOfGT).length
      //val hitAt10 = A.take(10).intersect(arrayOfGT).length
      //val hitAt15 = A.take(15).intersect(arrayOfGT).length
      val hitAt20 = A.take(20).intersect(arrayOfGT).length

      if (hitAt1 != 0) {
        successRateAt1 = 1.0
        successRateAt3 = 1.0
        successRateAt5 = 1.0

      }
      else if (hitAt3 != 0) {
        successRateAt3 = 1.0
        successRateAt5 = 1.0
      }
      else if (hitAt5 != 0) {
        successRateAt5 = 1.0

      }


      val pAt1 = hitAt1.toDouble / 1.0
      val pAt3 = hitAt3.toDouble / 3.0
      val pAt5 = hitAt5.toDouble / 5.0


      val rAt1 = hitAt1.toDouble / sizeOfGT.toDouble
      val rAt3 = hitAt3.toDouble / sizeOfGT.toDouble
      val rAt5 = hitAt5.toDouble / sizeOfGT.toDouble


      val arrayOfIndex = new Array[(String, Int)](hitAt20)
      var countOfArrayOfIndex = 0
      for (cand <- A.take(20)) {

        if (arrayOfGT.contains(cand)) {
          val indexOfCand = (cand, countOfPosition)
          arrayOfIndex(countOfArrayOfIndex) = indexOfCand
          countOfArrayOfIndex += 1
        }
        countOfPosition += 1
      }

      arrayOfIndex.foreach(println)

      val arrayOfPosition = arrayOfIndex.map(_._2)

      val arrayOfGain = arrayOfPosition.map(line => 1.0 / Math.pow(2, line - 1.0))


      //arrayOfZipedPosition.foreach(println)
      //println("这个是命中的位置")
      //arrayOfPosition.foreach(println)
      //println("这个是相应的指数折损率的Gain")
      //arrayOfGain.foreach(println)

      val gainAt5 = if (hitAt5 > 0) {
        arrayOfGain.take(hitAt5).sum
      } else {
        0.0
      }


      val idealDCGAt5 = arrayOfIdealGain.take(5).sum

      println("-------------------------------------------------")

      ///println(s"The gains are $gainAt5,$gainAt10,$gainAt20")
      // println(s"The idealGains are $idealDCGAt5,$idealDCGAt10,$idealDCGAt20")
      //println(s"The gainsLog are $gainLogAt5,$gainLogAt10,$gainLogAt20")
      //println(s"The idealGainsLog are $idealDCGLogAt5,$idealDCGLogAt10,$idealDCGLogAt20")
      val nAt5 = gainAt5 / idealDCGAt5
      //arrayOfZipedPosition.foreach(println)


      val RRAt5 = if (hitAt5 > 0) {
        1.0 / arrayOfPosition.head.toDouble
      } else {
        0.0
      }


      FinalResult(successRateAt1: Double, successRateAt3: Double, successRateAt5: Double, pAt1: Double, pAt3: Double, pAt5: Double, rAt1: Double, rAt3: Double, rAt5: Double, RRAt5: Double, nAt5: Double)


    }

    for (r <- 1 to 10) {
      //val r = 5
      val pathOfRounds = pathRootForList + i + j + k + "round" + r + "/"




        val metricsWithParameters = for (para <- paraPool.collect();
                                         repID <- 1 to 5) yield {

          val listOfPro = sc.textFile(pathOfRounds).map(_.split(",").head)


          val metricsOfOneRound = for (nameOfPro <- listOfPro.collect()) yield {


            val pathOfRecList = pathRootForRead + i + j + k + "round" + r +  "/recList/" + nameOfPro + "/" + para.rank.toString + "/" + para.iteNum.toString + "/" + para.lambda.toString + "/" + para.alpha.toString + "/" + para.seed.toString + "/" + repID + "/"
            val pathOfRawGT = pathRoot + i + j + k + "evaluation/round" + r + "/GroundTruth/" + nameOfPro
            val arrayOfRecList = sc.textFile(pathOfRecList).collect()
            getFinalResult(pathOfRawGT, arrayOfRecList)
          }
          metricsOfOneRound.toList.toDS().coalesce(1).write.mode("overwrite").csv(pathRootForWrite + i + j + k + "round" + r + "/metrics/" + para.rank.toString + "/" + para.iteNum.toString + "/" + para.lambda.toString + "/" + para.alpha.toString + "/" + para.seed.toString + "/" + repID + "/")
          val describe = metricsOfOneRound.toList.toDS.describe().coalesce(1)
          val pathOfDescribe = pathRootForWrite + i + j + k + "round" + r +   "/describe/" + para.rank.toString + "/" + para.iteNum.toString + "/" + para.lambda.toString + "/" + para.alpha.toString + "/" + para.seed.toString + "/" + repID + "/"
          describe.coalesce(1).write.mode("overwrite").csv(pathOfDescribe)
          println("This is the describe of " + i + j + k + "round" + r +  "/describe/" + para.rank.toString + "/" + para.iteNum.toString + "/" + para.lambda.toString + "/" + para.alpha.toString + "/" + para.seed.toString + "/" + repID + "/")
          describe.show(false)
          val describeStringRdd = describe.rdd.map(_.toSeq.toArray.map(_.toString))
          val meanArray = describeStringRdd.filter(_.head == "mean").first().drop(1).map(_.toDouble)
          metricsWithPara(meanArray.head, meanArray(1), meanArray(2), meanArray(9), meanArray(10), para.rank, para.iteNum, para.lambda, para.alpha, para.seed, repID)
        }
        val metricsWithParaDS = metricsWithParameters.toList.toDS().coalesce(1)
        val rankedResult = metricsWithParaDS.rdd.sortBy(_.SRAt1, false)
        rankedResult.toDS().coalesce(1).write.mode("overwrite").csv(pathRootForWrite + i + j + k + "round" + r + "/rankedResult")



    }


  }
}