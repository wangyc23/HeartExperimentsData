package main.scala

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession

object searchOverParaPool {

  case class Metrics(SRAt1: Double, SRAt5: Double, SRAt10: Double, SRAt15: Double, SRAt20: Double,
                     PAt1: Double, PAt5: Double, PAt10: Double, PAt15: Double, PAt20: Double,
                     RAt1: Double, RAt5: Double, RAt10: Double, RAt15: Double, RAt20: Double,
                     MRR: Double, MAP: Double)

  case class Para(rank: Int, iteNum: Int, lambda: Double, alpha: Int, seed: Int)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("searchOverParaPool")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    sc.setCheckpointDir("~/checkpoint1")
    sc.setLogLevel("ERROR")
   

    val pathRoot = "/home/cr/DataOfFocus/DataSetAll/"
    val pathRootNew = "/home/cr/Data/DataOfMF/"
    val pathRootForParaPool = pathRootNew + "paraPool/"
    val pathRootForWrite = pathRootNew + "searchWithParaPool/"
    val pathRootForList = pathRootNew + "listOfPros/"


   

    val Type = Seq("Focus1/", "Focus2/")
    val cfg = Seq("1.1/", "1.2/", "2.1/", "2.2/")
    val DS = Seq("SH_S/", "SH_L/", "MV_S/", "MV_L/")

    def getMetricsAndMAPMRR(pathOfGT: String, A: Array[String]): Metrics = {
      val GT = sc.textFile(pathOfGT).map(line => {
        val lineArray = line.split("#")
        lineArray(1)
      })
      val recList = sc.parallelize(A).coalesce(1)
      var countOfPosition = 1
      val sizeOfGT = GT.count()
      val arrayOfGT = GT.collect()
      val sizeOfIntersection = sc.parallelize(recList.take(20)).intersection(GT).count()

      var successRateAt1 = 0.0
      var successRateAt5 = 0.0
      var successRateAt10 = 0.0
      var successRateAt15 = 0.0
      var successRateAt20 = 0.0

      val hitAt1 = sc.parallelize(A.take(1)).intersection(GT)
      val hitAt5 = sc.parallelize(A.take(5)).intersection(GT)
      val hitAt10 = sc.parallelize(A.take(10)).intersection(GT)
      val hitAt15 = sc.parallelize(A.take(15)).intersection(GT)
      val hitAt20 = sc.parallelize(A).intersection(GT)
      if (hitAt1.count() != 0) {
        successRateAt1 = 1.0
        successRateAt5 = 1.0
        successRateAt10 = 1.0
        successRateAt15 = 1.0
        successRateAt20 = 1.0
      }
      else if (hitAt5.count() != 0) {
        successRateAt5 = 1.0
        successRateAt10 = 1.0
        successRateAt15 = 1.0
        successRateAt20 = 1.0
      }
      else if (hitAt10.count() != 0) {
        successRateAt10 = 1.0
        successRateAt15 = 1.0
        successRateAt20 = 1.0
      }
      else if (hitAt15.count() != 0) {
        successRateAt15 = 1.0
        successRateAt20 = 1.0
      }
      else if (hitAt20.count() != 0)
        successRateAt20 = 1.0


      val pAt1 = hitAt1.count.toDouble / 1.0
      val pAt5 = hitAt5.count.toDouble / 5.0
      val pAt10 = hitAt10.count.toDouble / 10.0
      val pAt15 = hitAt15.count.toDouble / 15.0
      val pAt20 = hitAt20.count.toDouble / 20.0

      val rAt1 = hitAt1.count.toDouble / sizeOfGT.toDouble
      val rAt5 = hitAt5.count.toDouble / sizeOfGT.toDouble
      val rAt10 = hitAt10.count.toDouble / sizeOfGT.toDouble
      val rAt15 = hitAt15.count.toDouble / sizeOfGT.toDouble
      val rAt20 = hitAt20.count.toDouble / sizeOfGT.toDouble


      val arrayOfIndex = new Array[(String, Int)](sizeOfIntersection.toInt)
      var countOfArrayOfIndex = 0
      for (cand <- recList.take(20)) {

        if (arrayOfGT.contains(cand)) {
          val indexOfCand = (cand, countOfPosition)
          arrayOfIndex(countOfArrayOfIndex) = indexOfCand
          countOfArrayOfIndex += 1
        }
        countOfPosition += 1
      }


      val sizeOfHits = arrayOfIndex.length
      var RR = 0.0
      var AP = 0.0
      val arrayOfPrecision = new Array[Double](sizeOfHits)
      if (sizeOfHits > 0) {
        val indexOfTheFirstHit = arrayOfIndex.head._2.toDouble
        RR = 1.0 / indexOfTheFirstHit

        //接下来计算relaxed MAP
        for (countOfHits <- 1 to sizeOfHits) {
          val position = arrayOfIndex(countOfHits - 1)._2.toDouble
          arrayOfPrecision(countOfHits - 1) = countOfHits.toDouble / position
        }
        AP = arrayOfPrecision.sum / sizeOfGT.toDouble

      }
      Metrics(successRateAt1, successRateAt5, successRateAt10, successRateAt15, successRateAt20, pAt1, pAt5, pAt10, pAt15, pAt20, rAt1, rAt5, rAt10, rAt15, rAt20, RR, AP)


    }

    val i = Type.head
    val j = cfg.head
    val k = DS(2)

      val paraPool = sc.textFile(pathRootForParaPool).map(line => {
        val lineArray = line.split(",")
        Para(lineArray.head.toInt, lineArray(1).toInt, lineArray(2).toDouble, lineArray(3).toInt, lineArray(4).toInt)
      })


    for(r <- 1 to 10){


    val pathOfRound = pathRootForList + i + j + k + "round" + r + "/"



      for (para <- paraPool.collect();
           repID <- 1 to 5) {

        val listOfPro = sc.textFile(pathOfRound)


        val metricsOfOneCluster = for (nameOfPro <- listOfPro.collect()) yield {

          val pathOfMatrix = pathRootNew + "newMatrixDataDistinct/" + i + j + k + "round" + r + "/"
          val pathOfMap = pathOfMatrix + "mapOfAPI.csv/" + nameOfPro + "/"
          val pathOfDec = pathOfMatrix + "mapOfDec.csv/" + nameOfPro + "/"

          val rawMapOfAPI = sc.textFile(pathOfMap)
          val mapOfAPIRdd = rawMapOfAPI.map(line => {
            val lineArray = line.split("\t")
            (lineArray.head.toInt, lineArray(1))
          })

          val map = mapOfAPIRdd.collectAsMap()


          val pathOfQuery = pathOfMatrix + "Query.csv/" + nameOfPro + "/"
          val Query = sc.textFile(pathOfQuery)


          val pathOfRT = pathOfMatrix + "RT.csv/" + nameOfPro + "/"
          val RT = sc.textFile(pathOfRT)
          //RT.printSchema()
          //RT.show(false)


          val ratingString = RT.map(line => {
            val lineArray = line.split("\t")
            (lineArray.head, lineArray(1), lineArray(2))
          })


          val ratingTuple = ratingString.map(line => {
            Rating(line._1.toInt, line._2.toInt, line._3.toDouble)
          })


          val start = System.currentTimeMillis()
          val RSModel = new ALS()
            .setIterations(para.iteNum)
            .setBlocks(-1)
            .setImplicitPrefs(true)
            .setNonnegative(true)
            .setRank(3)
            .setSeed(para.seed)
            .setLambda(para.lambda)
            .setAlpha(para.alpha)
            .run(ratingTuple)

          val end1 = System.currentTimeMillis()
          val costTime1 = end1 - start

          println("Time cost for MF:" + costTime1 / 1000 + "s")

          val rawDec = sc.textFile(pathOfDec)
          val uid = rawDec.count().toInt


          val lengthRec = 24
          val topKRecs = RSModel.recommendProducts(uid, lengthRec)
          //println(topKRecs.mkString("\n"))


          val topKRecsWithString = topKRecs.map(line => {
            (line.user, map(line.product), line.rating)
          })

          //println("This the top-k recommendation list with the true name")

          //topKRecsWithString.foreach(println)


          val rankedList = topKRecsWithString.map(line => {
            line._2
          })

          val realRecList = rankedList.diff(Query.collect()).take(20)

          val realRecListForSave = sc.parallelize(realRecList).coalesce(1)
          realRecListForSave.saveAsTextFile(pathRootForWrite + i + j + k + "round" + r +  "/recList/" + nameOfPro + "/" + 3.toString + "/"
            + para.iteNum.toString + "/" + para.lambda.toString + "/" + para.alpha.toString + "/" + para.seed.toString + "/" + repID + "/")
          val pathForSaveModel = pathRootForWrite + i + j + k + "round" + r  + "/model/" + nameOfPro + "/" + 3.toString + "/" + para.iteNum.toString + "/" + para.lambda.toString + "/" + para.alpha.toString + "/" + para.seed.toString + "/" + repID + "/"
          RSModel.save(sc, pathForSaveModel)

          val pathOfRawGT = pathRoot + i + j + k + "evaluation/round" + r + "/GroundTruth/" + nameOfPro
          getMetricsAndMAPMRR(pathOfRawGT, realRecList)
        }
        metricsOfOneCluster.toList.toDF().coalesce(1).write.mode("overwrite").csv(pathRootForWrite + i + j + k + "round" + r + "/"+ "/metrics/" + 3.toString + "/" + para.iteNum.toString + "/" + para.lambda.toString + "/" + para.alpha.toString + "/" + para.seed.toString + "/" + repID + "/")
        val describe = metricsOfOneCluster.toList.toDF.describe().coalesce(1)
        val pathOfDescribe = pathRootForWrite + i + j + k + "round" + r + "/"  + "/describe/" + 3.toString + "/" + para.iteNum.toString + "/" + para.lambda.toString + "/" + para.alpha.toString + "/" + para.seed.toString + "/" + repID + "/"
        describe.coalesce(1).write.mode("overwrite").csv(pathOfDescribe)
        println("This is the describe of " + i + j + k + "round" + r + "/" +"/describe/" + 3.toString + "/" + para.iteNum.toString + "/" + para.lambda.toString + "/" + para.alpha.toString + "/" + para.seed.toString + "/" + repID + "/")
        describe.show(false)
      }

    }


  }

}
