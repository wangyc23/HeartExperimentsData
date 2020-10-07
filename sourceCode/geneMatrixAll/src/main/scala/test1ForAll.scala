package main.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object test1ForAll {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("geneMatrixTest")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("~/checkPoint")


    val pathRootForRead = "~~~~" //the path of the data

    val pathRootForWrite = pathRootForRead + "newMatrixDataDistinct/"


    val Type = Seq("Focus1/", "Focus2/")
    val cfg = Seq("1.1/", "1.2/", "2.1/", "2.2/")
    val DS = Seq("SH_S/", "SH_L/", "MV_S/", "MV_L/")
    val listOfProsAll = pathRootForRead + "listOfPros/"
    val i = Type.head
    val j = cfg(1)
    val k = DS(1)
    val r = 1 to 10
    //val numberOfRound = r.head
    for (numberOfRound <- r){
    val pathOfRawData = pathRootForRead + i + j + k
    println($"This is the information of $i $j $k round $numberOfRound ")


    val pathOfData = pathOfRawData + "evaluation/"
    //val nameOfPro = "g_jwi-2.2.3.jar.txt"
    val pathOfTestInvo = pathOfData + "round" + numberOfRound.toString + "/TestingInvocations/"
    val pathOfSimPro = pathOfData + "round" + numberOfRound.toString + "/Similarities/"

    val listOfPro = sc.textFile(listOfProsAll + i + j + k + "round" + numberOfRound).collect()
    //val listOfProFiltered = sc.textFile(listOfProsAll + "Filtered/" + i + j + k + "round" + numberOfRound).collect()

    for (nameOfPro <- listOfPro) {

      val rawActiveDec = sc.textFile(pathOfTestInvo.concat(nameOfPro))

      case class ActiveDecAndTesInv(nameOfAD: String, nameOfTI: String)

      val ADTI = rawActiveDec.map(l => {
        val ADTIArray = l.split("#")
        ActiveDecAndTesInv(ADTIArray(0), ADTIArray(1))
      })

      // println("This is the active declaration and test invocations")
      //val NameOfAD = ADTI.first().nameOfAD
      // println("*************************************************************")
      // println("The name of the active declaration is: ")
      //println(NameOfAD )

      val Query = ADTI.map(l => l.nameOfTI)


      val rawSimPro = sc.textFile(pathOfSimPro + nameOfPro)
      val simPro = rawSimPro.take(2).map(line => {
        val SimProArray = line.split("\t")
        SimProArray(1)
      })


      val simPro0 = simPro(0)
      val simPro1 = simPro(1)

      val rawData0 = sc.textFile(pathOfRawData + simPro0)

      val data0 = rawData0.map(line => {
        val dataArray = line.split("#")
        (dataArray(0), dataArray(1))
      })

      val rawData1 = sc.textFile(pathOfRawData + simPro1)

      val data1 = rawData1.map(line => {
        val dataArray = line.split("#")
        (dataArray(0), dataArray(1))
      })


      //从simD中读取信息
      val rawSimD = sc.textFile(pathOfTestInvo.concat("simD".concat(nameOfPro)))
      case class PDNS(simProject: Int, simDeclar: Int, nameOfSimDec: String, similarity: Double)
      val numberOfSimD = Seq(rawSimD.count().toInt, 5).min
      val SimD = rawSimD.take(numberOfSimD).map(l => {
        val simDArray = l.split(" ")
        PDNS(simDArray(0).toInt, simDArray(1).toInt, simDArray(2), simDArray(3).toDouble)
      })

      val SimDPair = SimD.zipWithIndex


      val listOfSimD = SimDPair.map(line => {
        if (line._1.simProject == 0) {
          line._1.nameOfSimDec.concat("@").concat(simPro0)


        } else {
          line._1.nameOfSimDec.concat("@").concat(simPro1)
        }
      })


      val IndexForDec = sc.parallelize(listOfSimD).zipWithIndex().collectAsMap()
      val IndexAndName = SimDPair.map(line => {
        (line._1.simProject, line._1.nameOfSimDec)
      })

      val arrayOfInvOfSimD = new Array[RDD[(String, String)]](numberOfSimD)
      for (countOfSimD <- 0 until numberOfSimD) {
        val invOfDec = {
          if (IndexAndName(countOfSimD)._1 == 0) {
            data0.filter(_._1.equals(IndexAndName(countOfSimD)._2)).map(line => {
              (line._1.concat("@").concat(simPro0), line._2)
            })
          } else {
            data1.filter(_._1.equals(IndexAndName(countOfSimD)._2)).map(line => {
              (line._1.concat("@").concat(simPro1), line._2)
            })
          }
        }
        arrayOfInvOfSimD(countOfSimD) = invOfDec.distinct()
      }


      val arrayOfCand = arrayOfInvOfSimD.map(_.map(_._2))


      val candTemp = arrayOfCand.reduce(_.union(_))
      val candFinal = candTemp.union(Query)

      //println("this is the size of candTemp")
      //println(candTemp.count )
      val invOfDecTemp = arrayOfInvOfSimD.reduce(_.union(_))


      val candidates = candFinal.distinct
      println("This is the size of the final candidates")
      println(candidates.count())


      val zipedCand = candidates.zipWithIndex()
      val indexOfCand = zipedCand.collectAsMap()



      val zipedCandInverse = zipedCand.map(line => {
        (line._2, line._1)
      })


      val tuple = invOfDecTemp.map(line => {
        val target1 = IndexForDec(line._1)
        val target2 = indexOfCand(line._2)
        (target1, target2, 1)

      })


      val activeTuple = ADTI.map(line => {
        (numberOfSimD.toLong, indexOfCand(line.nameOfTI), 1)
      })
      println("This is ActiveTuple")
      activeTuple.collect.foreach(println)

      val completeTuple = tuple.union(activeTuple)
      println("This is size for completeTuple")
      println(completeTuple.count())


      val pathOfMatrix = pathRootForWrite + i + j + k + "round" + numberOfRound + "/"


      val indexForDecForSave = sc.parallelize(IndexForDec.toSeq).toDF().coalesce(1)
      indexForDecForSave.write.mode("overwrite").option("sep", "\t").csv(pathOfMatrix + "mapOfDec.csv/" + nameOfPro)

      val QueryForSave = Query.toDF().coalesce(1)
      QueryForSave.write.mode("overwrite").option("sep", "\t").csv(pathOfMatrix + "Query.csv/" + nameOfPro)

      val mapOfAPI = zipedCandInverse.toDF().coalesce(1)
      mapOfAPI.write.mode("overwrite").option("sep", "\t").csv(pathOfMatrix + "mapOfAPI.csv/" + nameOfPro)

      val RT = completeTuple.toDF().coalesce(1)
      RT.write.mode("overwrite").option("sep", "\t").csv(pathOfMatrix + "RT.csv/" + nameOfPro)





    }

  }

  }
}
