import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Exercise01 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org")
          .setLevel(Level
            .OFF)

    val spark = SparkSession.builder()
                            .master("local")
                            .appName("DataFrameExamples")
                            .getOrCreate()

    val rdd = spark.sparkContext
                   .textFile("./dataset/Exercise01.txt")

    val header = rdd.first()

    val count = rdd.count()

    val fileDataWithoutHeaderFooter = rdd.filter(record => record != header)
                                         .zipWithIndex()
                                         .filter(row => row._2 < count - 2)

    fileDataWithoutHeaderFooter.foreach(e => println(e._1))

    val outputFIle = fileDataWithoutHeaderFooter.map(e => e._1)

    outputFIle.saveAsTextFile("./dataset/exercise01output")
  }

}
