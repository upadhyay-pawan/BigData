import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.util.Random


object Exercise03 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org")
          .setLevel(Level
            .OFF)

    val spark = SparkSession.builder()
                            .master("local")
                            .appName("DataFrameExamples")
                            .getOrCreate()

    val KB = 1024
    val MAX = 20
//    val random = scala
//      .util
//      .Random
    //    val kiloBytes = List(KB * random.nextInt(MUL),
    //      KB * random.nextInt(MUL),
    //      KB * random.nextInt(MUL),
    //      KB * random.nextInt(MUL),
    //      KB * random.nextInt(MUL),
    //      KB * random.nextInt(MUL),
    //      KB * random.nextInt(MUL),
    //      KB * random.nextInt(MUL))

    //generate INT list of max 8 elments and multiple of 1024
    val kiloBytes = Seq.fill(8)(Random.nextInt(MAX)).map(k => k * 1024)

    print(kiloBytes + "\n")


    val rdd = spark.sparkContext
                   .parallelize(kiloBytes)

    //Convert each element of RDD to its respective MB.
    val megaBytes = rdd.map(k => k / 1024)
    print("Converted into MB:\n")
    megaBytes.collect
             .foreach(println)

    //Converted to Double
    val megaBytesDouble = megaBytes.map(m => m
      .toDouble)

    //Filter elements which are greater than 10 MB
    print("Filter greater than 10 MB:\n")
    val megaBytesGreaterThan10MB = megaBytes.filter(m => m > 10)
    megaBytesGreaterThan10MB.collect
                            .foreach(println)

    //Apply transformation to convert MB to GB

    val gegaBytes = megaBytesDouble.map(m => m / 1024)
    print("Converted into GB:\n")
    gegaBytes.collect
             .foreach(println)
  }
}
