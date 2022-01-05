import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Exercise02 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org")
          .setLevel(Level
            .OFF)

    val spark = SparkSession.builder()
                            .master("local")
                            .appName("DataFrameExamples")
                            .getOrCreate()

    // List of Strings
    val data = List("C Programming", "C++", "Java", "Scala", "Python")
    //Creating RDD
    val rdd = spark.sparkContext
                   .parallelize(data)
   //Fileter String length greater than 5
    val dataGreaterThanLenFive = rdd.filter(rec => rec.length > 5)
    print("output01:\n")
    dataGreaterThanLenFive.collect().foreach(e => println(e))
   //Reversing the String
    print("\noutput02:\n")
    dataGreaterThanLenFive.zipWithIndex()
                          .reduceByKey(_ + _)
                          .map(e => e.swap)
                          .sortByKey(false)
                          .collect
                          .foreach(e => println(e._2))


  }
}
