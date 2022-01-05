import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

case class MovieData(
                      userId: String,
                      itemId: String,
                      rating: String,
                      timeStamp: String
                    )

object Exercise08 {

  def parseDataToDomainObject(record: String): MovieData = {
    val fields = record.split("\\t")
    MovieData(
      fields(0),
      fields(1),
      fields(2),
      fields(3)
    )
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org")
          .setLevel(Level
            .OFF)

    val spark = SparkSession.builder()
                            .master("local")
                            .appName("DataFrameExamples")
                            .getOrCreate()

    val movieDataRdd = spark.sparkContext
                            .textFile("./dataset/popular_movies.txt")

    val movieDomainObj = movieDataRdd.map(parseDataToDomainObject(_))

    val movieFilteredByRating = movieDomainObj.map(m => (m.rating, 1))
                                              .reduceByKey(_ + _)
                                              .sortByKey(true)
    movieFilteredByRating.collect().foreach(m => println("Total " + m._1 + " star rating = " + m._2))

  }
}
