import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

case class AirportData(
                        id: Int,
                        airportName: String,
                        city: String,
                        country: String,
                        code: String,
                        direction: String,
                        latitude: Double,
                        x: Double,
                        y: Double,
                        z: Int,
                        code2: Char,
                        countryWithCity: String
                      )

object Exercise06 {
  def toInt(s: String): Int = {
    try {
      s.toInt
    } catch {
      case e: Exception => 0
    }
  }
  def toDouble(s: String): Double = {
    try {
      s.toDouble
    } catch {
      case e: Exception => 0
    }
  }
  def parseDataToDomainObject(record: String): AirportData = {
    val fields = record.split(",")
    AirportData(
      fields(0).toInt,
      fields(1),
      fields(2),
      fields(3),
      fields(4),
      fields(5),
      toDouble(fields(6)),
      fields(7).toDouble,
      toDouble(fields(8)),
      toInt(fields(9)),
      fields(10)(0),
      fields(11)

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
    val airportDataRdd = spark.sparkContext
                              .textFile("./dataset/airports.txt")


    val airportDataDomainObj = airportDataRdd.map(parseDataToDomainObject(_))



  val recordsFilterByLatitude = airportDataDomainObj.filter(obj => obj.latitude > 40)

   recordsFilterByLatitude.collect().take(5).foreach(r=> println(r.airportName,r.latitude))
  }
}
