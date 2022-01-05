import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Exercise07 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org")
                  .setLevel(Level
                    .OFF)

            val spark = SparkSession.builder()
                                    .master("local")
                                    .appName("DataFrameExamples")
                                    .getOrCreate()

    val numbers = List(1,2,3,4,5,6,7,8,9,10)
    val numbersRdd = spark.sparkContext.parallelize(numbers,2)
    print("\n number of partitions = " + numbersRdd.getNumPartitions)
    print("\n first element = " + numbersRdd.first())
    print("\n sum = " + numbersRdd.sum())
    print("\n min = " + numbersRdd.min())
    print("\n max = " + numbersRdd.max())
    print("\n avg = " + numbersRdd.sum()/numbersRdd.count())
  }
}
