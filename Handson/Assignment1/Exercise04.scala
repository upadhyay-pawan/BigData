import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Exercise04 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org")
          .setLevel(Level
            .OFF)

    val spark = SparkSession.builder()
                            .master("local")
                            .appName("DataFrameExamples")
                            .getOrCreate()

    val numbers = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    val numbersRdd = spark.sparkContext
                          .parallelize(numbers)

    //sum of all numbers
    print("\nsum =" + numbersRdd.reduce(_ + _))

    //Total element in the list
    print("\nTotal count =" + numbersRdd.count())

    //average of numbers in the list
    print("\nAverage =" + numbersRdd.reduce(_ + _) / numbersRdd.count().toDouble)

    //sum of all even numbers in the list
    print("\nEven numbers Sum =" + numbersRdd.filter(e => e % 2 == 0).sum())

    //number of elements in the list divisible by both 5 and 3
    print("\nnumbers divisible by 3 & 5 =" + numbersRdd.filter(e => (e % 3 == 0) && (e % 5 == 0)).count())

  }
}
