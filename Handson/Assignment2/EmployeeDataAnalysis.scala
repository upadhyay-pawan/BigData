import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{array_max, col, split}

object EmployeeDataAnalysis {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org")
          .setLevel(Level
            .OFF)

    val spark = SparkSession.builder()
                            .master("local")
                            .appName("DataFrameExamples")
                            .getOrCreate()

    val employeeDF = spark.read
                          .option("header", "true")
                          .option("inferschema", "true")
                          .csv("./dataset/employee_skills.csv")


    val cleansedDF = employeeDF
      .withColumn("names", split(col("name"), ","))
      .withColumn("salaries", split(col("salary"), ","))
      .drop("name", "salary", "skill")


    cleansedDF.select(col("names").getItem(0)
                                  .as("name"),
      array_max(col("salaries")).as("max salary"))
              .show()

    cleansedDF.select(col("names").getItem(0)
                                  .as("name"),
      col("salaries").getItem(0)
                     .as("actual salary"),
      col("salaries").getItem(1)
                     .as("expected salary"))
              .filter(col("expected salary") < col("actual salary"))
              .show()

  }
}
