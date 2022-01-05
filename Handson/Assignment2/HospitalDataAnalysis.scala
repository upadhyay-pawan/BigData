import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, grouping, length, regexp_replace, sum, when}

object HospitalDataAnalysis {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org")
          .setLevel(Level
            .OFF)

    val spark = SparkSession.builder()
                            .master("local")
                            .appName("DataFrameExamples")
                            .getOrCreate()

    val hospitalOrginalDF = spark.read
                                 .option("header", "true")
                                 .option("inferschema", "true")
                                 .csv("./dataset/inpatientCharges.csv")

    //    hospitalOrginalDF.printSchema()
     // hospitalOrginalDF.show()





    val cleansedHospitalDF = hospitalOrginalDF.select(
      col("DRG Definition").substr(1, 3).as("disease"),
      col("Total Discharges").as("tot disch"),
      col("Average Covered Charges").substr(2, 10)
                                    .cast("double")
                                    .as("avg cov chg"),
      col("Average Total Payments").substr(2, 10)
                                   .cast("double")
                                   .as("avg tot pymt"),
      col("Average Medicare Payments").substr(2, 10)
                                      .cast("double")
                                      .as("avg medi pymt"),
      col("Provider State").as("prov state"))
                                              .dropDuplicates()

    cleansedHospitalDF.printSchema()
    //cleansedHospitalDF.show()

    //Exercise 01 : Find the amount of Average Covered Charges per state.

    cleansedHospitalDF.groupBy("prov state")
                      .agg(
                        sum(col("avg cov chg")).as("Average covered Charges")
                      )
                      .show()

    // Exercise 02 : Find the amount of Average Total Payments charges per state.

    cleansedHospitalDF.groupBy("prov state")
                      .agg(
                        sum(col("avg tot pymt")).as("average total payments")
                      )
                      .show()

    // Exercise 03 : Find the amount of Average Medicare Payments charges per state.

    cleansedHospitalDF.groupBy("prov state")
                      .agg(
                        sum(col("avg medi pymt")).as("average medicare payments")
                      )
                      .show()

    // Exercise 04 : Find out the total number of Discharges per state and for each disease.

    cleansedHospitalDF.groupBy("prov state","disease").agg(
      sum(col("tot disch")).as("total discharged")
    ).show()
  }
}
