import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object GlobalCounter {
  var lineCount = 0
  var maxWords = 0

  def lineCounter(): Int = {
    this
      .lineCount = lineCount + 1
    return lineCount
  }

  def maxWords(maxWords: Int): Int = {
    if (maxWords > this
      .maxWords) {
      this
        .maxWords = maxWords
    }
    return maxWords
  }
}


object Exercise05 {

  def countWordsInLine(line: Array[String]) = {
    var wordCount = 0

    for (i <- 0 to line.length - 1) {
      if (!(line(i).equals(""))) {
        wordCount = wordCount + 1
      }
    }
    GlobalCounter.maxWords(wordCount)
    print("\n words in line " + GlobalCounter.lineCounter() + "=" + wordCount)
  }


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org")
          .setLevel(Level
            .OFF)

    val spark = SparkSession.builder()
                            .master("local")
                            .appName("DataFrameExamples")
                            .getOrCreate()

    val fileRdd = spark.sparkContext
                       .textFile("./dataset/100west.txt")

    val mapTransformation = fileRdd.map(line => line.split(" "))
                                   .filter(word => word.length > 1)

    //words in each line
    mapTransformation.foreach(e => countWordsInLine(e))

    //total words
    val flatMapTransformation = fileRdd.flatMap(line => line.split(" "))
                                       .filter(word => word.length > 1)
                                       .map(word => (word, 1))
                                       .reduceByKey(_ + _) count()
    print("\n Total words=" + flatMapTransformation)


    //max word count
    print("\n Max words in line = " + GlobalCounter.maxWords)
  }
}
