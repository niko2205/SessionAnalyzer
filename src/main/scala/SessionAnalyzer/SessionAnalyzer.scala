import org.apache.spark.sql.{SparkSession, functions}

object SessionAnalyzer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Session Analyzer")
      .master("local[*]")
      .getOrCreate()

    val dataSet = spark.read.textFile("sp/data/*.txt")

    import spark.implicits._
    import functions._

    val quickSearchCounts = dataSet.filter(line => line.contains("QS"))
      .flatMap(line => {
        val words = line.split(" ")
        val dateTime = words(1)
        val date = dateTime.split("_")(0)
        val docIds = words.drop(4).map(_.trim)
        docIds.map(docId => ((date, docId), 1))
      })
      .toDF("date", "docId", "count") // Преобразуем RDD в DataFrame
      .groupBy("date", "docId") // Группируем данные по дате и идентификатору документа
      .agg(sum("count").alias("count")) // Суммируем количество открытий для каждой группы

    quickSearchCounts.show()

    spark.stop()
  }
}