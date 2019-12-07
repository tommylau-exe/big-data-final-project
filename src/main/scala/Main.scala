import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("Big Data Final Project").getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    lazy val moviesMetadata = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("data/the-movies-dataset/movies_metadata.csv")

    lazy val posterStats = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("data/etc/img_stats.csv")

    val getPosterId = udf { (s: String) => s.drop(1).dropRight(4) }

    lazy val joinedData = moviesMetadata
      .withColumn("poster_id", getPosterId($"poster_path"))
      .join(posterStats.withColumnRenamed("img_id", "poster_id"), "poster_id")

    spark.stop()
  }
}
