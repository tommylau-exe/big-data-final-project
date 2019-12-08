import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{ StringIndexer, VectorAssembler, IndexToString }
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("Big Data Final Project").getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    // Raw datasets
    lazy val moviesMetadata = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("data/the-movies-dataset/movies_metadata.csv")

    lazy val posterStats = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("data/etc/img_stats.csv")

    lazy val posterMovieLink = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("data/etc/img_movie_link.csv")

    lazy val moviesGenre = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("data/etc/movies_genre.csv")

    // Join all datasets
    lazy val joinedData = moviesMetadata
      .join(posterMovieLink, "title")
      .join(posterStats, "img_id")
      .join(moviesGenre, "id")

    val numGenres = joinedData.select("genre").distinct().count
    println(s"Total number of genres: $numGenres")

    // Set up pipeline
    val indexer = new StringIndexer()
      .setInputCol("genre")
      .setOutputCol("genreIndex")
      .fit(joinedData)

    val va = new VectorAssembler()
      .setInputCols(Array("avg_h", "avg_s", "avg_v"))
      .setOutputCol("features")

    val rf = new RandomForestClassifier()
      .setLabelCol("genreIndex")
      .setFeaturesCol("features")
      .setNumTrees(10)

    val unIndexer = new IndexToString()
      .setLabels(indexer.labels)
      .setInputCol("prediction")
      .setOutputCol("predictionLabel")

    val pipeline = new Pipeline()
      .setStages(Array(indexer, va, rf, unIndexer))

    // Split data and apply
    val Array(trainingData, testData) = joinedData.randomSplit(Array(0.75, 0.25))

    val model = pipeline.fit(trainingData)

    val predictions = model.transform(testData)

    // Show some predictions
    predictions.select("features", "genre", "predictionLabel").show()

    // Evaluate predictions
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("genreIndex")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)
    println(s"Classifier accuracy: ${accuracy}")

    // val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    // println(s"Learned classification forest model:\n ${rfModel.toDebugString}")

    spark.stop()
  }
}
