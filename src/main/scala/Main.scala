import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{ StringIndexer, VectorAssembler, IndexToString, OneHotEncoderEstimator }
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{ RandomForestClassificationModel, RandomForestClassifier }
import org.apache.spark.ml.regression.{ RandomForestRegressor }
import org.apache.spark.ml.evaluation.{ MulticlassClassificationEvaluator, RegressionEvaluator }

import swiftvis2.plotting.Plot
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer

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

    // Some quick statistics
    val numGenres = joinedData.select("genre").distinct().count
    println(s"Total number of genres: $numGenres")

    val numMovies = moviesMetadata.count
    val numDrama  = moviesGenre.filter($"genre" === "Drama").count
    println(s"Percentage of movies that are Drama: ${(numDrama.toDouble / numMovies) * 100}")

    val allColors = List(RedARGB, YellowARGB, GreenARGB, CyanARGB, BlueARGB, MagentaARGB, RedARGB)
    val rainbowCg = ColorGradient(allColors.zipWithIndex.map { case (c,i) => i.toDouble / (allColors.length - 1) -> c }:_*)
    lazy val hsvGraphs = {
      val data = joinedData
        .select("avg_h", "avg_s", "avg_v")
        .as[(Double, Double, Double)]
        .collect()

        val plot = Plot.scatterPlot(
          data.map(_._1),
          data.map(_._3),
          s"HSV Distribution in Movie Posters",
          "Average Hue",
          "Average Value",
          symbolSize = data.map(data => data._2 * 5 + 2),
          symbolColor = data.map(data => rainbowCg(data._1))
        )

        SwingRenderer(plot, 800, 600, true)
    }

    lazy val hueOverTime = {
      val getYear = udf { (s: String) => s.take(4).toInt }

      val data = joinedData
        .filter($"release_date".isNotNull)
        .withColumn("year", getYear($"release_date"))
        .select("avg_h", "year")
        .as[(Double, Int)]
        .collect()

      val plot = Plot.scatterPlot(
        data.map(_._2),
        data.map(_._1),
        s"Average Hue Distribution in Movie Posters over Time",
        "Release Year",
        "Average Poster Hue",
        symbolSize = 4,
        symbolColor = data.map(data => rainbowCg(data._1)),
      )

      SwingRenderer(plot, 800, 800, true)
    }

    lazy val genrePrediction = {
      // Set up pipeline
      val indexer = new StringIndexer()
        .setInputCol("genre")
        .setOutputCol("genreIndex")
        .fit(joinedData)

      val indexedData = indexer.transform(joinedData)
      
      val ohe = new OneHotEncoderEstimator()
        .setInputCols(Array("genreIndex"))
        .setOutputCols(Array("genreOhe"))
        .fit(indexedData)

      val va = new VectorAssembler()
        .setInputCols(Array("avg_h", "avg_s", "avg_v"))
        .setOutputCol("features")

      val rf = new RandomForestRegressor()
        .setLabelCol("genreIndex")
        .setFeaturesCol("features")
        .setNumTrees(10)

      val pipeline = new Pipeline()
        .setStages(Array(ohe, va, rf))

      // Split data and apply
      val Array(trainingData, testData) = indexedData.randomSplit(Array(0.75, 0.25))

      val model = pipeline.fit(trainingData)

      val predictions = model.transform(testData)

      // Show some predictions
      predictions.select("features", "genre", "genreIndex", "prediction").show()

      // Evaluate predictions
      val evaluator = new RegressionEvaluator()
        .setLabelCol("genreIndex")
        .setPredictionCol("prediction")
        .setMetricName("rmse")

      val rmse = evaluator.evaluate(predictions)
      println(s"Classifier rmse: $rmse")

      // val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
      // println(s"Learned classification forest model:\n ${rfModel.toDebugString}")
    }

    println(hueOverTime)

    spark.stop()
  }
}
