import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.{ StringIndexer, VectorAssembler, IndexToString, OneHotEncoderEstimator }
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{ RandomForestClassificationModel, RandomForestClassifier }
import org.apache.spark.ml.regression.{ RandomForestRegressor }
import org.apache.spark.ml.evaluation.{ MulticlassClassificationEvaluator, RegressionEvaluator }
import org.apache.spark.ml.recommendation.ALS

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

    lazy val ratings = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("data/the-movies-dataset/ratings.csv")

    // Join all datasets
    val getYear = udf { (s: String) => s.take(4).toInt }

    lazy val joinedData = moviesMetadata
      .join(posterMovieLink, "title")
      .join(posterStats, "img_id")
      .join(moviesGenre, "id")

    lazy val yearData = joinedData
      .filter($"release_date".isNotNull)
      .withColumn("year", getYear($"release_date"))

    lazy val movieRatings = moviesMetadata
      .join(ratings, $"id" === $"movieId")

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
      val data = yearData
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
      // Some quick statistics
      val numGenres = joinedData.select("genre").distinct().count
      println(s"Total number of genres: $numGenres")

      val numMovies = moviesMetadata.count
      val numDrama  = moviesGenre.filter($"genre" === "Drama").count
      println(s"Percentage of movies that are Drama: ${(numDrama.toDouble / numMovies) * 100}")

      val usingGenres = moviesGenre
        .groupBy("genre")
        .count()
        .sort(-$"count")
        .select("genre")
        .as[String]
        .take(5)

      println(s"Using genres ${usingGenres.mkString(", ")}")

      val genreFromRank = udf { (r: Int) => usingGenres(r) }

      val trimmedData = moviesGenre
        .join(spark.createDataset(usingGenres.zipWithIndex.toSeq).toDF("genre", "genreRank"), "genre")
        .groupBy("id")
        .max("genreRank") // To break ties using most common genre, change .max() to .min()
        .withColumnRenamed("max(genreRank)", "genreRank")
        .withColumn("genre", genreFromRank($"genreRank"))
        .join(yearData, Seq("id", "genre"))

      trimmedData.show

      // Set up pipeline
      val indexer = new StringIndexer()
        .setInputCol("genre")
        .setOutputCol("genreIndex")
        .fit(trimmedData)

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
      val Array(trainingData, testData) = trimmedData.randomSplit(Array(0.75, 0.25))

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

      // Calculate percent each genre is predicted
      val numPredictions = predictions.count
      predictions
        .groupBy("predictionLabel")
        .count()
        .withColumn("pctPredicted", ($"count" / numPredictions.toDouble) * 100)
        .show(false)

      // val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
      // println(s"Learned classification forest model:\n ${rfModel.toDebugString}")
    }

    lazy val ratingsGraph = {
      val buckets = (0.0 to 5.5 by 0.5).toArray

      val histData = movieRatings
        .select("rating")
        .as[Double]
        .rdd
        .histogram(buckets, true)

      val plot = Plot.histogramPlot(
        buckets,
        histData,
        BlackARGB,
        centerOnBins = false,
        "Frequency of Movie Ratings",
        "Rating",
        "Frequency of Rating"
      )

      SwingRenderer(plot, 800, 600, true)
    }

    lazy val reccomendation = {
      val Array(training, test) = movieRatings.randomSplit(Array(0.75, 0.25))

      val als = new ALS()
        .setMaxIter(5)
        .setRegParam(0.1)
        .setUserCol("userId")
        .setItemCol("movieId")
        .setRatingCol("rating")

      val model = als.fit(training)

      model.setColdStartStrategy("drop")
      val predictions = model.transform(test)

      val evaluator = new RegressionEvaluator()
        .setMetricName("rmse")
        .setLabelCol("rating")
        .setPredictionCol("prediction")

      val rmse = evaluator.evaluate(predictions)
      println(s"rmse: $rmse")

      val users = movieRatings.select("userId").distinct().limit(3)
      val userSubsetRecs = model.recommendForUserSubset(users, 10)
      userSubsetRecs.show
    }

    println(reccomendation)

    spark.stop()
  }
}
