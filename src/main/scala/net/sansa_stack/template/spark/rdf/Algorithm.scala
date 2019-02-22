package net.sansa_stack.template.spark.rdf

import net.sansa_stack.template.spark.rdf._

import scala.Predef._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.rockymadden.stringmetric.phonetic.RefinedSoundexMetric
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.datasyslab.geosparksql.utils.DataFrameFactory


object Algorithm {

  case class FeatureVector(Tuple: String,
                           Author: Integer,
                           Title: Integer,
                           Venue: Integer,
                           Review: Integer)

  def main(args: Array[String]): Unit = {
    run("dataset.csv")

//    parser.parse(args, Config()) match {
//      case Some(config) =>
//        run(config.in)
//      case None =>
//        println(parser.usage)
//    }
  }

  def run(input: String): Unit = {

    val spark = SparkSession.builder
        .master("local[*]")
      .appName(s"Spark CSV Reader")
      .getOrCreate()

    val resourcePath = "src/main/resources/dataset.csv"
    val df = spark.read.format("csv").option("header", "true").load(resourcePath)

    val featureVectors = generateFeatureVectors(spark, df)

    var n: Long = 0
    var oldN: Long = 0
    var stop = false

    val budget = 170
    var S = featureVectors.columns
    var optimalScheme = ""
    var errorRateThreshold = 0.3

    val sampleSize = 45
    var X = randomSample(featureVectors, sampleSize)

    var trainingSet = spark.emptyDataFrame

    var constraintSatisfactionNs: Long = 0

    while (n < budget && !stop) {

      S.foreach { scheme =>
        val featureVectorsExceptSample = featureVectors.exceptAll(X)
        val gamma = calculateGamma(X, scheme)
        val deficiency = (gamma * X.count()).toInt

        X = addSample(X, featureVectorsExceptSample,
                        scheme, deficiency, gamma <=0)

        n = X.count()
      }

      if (n == oldN) {
        stop = true
      }
      oldN = n

      trainingSet = humanOracle(spark, X)

      val trainingSetColumnNames = trainingSet.columns

      var schemeErrorRates = S.map { s => spark.sparkContext.doubleAccumulator }

      trainingSet.foreach { row =>
        val humanOracle = row.getString(row.length - 1)
        var schemes = S.clone()

        for (i <- 0 to row.length - 2) {
          val columnName = trainingSetColumnNames(i)
          val value = row.getInt(i)

          schemes = schemes.map { scheme =>
            scheme.replaceAll(columnName, value.toString)
          }
        }

        val schemeResults = schemes.map { scheme =>
          BlockingSchemeCalculator.calculate(scheme)
        }

        for (i <- 0 to schemeResults.length - 1) {
          val result = schemeResults(i)

          if ((result && humanOracle == "N") || (!result && humanOracle == "M")) {
            schemeErrorRates(i).add(1)
          }
        }
      }

      val finalErrorRates = schemeErrorRates.map { error =>
        error.value / trainingSet.count()
      }

      var minIndex = 0
      var min: Double = finalErrorRates(0)

      for (i <- 0 to finalErrorRates.length - 1) {
        val value = finalErrorRates(i)
        if (value < min) {
          minIndex = i
          min = value
        }
      }

      optimalScheme = S(minIndex)

      val sPrevious = S.clone()
      S = Array[String]()

      if (min <= errorRateThreshold) {

        constraintSatisfactionNs = n
        val optimumFP = falsePositives(spark, optimalScheme, trainingSet)

        for (i <- 0 to sPrevious.length - 1) {
          val scheme = sPrevious(i)
          val newSchemes = generateBlockingSchemes(Array(scheme), optimalScheme, "and")

          if (!newSchemes.isEmpty) {
            val andScheme = newSchemes(0)

            val fp = falsePositives(spark, scheme, trainingSet)

            val andSchemeFP = falsePositives(spark, andScheme, trainingSet)

            if ((optimumFP >= andSchemeFP) && fp >= andSchemeFP) {
              S = S :+ andScheme
            } else {
              S = S :+ scheme
            }
          }
        }

      } else {

        val optimalFN = falseNegatives(spark, optimalScheme, trainingSet)
        val optimalTP = truePositives(spark, optimalScheme, trainingSet)

        for (i <- 0 to sPrevious.length - 1) {
          val scheme = sPrevious(i)
          val fn = falseNegatives(spark, scheme, trainingSet)
          val tp = truePositives(spark, scheme, trainingSet)

          val newSchemes = generateBlockingSchemes(Array(scheme), optimalScheme, "or")

          if (!newSchemes.isEmpty) {
            val orScheme = newSchemes(0)
            val orSchemeFN = falseNegatives(spark, orScheme, trainingSet)
            val orSchemeTP = truePositives(spark, orScheme, trainingSet)

            if (((optimalFN/optimalTP) >= (orSchemeFN/orSchemeTP)) && ((fn/tp) >= (orSchemeFN/orSchemeTP))) {
              S = S :+ orScheme
            } else {
              S = S :+ scheme
            }
          }
        }
      }
    }

    println(s"Optimal Scheme ${optimalScheme}")
    println("Results")
    val reductionRatio = calculateReductionRatio(trainingSet, optimalScheme)
    val completness = pairCompleteness(trainingSet, optimalScheme)
    val quality = pairQuality(trainingSet, optimalScheme)
    val fmeas = fMeasure(completness, quality)
    val cSatisfaction = constraintSatisfaction(constraintSatisfactionNs, n)

    println(s"Budget ${budget}")
    println("Human Oracle Percentage 100%")
    println(s"Initial Sample Size ${sampleSize}")
    println(s"Sample Size ${trainingSet.count()}")
    println(s"Error Rate Threshold ${errorRateThreshold}")
    println(s"Reduction Ratio ${reductionRatio}")
    println(s"Pair Completeness ${completness}")
    println(s"Pair Quality ${quality}")
    println(s"F Measure ${fmeas}")
    println(s"Constraint Satisfaction ${cSatisfaction}")

    spark.stop
  }


  def constraintSatisfaction(ns: Long, n: Long): Float = {
    return ns/n
  }

  def calculateReductionRatio(trainingSet: DataFrame, optimalScheme: String): Float = {

    val columnNames = trainingSet.columns

    val satisfyingRows = trainingSet.filter { row =>
      var scheme = optimalScheme

      for (i <- 0 to columnNames.length - 2) {
        val columnName = columnNames(i)
        val value = row.getInt(i)

        scheme = scheme.replaceAll(columnName, value.toString)
      }

      BlockingSchemeCalculator.calculate(scheme)
    }

    val notSatisfyingRows = trainingSet.exceptAll(satisfyingRows)

    return 1.toFloat - (satisfyingRows.count().toFloat/notSatisfyingRows.count().toFloat)
  }

  def pairCompleteness(trainingSet: DataFrame, optimalScheme: String): Float = {

    val columnNames = trainingSet.columns

    val satisfyingRows = trainingSet.filter { row =>
      var scheme = optimalScheme
      val humanOracle = row.getString(row.length - 1)

      for (i <- 0 to columnNames.length - 2) {
        val columnName = columnNames(i)
        val value = row.getInt(i)

        scheme = scheme.replaceAll(columnName, value.toString)
      }

      BlockingSchemeCalculator.calculate(scheme) && humanOracle == "M"
    }

    val matchedRows = trainingSet.filter { row =>
      val humanOracle = row.getString(row.length - 1)
      humanOracle == "M"
    }

    return satisfyingRows.count().toFloat/matchedRows.count().toFloat
  }

  def pairQuality(trainingSet: DataFrame, optimalScheme: String): Float = {

    val columnNames = trainingSet.columns

    val satisfyingRows = trainingSet.filter { row =>
      var scheme = optimalScheme
      val humanOracle = row.getString(row.length - 1)

      for (i <- 0 to columnNames.length - 2) {
        val columnName = columnNames(i)
        val value = row.getInt(i)

        scheme = scheme.replaceAll(columnName, value.toString)
      }

      BlockingSchemeCalculator.calculate(scheme) && humanOracle == "M"
    }

    val schemeSatisfyRows = trainingSet.filter { row =>
      var scheme = optimalScheme

      for (i <- 0 to columnNames.length - 2) {
        val columnName = columnNames(i)
        val value = row.getInt(i)

        scheme = scheme.replaceAll(columnName, value.toString)
      }

      BlockingSchemeCalculator.calculate(scheme)
    }

    return satisfyingRows.count().toFloat/schemeSatisfyRows.count().toFloat
  }

  def fMeasure(pairCompleteness: Float, pairQuality: Float): Float = {
    return (2 * pairCompleteness * pairQuality)/(pairQuality + pairCompleteness)
  }

  def falsePositives(spark: SparkSession, scheme: String, trainingSet: DataFrame): Long = {

    val count = spark.sparkContext.longAccumulator
    val columnNames = trainingSet.columns

    trainingSet.foreach { row =>
      val humanOracle = row.getString(row.length - 1)
      var currentScheme = scheme

      for (i <- 0 to row.length - 2) {
        val columnName = columnNames(i)
        val value = row.getInt(i)

        currentScheme = currentScheme.replaceAll(columnName, value.toString)
      }

      currentScheme = currentScheme.replaceAll(" and ", "&")
      currentScheme = currentScheme.replaceAll(" or ", "|")

      val result = BlockingSchemeCalculator.calculate(currentScheme)

      if (result && humanOracle == "N") {
        count.add(1)
      }
    }

    return count.value
  }

  def falseNegatives(spark: SparkSession, scheme: String, trainingSet: DataFrame): Long = {

    val count = spark.sparkContext.longAccumulator
    val columnNames = trainingSet.columns

    trainingSet.foreach { row =>
      val humanOracle = row.getString(row.length - 1)
      var currentScheme = scheme

      for (i <- 0 to row.length - 2) {
        val columnName = columnNames(i)
        val value = row.getInt(i)

        currentScheme = currentScheme.replaceAll(columnName, value.toString)
      }

      currentScheme = currentScheme.replaceAll(" and ", "&")
      currentScheme = currentScheme.replaceAll(" or ", "|")

      val result = BlockingSchemeCalculator.calculate(currentScheme)

      if (!result && humanOracle == "M") {
        count.add(1)
      }
    }

    return count.value
  }

  def truePositives(spark: SparkSession, scheme: String, trainingSet: DataFrame): Long = {

    val count = spark.sparkContext.longAccumulator
    val columnNames = trainingSet.columns

    trainingSet.foreach { row =>
      val humanOracle = row.getString(row.length - 1)
      var currentScheme = scheme

      for (i <- 0 to row.length - 2) {
        val columnName = columnNames(i)
        val value = row.getInt(i)

        currentScheme = currentScheme.replaceAll(columnName, value.toString)
      }

      currentScheme = currentScheme.replaceAll(" and ", "&")
      currentScheme = currentScheme.replaceAll(" or ", "|")

      val result = BlockingSchemeCalculator.calculate(currentScheme)

      if (result && humanOracle == "M") {
        count.add(1)
      }
    }

    return count.value
  }

  def humanOracle(spark: SparkSession, sampleFeatureVectors: DataFrame): DataFrame = {

    val newSample = sampleFeatureVectors.withColumn("Label", lit("M"))

    val rdd = newSample.rdd.map { row =>
      val valuesArray = row.toSeq.toArray

      var numberOf1 = 0
      val total = row.length - 2

      for (i <- 0 to total) {
        val value = row.getInt(i)

        if (value == 1) {
          numberOf1 = numberOf1 + 1
        }
      }

      val totalFeatures = row.length - 1
      val percentage: Float = (numberOf1.toFloat/totalFeatures.toFloat) * 100
      var result = "N"
      if (percentage >= 100) {
        result = "M"
      }

      valuesArray(row.length - 1) = result

      Row.fromSeq(valuesArray.toSeq)
    }

    spark.createDataFrame(rdd, newSample.schema)
  }

  def generateFeatureVectors(spark: SparkSession, dataframe: DataFrame): DataFrame = {

    import spark.implicits._

    var featureVectors: Vector[FeatureVector] = Vector()
    val count = dataframe.count().intValue()

    for (x <- 0 to count) {
      val xIndex = x+1
      val xRecord = dataframe.filter(($"Record" === s"r${xIndex}"))

      if (xRecord.isEmpty == false) {

        val xAuthor = xRecord.first().get(1).asInstanceOf[String]
        val xTitle = xRecord.first().get(2).asInstanceOf[String]
        val xVenue = xRecord.first().get(3).asInstanceOf[String]
        val xRating = xRecord.first().get(4).asInstanceOf[String]

        for (y <- 0 to count) {
          val yIndex = y + 1

          val yRecord = dataframe.filter(($"Record" === s"r${yIndex}"))

          if (yRecord.isEmpty == false && y > x) {
            val yAuthor = yRecord.first().get(1).asInstanceOf[String]
            val yTitle = yRecord.first().get(2).asInstanceOf[String]
            val yVenue = yRecord.first().get(3).asInstanceOf[String]
            val yRating = yRecord.first().get(4).asInstanceOf[String]

            val authorFeature = RefinedSoundexMetric.compare(xAuthor, yAuthor).getOrElse(false)
            val titleFeature = RefinedSoundexMetric.compare(xTitle, yTitle).getOrElse(false)
            val venueFeature = RefinedSoundexMetric.compare(xVenue, yVenue).getOrElse(false)
            val ratingFeature = RefinedSoundexMetric.compare(xRating, yRating).getOrElse(false)

            var aF = 0
            var tF = 0
            var vF = 0
            var rF = 0

            if (authorFeature) {
              aF = 1
            }
            if (titleFeature) {
              tF = 1
            }
            if (venueFeature) {
              vF = 1
            }
            if (ratingFeature) {
              rF = 1
            }

            val featureVector = FeatureVector(s"r${xIndex},r${yIndex}", aF, tF, vF, rF)
            featureVectors = featureVectors :+ featureVector
          }
        }
      }
    }

    val columnNames = Seq("Author", "Title", "Venue", "Review")
    val featureVectorWithoutTupleColumn = featureVectors.toDF().select(columnNames.head, columnNames.tail: _*)

    return featureVectorWithoutTupleColumn.toDF()
  }

  def randomSample(dataFrame: DataFrame, size: Int): DataFrame = {
    val sample = dataFrame.sample(false, 1D*size/dataFrame.count)
    return sample
  }

  def calculateGamma(sample: DataFrame, blockingScheme: String): Float = {

    val columnNames = sample.columns
    val satisfyingRows = sample.filter { row =>
      var scheme = blockingScheme

      for (i <- 0 to columnNames.length - 1) {
        val columnName = columnNames(i)
        val value = row.getInt(i)

        scheme = scheme.replaceAll(columnName, value.toString)
      }

      BlockingSchemeCalculator.calculate(scheme)
    }

    val notSatisfyingRows = sample.exceptAll(satisfyingRows)
    val gamma: Float = ((satisfyingRows.count() - notSatisfyingRows.count()).toFloat / sample.count())

    return gamma
  }

  def addSample(sampleFeatureVectors: DataFrame,
                featureVectorsExceptSample: DataFrame,
                blockingScheme: String,
                deficiency: Int,
                similar: Boolean): DataFrame = {


    val columnNames = featureVectorsExceptSample.columns

    val filteredRows = featureVectorsExceptSample.filter { row =>
      var scheme = blockingScheme

      for (i <- 0 to columnNames.length - 1) {
        val columnName = columnNames(i)
        val value = row.getInt(i)

        scheme = scheme.replaceAll(columnName, value.toString)
      }

      if (similar) {
        BlockingSchemeCalculator.calculate(scheme)
      } else {
        !BlockingSchemeCalculator.calculate(scheme)
      }
    }

    val newRows = filteredRows.limit(math.abs(deficiency))

    return sampleFeatureVectors.union(newRows)
  }

  def generateBlockingSchemes(schemes: Array[String],
                              optimumScheme: String,
                              condition: String): Array[String] = {

    val schemesExceptOptimum = schemes.filter { scheme =>
      scheme != optimumScheme
    }

    val newSchemes = schemesExceptOptimum.map { scheme =>
      s"(${scheme} ${condition} ${optimumScheme})"
    }

    return newSchemes
  }

//  case class Config(in: String = "")
//
//  val parser = new scopt.OptionParser[Config]("Spark CSV Reader") {
//
//    head("Spark CSV Reader")
//
//    opt[String]('i', "input").required().valueName("<path>").
//      action((x, c) => c.copy(in = x)).
//      text("path to file that contains the data (in N-Triples format)")
//
//    help("help").text("prints this usage text")
//  }
}
