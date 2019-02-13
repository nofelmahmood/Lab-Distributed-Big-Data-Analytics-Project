package net.sansa_stack.template.spark.rdf

import scala.Predef._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import com.rockymadden.stringmetric.phonetic.RefinedSoundexMetric
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

    val sampleSize = 20
    val sample = randomSample(featureVectors, sampleSize)

    val authorsGamma = calculateGamma(sample, "Author")
    val titlesGamma = calculateGamma(sample, columnName = "Title")
    val venueGamma = calculateGamma(sample, columnName = "Venue")
    val reviewGamma = calculateGamma(sample, columnName = "Review")

    val authorsDeficient = authorsGamma * sample.count()
    val titlesDeficient = titlesGamma * sample.count()
    val venueDeficient = venueGamma * sample.count()
    val reviewDeficient = reviewGamma * sample.count()

    val featureVectorsExceptSample = featureVectors.exceptAll(sample)
    println(s"Feature Vectors count ${featureVectors.count()}")
    println(s"Sample Count ${sample.count()}")
    println(s"Feature Vectors Except Sample ${featureVectorsExceptSample.count()}")

    // Add similar or dissimilar samples
    addSample(sample, featureVectorsExceptSample,
      "Author",
      authorsDeficient.toInt,
      authorsDeficient <= 0)


    spark.stop
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

    featureVectorWithoutTupleColumn.show()

    return featureVectorWithoutTupleColumn.toDF()
  }

  def randomSample(dataFrame: DataFrame, size: Int): DataFrame = {
    val sample = dataFrame.sample(false, 1D*size/dataFrame.count)
    return sample
  }
  
  def calculateGamma(sample: DataFrame, columnName: String): Float = {
    val column = sample.select(columnName)
    val count = column.count().toInt

    val numberOf0 = column.filter { row =>
      val value = row.getInt(0)
      value.equals(0)
    }.count()

    val numberOf1 = column.filter { row =>
      val value = row.getInt(0)
      value.equals(1)
    }.count()

    val gamma: Float = ((numberOf1 - numberOf0).toFloat / count)

    return gamma
  }

  def satisfiesBlockingScheme(featureVector: Row,
                              columnIndexes: Array[Int],
                              conjunctionsAndDisjunctions: Array[Boolean]): Boolean = {

    val firstColumnIndex = columnIndexes(0)
    var result = featureVector.getInt(firstColumnIndex) == 1

    for (index <- 0 to (conjunctionsAndDisjunctions.length - 1)) {

      val columnIndex = columnIndexes(index + 1)
      val featureValue = featureVector.getInt(columnIndex) == 1
      val condition = conjunctionsAndDisjunctions(index)

      // Condition == true means and
      // Condition == false means or

      if (condition) {
        result = result && featureValue
      } else {
        result = result || featureValue
      }
    }

    return result
  }

  def addSample(sampleDataframe: DataFrame,
                dataframe: DataFrame,
                columnName: String,
                deficiency: Int,
                similar: Boolean): Unit = {

    val column = dataframe.select(columnName)



    if (similar) {
      var selectedRows: Vector[FeatureVector] = Vector()

      dataframe.foreach { row =>
        val author = row.getInt(0)
        println(s"Author ${author}")

      }

    } else {

      for (i <- 0 to deficiency) {

      }
    }
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
