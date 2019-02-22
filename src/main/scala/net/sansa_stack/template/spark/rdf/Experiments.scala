package net.sansa_stack.template.spark.rdf

import org.apache.spark.sql.DataFrame

object Experiments {

  def constraintSatisfaction(ns: Long, n: Long): Float = {
    return ns/n
  }

  def reductionRatio(trainingSet: DataFrame, optimalScheme: String): Float = {

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
}
