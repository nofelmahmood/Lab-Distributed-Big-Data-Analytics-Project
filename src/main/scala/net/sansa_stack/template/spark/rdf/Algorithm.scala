package net.sansa_stack.template.spark.rdf

import org.apache.spark.sql.SparkSession

object Algorithm {

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
        .master("local")
      .appName(s"Spark CSV Reader")
      .getOrCreate()

    val resourcePath = "src/main/resources/dataset.csv"
    val df = spark.read.format("csv").option("header", "true").load(resourcePath)
    val rdd = df.rdd

    println("Printing RDD")
    rdd.foreach(println)
    println("Printing Data frame")

    df.show()

    spark.stop
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
