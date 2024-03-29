Active Blocking Scheme Learning for Entity Resolution
=============================
[![Build Status](https://travis-ci.org/SANSA-Stack/SANSA-Template-Maven-Spark.svg?branch=develop)](https://travis-ci.org/SANSA-Stack/SANSA-Template-Maven-Spark)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Twitter](https://img.shields.io/twitter/follow/SANSA_Stack.svg?style=social)](https://twitter.com/SANSA_Stack)

This is a [Maven](https://maven.apache.org/) template to generate a [SANSA](https://github.com/SANSA-Stack) project using [Apache Spark](http://spark.apache.org/).

How to use
----------

```
git clone https://github.com/SANSA-Stack/SANSA-Template-Maven-Spark.git
cd SANSA-Template-Maven-Spark

mvn clean package
````

How to run
----------

```
mvn clean
sbt package

// To run spark locally
spark-submit --class net.sansa_stack.template.spark.rdf.Algorithm 
             --master local ./target/scala-2.11/sansa-template-maven-spark_2.11-0.1.jar
````

The subsequent steps depend on your IDE. Generally, just import this repository as a Maven project and start using SANSA / Spark. Enjoy it! :)
