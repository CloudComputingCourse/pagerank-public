/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, IntegerType, StringType, DoubleType}
import org.apache.spark.sql.functions.{lit, size, col, collect_list, explode, sum }

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL;neighbor URL
 * URL;neighbor URL
 * URL;neighbor URL
 * ...
 * where URL and their neighbors are separated by a semi-colon.
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.graphx.lib.PageRank
 *
 * This code is the modified version of 
 * https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/SparkPageRank.scala
 * 
 * This modified code has the same execution logic but is modified to take advantage of the Dataframe API.
 * Using the DataFrame API has several benefits which was outlined in the primer.
 * 
 * Example Usage:
 * {{{
 * bin/run-example SparkPageRank data/mllib/pagerank_data.txt 10
 * }}}
 * 
 * 
 */
object SparkPageRank {

  def showWarning(): Unit = {
    System.err.println(
      """WARN: This is a naive implementation of PageRank and is given as an example!
        |Please use the PageRank implementation found in org.apache.spark.graphx.lib.PageRank
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <file> <iter>")
      System.exit(1)
    }

    showWarning()

    val spark = SparkSession
      .builder()
      .appName("SparkPageRank")
      .getOrCreate()

    val iters = if (args.length > 1) args(1).toInt else 10
    val schema = new StructType()
    .add("follower", DoubleType)
    .add("followee", DoubleType)
    
    val links = spark.read.option("delimiter", ";").schema(schema).csv(args(0))

    val userFollows = links.groupBy("follower")
    .agg(collect_list("followee").as("followees"))
    
    var ranks = userFollows.select(col("follower").as("url"), lit(1.0).as("rank")).distinct()

    for (i <- 1 to iters) {
        
        val contribs = userFollows.join(ranks, col("follower") === col("url"), "left")
        .withColumn("contribs", lit(col("rank")/size(col("followees"))))
        .withColumn("followee", explode(col("followees")))
        .select(col("followee"), col("contribs"))

        ranks = contribs.select(col("followee").as("url"), col("contribs"))
        .groupBy("url")
        .agg(sum(col("contribs")).as("total_contribs"))
        .withColumn("rank", lit(0.15) + lit(0.85)*col("total_contribs"))
        .select(col("url"), col("rank"))
    
    } 

    ranks.show()
    spark.stop()
  }
}
// scalastyle:on println