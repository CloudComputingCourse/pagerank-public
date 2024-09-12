/*
You may find these resources useful while working with Spark Datasets and DataFrames in Scala:

https://spark.apache.org/docs/latest/sql-programming-guide.html
https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html
https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Column.html
https://spark.apache.org/docs/latest/api/sql/index.html

Note: These resources are not exhaustive and you must treat them as a starting point while navigating the documentation for Scala Spark APIs
*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, LongType, DoubleType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{lit, size, col, collect_list, array, explode_outer, concat, when, coalesce, max, sum, greatest}
import java.io.PrintWriter

object PageRank {
  
  // Do not modify
  val PageRankIterations = 10
  
  /**
   * Input graph is a plain text file of the following format:
   *
   *   follower  followee
   *   follower  followee
   *   follower  followee
   *   ...
   *
   * where the follower and followee are separated by `\t`.
   *
   * After calculating the page ranks of all the nodes in the graph,
   * the output should be written to `outputPath` in the following format:
   *
   *   node  rank
   *   node  rank
   *   node  rank
   *
   * where node and rank are separated by `\t`.
   *
   * Similarly, for task 2.2, after calculating the recommended users for the users,
   * the output should be written to `recsOutputPath` in the following format:
   *
   *   node  rank
   *   node  rank
   *   node  rank
   *
   * where node and rank are separated by `\t`.
   *
   * @param inputGraphPath path of the input graph.
   * @param outputPath     path of the output of page rank.
   * @param iterations     number of iterations to run on the PageRank.
   * @param spark          the SparkSession.
   */
  def calculatePageRank(
                         inputGraphPath: String,
                         graphTopicsPath: String,
                         pageRankOutputPath: String,
                         recsOutputPath: String,
                         iterations: Int,
                         spark: SparkSession): Unit = {
    val sc = spark.sparkContext
    
    import spark.implicits._
    
    // load graph
    val schema = StructType(Array(
      StructField("follower", LongType),
      StructField("followee", LongType)
    ))
    
    val df = spark.read
      .option("sep", "\t")
      .schema(schema)
      .csv(inputGraphPath)
    
    // load topics
    val schema2 = StructType(Array(
      StructField("user", LongType),
      StructField("games", DoubleType),
      StructField("movies", DoubleType),
      StructField("music", DoubleType)
    ))
    
    // Reads users and their interests - Relevant to Task 2.2
    val df_topics = spark.read
      .option("sep", "\t")
      .schema(schema2)
      .csv(graphTopicsPath)
    
    // collect static data
    val df_following = df.groupBy("follower")
      .agg(collect_list("followee") as "followees")
      .withColumn("numfollowing", size(col("followees")))
    
    val statics = df_following.join(df_topics, df_following("follower") === df_topics("user"), "right")
    
    // collect variable data
    val vertices = df.flatMap(r => Array((r.getLong(0), 0.0), (r.getLong(1), 0.0)))
      .toDF("user", "rank")
    var ranks = vertices.groupBy("user").agg(lit(1.0).as("rank"))
    
    
    var vars = df_topics
      // TODO: Note that you will have to change the rank initialization based on the information given in the writeup
      .withColumn("rank", lit(1.0))
      
      // you may comment the next 3 lines while solving Task 2.1
      .withColumn("games_rec", array(df_topics("games"), df_topics("user")))
      .withColumn("movies_rec", array(df_topics("movies"), df_topics("user")))
      .withColumn("music_rec", array(df_topics("music"), df_topics("user")))
      
      .select("user", "rank", "games_rec", "movies_rec", "music_rec")
    
    for (i <- 1 to 10) {
      // join static and variable data
      val joined = statics.join(vars, "user")
      
      // explode each row in joined df into contribution vectors for each followee
      val contribs = joined
        // add user to list of followees to generate self-contribution vector
        // * keep follower-less nodes from disappearing in PageRank
        // * include current max in comparison for recommendations
        .withColumn("followee", explode_outer(concat($"followees", array($"user"))))
        .withColumn("contrib", when($"user" !== $"followee", joined("rank") / joined("numfollowing"))
          .otherwise(0)) // zero rank contrib in self-contrib case
        
        // you may comment the next three lines while solving Task 2.1
        // TODO: Task 2.2 zero out recommendation for topic if non-interest for sender
        .withColumn("games_rec", joined("games_rec"))
        .withColumn("movies_rec", joined("movies_rec"))
        .withColumn("music_rec", joined("music_rec"))
        
        // keep selected values
        // * replace null followee fields with user (from explode_outer on rows with null followee list)
        .select($"user", coalesce($"followee", $"user").alias("followee"), $"contrib",
          $"games_rec", $"movies_rec", $"music_rec")
        
        // replace null rank contributions with 0 so that they sum properly
        .na.fill(0, Seq("contrib"))
      
      // TODO: Handle dangling node contributions
      
      // aggregate contributions
      // TODO: You will have to aggregate values for Task 2.2
      vars = contribs.groupBy("followee").agg(
          sum("contrib").as("rank")
        )
        .withColumn("rank", $"rank" * lit(0.85) + lit(0.15))
        .withColumnRenamed("followee", "user")
      
    }
    
    // get final ranks
    val rank_output = vars.select("user", "rank")
    
    // TODO: get final recommendations as maximum over topics
    
    // TODO: Write to the output files
  }
  
  /**
   * @param args it should be called with two arguments, the input path, and the output path.
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.sparkSession()
    
    val inputGraph = args(0)
    val graphTopics = args(1)
    val pageRankOutputPath = args(2)
    val recsOutputPath = args(3)
    
    calculatePageRank(inputGraph, graphTopics, pageRankOutputPath, recsOutputPath, PageRankIterations, spark)
  }
}
