import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, LongType, DoubleType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{lit, size, col, collect_list, array, explode_outer, concat, when, coalesce, max, sum, greatest }
import java.io.PrintWriter

object SparkRecommendations {

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
    *   node  recommended_user
    *   node  recommended_user
    *   node  recommended_user
    *
    * where node and rank are separated by `\t`.
    *
    */
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
        .builder()
        .appName("SparkRecommendations")
        .getOrCreate()

    val iters = if (args.length > 1) args(1).toInt else 10

    val schema = new StructType()
    .add("follower", DoubleType)
    .add("followee", DoubleType)

    val links = spark.read.option("delimiter", "\t").schema(schema).csv(args(0))

    // load topics
    // TODO: Identify the appropriate data types
    val topicSchema = StructType(Array(
      StructField("user", null),
      StructField("games", null),
      StructField("movies", null),
      StructField("music", null)
    ))

    // TODO: Identify the right delimiter to use here
    val df_topics = spark.read
      .option("sep", null)
      .option("header","true")
      .schema(topicSchema)
      .csv(args(1))

    val userFollows = links.groupBy("follower")
        .agg(collect_list("followee").as("followees")).cache()

    val statics = userFollows.join(df_topics, userFollows("follower") === df_topics("user"), "right")

    // TODO: you may have to update the initial rank here
    var vars = df_topics
      .withColumn("games_rec", array(df_topics("games"), df_topics("user")))
      .withColumn("movies_rec", array(df_topics("movies"), df_topics("user")))
      .withColumn("music_rec", array(df_topics("music"), df_topics("user")))
      .select("user", "games_rec", "movies_rec", "music_rec")

    val d = 0.85
    for (i <- 1 to 10) {
      val joined = statics.join(vars, "user")

      val contribs = joined
        .withColumn("followee", explode_outer(concat($"followees", array($"user"))))

        // TODO: how would you filter out non-interests 
        .withColumn("games_rec", joined("games_rec"))
        .withColumn("movies_rec", joined("movies_rec"))
        .withColumn("music_rec", joined("music_rec"))

        // keep selected values
        // * replace null followee fields with user (from explode_outer on rows with null followee list)
        .select($"user", coalesce($"followee", $"user").alias("followee"), 
                $"games_rec", $"movies_rec", $"music_rec")
      

      // aggregate contributions
      // TODO: find the relevant aggregate function for games_rec, movies_rec, and music_rec
      vars = contribs.groupBy("followee").agg(

      )
      .withColumnRenamed("followee", "user")

    }

    // TODO: get final recommendations as maximum over topics, identify the relevant column function to achieve this
    rec_output.show()
  }
}