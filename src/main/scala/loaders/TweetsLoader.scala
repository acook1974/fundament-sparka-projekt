package loaders

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object TweetsLoader {
  
  def loadGrammysTweets(spark: SparkSession): DataFrame = {
    spark.read
        .option("header", "true")
        .csv("data/GRAMMYs_tweets.csv")
  }

}
