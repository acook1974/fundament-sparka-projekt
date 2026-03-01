package loaders

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object TweetsLoaderConstants {
  val GRAMMYS_TWEETS_PATH = "data/GRAMMYs_tweets.csv"
  val FINANCIALS_TWEETS_PATH = "data/financial.csv"
  val COVID19_TWEETS_PATH = "data/covid19_tweets.csv"
  
  val CATEGORY_GRAMMYS = "GRAMMYS"
  val CATEGORY_FINANCIALS = "Financials"
  val CATEGORY_COVID19 = "Covid19"
}

class TweetsLoader(spark: SparkSession) {

  def loadAllTweets(): DataFrame = {
    val grammysTweets = loadGrammysTweets()
    val financialsTweets = loadFinancialsTweets()
    val covid19Tweets = loadCovid19Tweets()

    grammysTweets
      .unionByName(financialsTweets, allowMissingColumns = true)
      .unionByName(covid19Tweets, allowMissingColumns = true)
  }
  
  def loadGrammysTweets(): DataFrame = {
    spark.read
        .option("header", "true")
        .csv(TweetsLoaderConstants.GRAMMYS_TWEETS_PATH)
        .withColumn("category", lit(TweetsLoaderConstants.CATEGORY_GRAMMYS))
        .na.drop()
  }
  
  def loadFinancialsTweets(): DataFrame = {
    spark.read
        .option("header", "true")
        .csv(TweetsLoaderConstants.FINANCIALS_TWEETS_PATH)
        .withColumn("category", lit(TweetsLoaderConstants.CATEGORY_FINANCIALS))
        .na.drop()
  }

  def loadCovid19Tweets(): DataFrame = {
    spark.read
        .option("header", "true")
        .csv(TweetsLoaderConstants.COVID19_TWEETS_PATH)
        .withColumn("category", lit(TweetsLoaderConstants.CATEGORY_COVID19))
        .na.drop()
  }

}
