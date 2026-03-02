import org.apache.spark.sql.SparkSession
import loaders.TweetsLoader
import cleaners.TweetsCleaner
import analysers.TweetsAnalyzer
import tweetssearch.TweetsSearch
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object App {  

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
        .appName("fundament-sparka")
        .master("local")
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") 

    val tweetsLoader = new TweetsLoader(spark)
    val tweetsCleaner = new TweetsCleaner(spark)
    val tweetsAnalyzer = new TweetsAnalyzer(spark)
    val tweetsSearch = new TweetsSearch(spark)

    import tweetsSearch._

    // ładowanie i czyszczenie tweetów
    val tweetsDF: DataFrame = tweetsLoader.loadAllTweets().cache()
    val cleanedTweetsDF: DataFrame = tweetsCleaner.cleanAllTweets(tweetsDF)

    // wyszukiwanie tweetów
    val trumpTweetsDF: DataFrame = cleanedTweetsDF
      .transform(searchByKeyWord("Trump"))
      .transform(onlyInLocation("United States"))

    // analiza tweetów
    val sourceCount: DataFrame = tweetsAnalyzer.calculateSourceFrequency(trumpTweetsDF)
    sourceCount
      .orderBy(desc("count"))
      .show(numRows = sourceCount.count().toInt, truncate = false)

    // aktywność użytkowników
    val topAuthors: DataFrame = tweetsAnalyzer.calculateTopAuthors(cleanedTweetsDF, topN = 20)
    topAuthors.show(numRows = topAuthors.count().toInt, truncate = false)

    // top 10 użytkowników z największą aktywnością w zależności od wieku konta
    val userAgeVsActivity: DataFrame = tweetsAnalyzer.calculateUserAgeVsActivity(cleanedTweetsDF)
    userAgeVsActivity
      .orderBy(desc("avg_account_age_days"))
      .show(numRows = 10, truncate = false)

    // top 10 użytkowników z największą liczbą tweetów
    val userTweetsCountVsActivity: DataFrame = tweetsAnalyzer.calculateUserAgeVsActivity(cleanedTweetsDF)
    userTweetsCountVsActivity
      .orderBy(desc("tweets_count"))
      .show(numRows = 10, truncate = false)


    
    // val grammysTweets = tweetsLoader.loadGrammysTweets()
    // grammysTweets.show()
    // println(s"Number of rows: ${grammysTweets.count()}")
    // grammysTweets.printSchema()

    // val financialsTweets = tweetsLoader.loadFinancialsTweets()
    // financialsTweets.show()
    // println(s"Number of rows: ${financialsTweets.count()}")
    // financialsTweets.printSchema()

    // val covid19Tweets = tweetsLoader.loadCovid19Tweets()
    // covid19Tweets.show()
    // println(s"Number of rows: ${covid19Tweets.count()}")
    // covid19Tweets.printSchema()

    spark.stop()

  }

}