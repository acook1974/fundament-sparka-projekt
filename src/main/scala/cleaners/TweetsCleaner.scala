package cleaners

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, LongType}

class TweetsCleaner(spark: SparkSession) {

  /** Czyści i przygotowuje dane tweetów do dalszej analizy.
    * Usuwa znaki specjalne z kolumny hashtags i zamienia ją na tablicę stringów,
    * rzutuje kolumny dat na DateType oraz kolumny liczbowe użytkownika na LongType.
    *
    * @param tweets surowy DataFrame z tweetami
    * @return DataFrame z przetworzonymi kolumnami (hashtags jako tablica, daty i liczby w poprawnych typach)
    */
  def cleanAllTweets(tweets: DataFrame): DataFrame = {
    
    tweets
        .withColumn("hashtags", regexp_replace(col("hashtags"), "['\\[\\]]", ""))
        .withColumn("hashtags", split(col("hashtags"), ","))
        .withColumn("date", col("date").cast(DateType))
        .withColumn("user_created", col("user_created").cast(DateType))
        .withColumn("user_followers", col("user_followers").cast(LongType))
        .withColumn("user_friends", col("user_friends").cast(LongType))
        .withColumn("user_favourites", col("user_favourites").cast(LongType))
  }

}
