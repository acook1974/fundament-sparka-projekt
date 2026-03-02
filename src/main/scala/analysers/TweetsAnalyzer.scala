package analysers

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, LongType}

object TweetsAnalyzerConstants {
  val HASHTAG_COLUMN: String = "hashtags"
  val IS_RETWEET_COLUMN: String = "is_retweet"
  val SOURCE_COLUMN: String = "source"
  val USER_FAVOURITES_COLUMN: String = "user_favourites"
  val USER_NAME_COLUMN: String = "user_name"
  val USER_LOCATION_COLUMN: String = "user_location"
  val USER_FOLLOWERS_COLUMN: String = "user_followers"
}

class TweetsAnalyzer(spark: SparkSession) {

    /** Oblicza częstość występowania hashtagów w tweetach.
      * Rozwija kolumnę hashtagów (explode_outer), grupuje po hashtagu i zlicza wystąpienia.
      *
      * @param tweets DataFrame z tweetami (musi zawierać kolumnę "hashtags")
      * @return DataFrame z kolumnami: hashtags, count
      */
    def calculateHashtagsFrequency(tweets: DataFrame): DataFrame = {
        tweets
            .withColumn(TweetsAnalyzerConstants.HASHTAG_COLUMN, explode_outer(col(TweetsAnalyzerConstants.HASHTAG_COLUMN)))
            .groupBy(TweetsAnalyzerConstants.HASHTAG_COLUMN)
            .count()
    }

    /** Oblicza częstość tweetów vs retweetów.
      * Grupuje wiersze po kolumnie is_retweet (true/false) i zlicza wystąpienia każdej wartości.
      *
      * @param tweets DataFrame z tweetami (musi zawierać kolumnę "is_retweet")
      * @return DataFrame z kolumnami: is_retweet, count
      */
    def calculateIsRetweetCount(tweets: DataFrame): DataFrame = {
        tweets
            .groupBy(TweetsAnalyzerConstants.IS_RETWEET_COLUMN)
            .count()
    }

    /** Oblicza częstość występowania źródeł tweetów (np. klientów: aplikacja, przeglądarka).
      * Grupuje wiersze po kolumnie source i zlicza wystąpienia każdego źródła.
      *
      * @param tweets DataFrame z tweetami (musi zawierać kolumnę "source")
      * @return DataFrame z kolumnami: source, count
      */
    def calculateSourceFrequency(tweets: DataFrame): DataFrame = {
        tweets
            .groupBy(TweetsAnalyzerConstants.SOURCE_COLUMN)
            .count()
    }

    /** Oblicza średnią liczbę obserwujących (followers) użytkowników wg lokalizacji.
      * Wybiera user_name, user_followers, user_location, usuwa duplikaty po użytkowniku (jeden wiersz na użytkownika),
      * grupuje po user_location i liczy średnią z user_followers.
      *
      * @param tweets DataFrame z tweetami (kolumny: user_name, user_followers, user_location)
      * @return DataFrame z kolumnami: user_location, avg(user_followers)
      */
    def calculateAvgUserFollowersPerLocation(tweets: DataFrame): DataFrame = {
        tweets
            .select(TweetsAnalyzerConstants.USER_NAME_COLUMN, TweetsAnalyzerConstants.USER_FOLLOWERS_COLUMN, TweetsAnalyzerConstants.USER_LOCATION_COLUMN)
            .filter(col(TweetsAnalyzerConstants.USER_NAME_COLUMN).isNotNull)
            .filter(col(TweetsAnalyzerConstants.USER_LOCATION_COLUMN).isNotNull)
            .dropDuplicates(TweetsAnalyzerConstants.USER_NAME_COLUMN)
            .groupBy(TweetsAnalyzerConstants.USER_LOCATION_COLUMN)
            .avg(TweetsAnalyzerConstants.USER_FOLLOWERS_COLUMN)
            .alias("avg")
    }

}
