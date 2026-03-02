package tweetssearch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object TweetsSearchConstants {
    val TEXT_COLUMN: String = "text"
    val USER_LOCATION_COLUMN: String = "user_location"
}

class TweetsSearch(spark: SparkSession) {
  
  /** Wyszukuje tweety zawierające jedno podane słowo kluczowe.
    * Zwraca tylko te wiersze, w których kolumna text zawiera przekazane słowo.
    *
    * @param keyWord słowo kluczowe, po którym ma być wykonane wyszukiwanie
    * @param tweets DataFrame z tweetami (musi zawierać kolumnę "text")
    * @return DataFrame z tweetami zawierającymi podane słowo kluczowe
    */
  def searchByKeyWord(keyWord: String)(tweets: DataFrame): DataFrame = {
    tweets
        .filter(col(TweetsSearchConstants.TEXT_COLUMN).contains(keyWord))
  }

  /** Wyszukuje tweety zawierające co najmniej jedno ze wskazanych słów kluczowych.
    * Tworzy pomocniczą kolumnę z przecięciem słów z tekstu i listy słów kluczowych,
    * a następnie filtruje tylko te tweety, gdzie to przecięcie nie jest puste.
    *
    * @param keyWords lista słów kluczowych, które mają zostać wyszukane
    * @param tweets DataFrame z tweetami (musi zawierać kolumnę "text")
    * @return DataFrame z tweetami, które zawierają przynajmniej jedno ze wskazanych słów kluczowych
    */
  def searchByKeyWords(keyWords: Seq[String])(tweets: DataFrame): DataFrame = {
    tweets
        .withColumn("keyWordsResult", array_intersect(split(col(TweetsSearchConstants.TEXT_COLUMN), " "), split(lit(keyWords), ",")))
        .filter(!(col("keyWordsResult").isNull.or(size(col("keyWordsResult")).equalTo(0))))
        .drop("keyWordsResult")
  }

  /** Filtruje tweety do tych pochodzących z konkretnej lokalizacji użytkownika.
    *
    * @param location nazwa lokalizacji (wartość kolumny "user_location"), po której filtrujemy
    * @param tweets DataFrame z tweetami (musi zawierać kolumnę "user_location")
    * @return DataFrame zawierający tylko tweety z zadanej lokalizacji
    */
  def onlyInLocation(location: String)(tweets: DataFrame): DataFrame = {
    tweets
        .filter(col(TweetsSearchConstants.USER_LOCATION_COLUMN).equalTo(location))
  }

}
