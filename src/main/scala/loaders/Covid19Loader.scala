package loaders

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Covid19Loader {
  
  def loadCovid19(spark: SparkSession): DataFrame = {
    spark.read
        .option("header", "true")
        .csv("data/covid19_tweets.csv")
  }

}
