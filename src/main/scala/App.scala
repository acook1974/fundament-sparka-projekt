import org.apache.spark.sql.SparkSession
import loaders.TweetsLoader
import loaders.FinancialsLoader
import loaders.Covid19Loader

object App {  

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
        .appName("fundament-sparka")
        .master("local")
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") 

    val tweets = TweetsLoader.loadGrammysTweets(spark)
    tweets.show()
    println(s"Number of rows: ${tweets.count()}")
    tweets.printSchema()

    val financials = FinancialsLoader.loadFinancials(spark)
    financials.show()
    println(s"Number of rows: ${financials.count()}")
    financials.printSchema()

    val covid19 = Covid19Loader.loadCovid19(spark)
    covid19.show()
    println(s"Number of rows: ${covid19.count()}")
    covid19.printSchema()

    spark.stop()

  }

}