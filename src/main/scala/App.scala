import org.apache.spark.sql.SparkSession
import loaders.TweetsLoader

object App {  

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
        .appName("fundament-sparka")
        .master("local")
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") 

    val tweetsLoader = new TweetsLoader(spark)
    
    val grammysTweets = tweetsLoader.loadGrammysTweets()
    grammysTweets.show()
    println(s"Number of rows: ${grammysTweets.count()}")
    grammysTweets.printSchema()

    val financialsTweets = tweetsLoader.loadFinancialsTweets()
    financialsTweets.show()
    println(s"Number of rows: ${financialsTweets.count()}")
    financialsTweets.printSchema()

    val covid19Tweets = tweetsLoader.loadCovid19Tweets()
    covid19Tweets.show()
    println(s"Number of rows: ${covid19Tweets.count()}")
    covid19Tweets.printSchema()

    spark.stop()

  }

}