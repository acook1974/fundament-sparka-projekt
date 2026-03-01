package loaders

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object FinancialsLoader {
  
  def loadFinancials(spark: SparkSession): DataFrame = {
    spark.read
        .option("header", "true")
        .csv("data/financial.csv")
  }

}
