import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.util.Try

object RBCTest {

  val productPath = "src/main/resources/data/rbcfiles/product.csv"
  val locationPath = "src/main/resources/data/rbcfiles/location.csv"
  val transactionPath = "src/main/resources/data/rbcfiles/trans*.csv"

  val spark = SparkSession.builder()
    .appName("RBC Test")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  def loadProductDF() = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(productPath)
  }


  def loadLocationDF() = {
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(locationPath)
  }


  def loadTransactionDF() =  {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(transactionPath)
  }

  //replace nulls with zeros for sales and units in Transactions
  def cleanseTransaction() =
    loadTransactionDF.na.fill(Map(
      "sales" -> 0.0,
      "units" -> 0
    ))


  /**
   * //1. Total Sales
   */
  def totalSalesDF() = {
   loadTransactionDF.selectExpr("sum(sales * units) as TotalSales")
  }

  //1. Total Sales using RDD
  case class transaction(sales: Double, units: Int)

  def totalSalesRDD() = {
    val transactionRDD = spark.sparkContext.textFile("src/main/resources/data/rbcfiles/trans*.csv")
      .map(line => line.split(","))
      .filter(tokens => tokens(0).toLowerCase != "trans_dt" )
      .map(tokens => transaction(Try(tokens(3).toDouble).getOrElse(0.0), Try(tokens(4).toInt).getOrElse(0)))

    val totalSalesRDD = transactionRDD.map(record => record.sales * record.units).reduce(_ + _)
    println(totalSalesRDD)
  }


  /**
   * //2. Top N stores with highest sales
   * @param topN number for the top entries
   */
  def topNStoresWithHighestSales() = {
    val locationDF = loadLocationDF()
    val transDF = cleanseTransaction()

    val joinDF = transDF.join(broadcast(locationDF), Seq("store_location_key"), "inner")
      .selectExpr("store_num", "sales * units as total_sales" )

    val finalDF = joinDF.groupBy("store_num").
      agg(sum("total_sales").as("TotalSales"))
      .orderBy(col("TotalSales").desc)

    finalDF.withColumn("TotalSales", format_number(col("TotalSales"), 2))

  }

  /**
   * //3. Top N Categories with highest sales
   * @param topN number for the top entries
   */
  def topNCategoriesWithHighestSales() = {
    val productDF = loadProductDF()
    val transDF = cleanseTransaction()

    val joinDF = transDF.join(broadcast(productDF), Seq("product_key"), "inner")
      .selectExpr("category", "sales * units as total_sales" )

    val finalDF = joinDF.groupBy("category").
      agg(sum("total_sales").as("TotalSales"))
      .orderBy(col("TotalSales").desc)

    finalDF.withColumn("TotalSales", format_number(col("TotalSales"), 2))

  }

  /**
   * extract top N stores per province based on total sales
   * @param topN top N numner
   */
  def topPerformingStoresByProvince(topN: Int) = {
    val locationDF = loadLocationDF()
    val transDF = cleanseTransaction()

    val joinDF = transDF.join(broadcast(locationDF), Seq("store_location_key"), "inner")
      .selectExpr("province", "store_num", "sales * units as sales" )

    val totalSalesByStoreDF = joinDF.groupBy("province", "store_num").
      agg(sum("sales").as("TotalSales"))
      .orderBy(col("TotalSales").desc)

    val winone = Window.partitionBy("province").orderBy(col("TotalSales").desc)
    val wintwo = Window.partitionBy("province")

    val salesByProvStoreDF = totalSalesByStoreDF.withColumn("row_num", row_number() over(winone))
      .withColumn("avgProvSales", avg("TotalSales").over(wintwo))
      .where(col("row_num") <= topN).drop("row_num")

    salesByProvStoreDF.withColumn("TotalSales", format_number(col("TotalSales"), 2))
      .withColumn("avgProvSales", format_number(col("avgProvSales"), 2))
  }

  /**
   * extract top N categories per department based on total sales
   * @param topN top N numner
   */
  def topPerformingCategoriesByDept(topN: Int) = {
    val prodcutDF = loadProductDF()
    val transDF = cleanseTransaction()

    val joinDF = transDF.join(broadcast(prodcutDF), Seq("product_key"), "inner")
      .selectExpr("department", "category", "sales * units as sales" )

    val totalSalesByCategoryDF = joinDF.groupBy("department", "category").
      agg(sum("sales").as("TotalSales"))

    val winone = Window.partitionBy("department").orderBy(col("TotalSales").desc)

    val salesByDeptCategoryDF = totalSalesByCategoryDF.withColumn("row_num", row_number() over(winone))
      .where(col("row_num") <= topN).drop("row_num")

    salesByDeptCategoryDF.withColumn("TotalSales", format_number(col("TotalSales"), 2))

  }

  def storeAsParquet(df: DataFrame, outputPath: String) = {
    df.repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .parquet(outputPath)
  }

  def main(args: Array[String]) : Unit = {
    totalSalesRDD()
    totalSalesDF().show()
    topNStoresWithHighestSales().show(20)
    topNCategoriesWithHighestSales().show(20)
    topPerformingStoresByProvince(5).show()
    topPerformingCategoriesByDept(5).show()
  }

}
