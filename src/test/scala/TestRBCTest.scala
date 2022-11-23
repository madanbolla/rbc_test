import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}
import org.scalatest.{BeforeAndAfterAll, FunSuite}


class TestRBCTest extends FunSuite with BeforeAndAfterAll{

  @transient var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("TestRBCTest").master("local")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("Product file loaded properly") {
    val productDF = RBCTest.loadProductDF()
    val result = productDF.count()
    result shouldBe 598337
  }

  test("Location file loaded properly") {
    val locationDF = RBCTest.loadLocationDF()
    val result = locationDF.count()
    result shouldBe 883
  }

  test("Transaction file loaded properly") {
    val transactionDF = RBCTest.loadTransactionDF()
    val result = transactionDF.count()
    result shouldBe 200000
  }

  test("Test total Sales") {
    val totalSales = RBCTest.totalSalesDF()
    totalSales.select("TotalSales").collect.foreach(row => row.getDouble(0) shouldBe 6434452.360000876)
  }

  test("Test top stores with highest sales") {
    val topStoresWithHighestSales = RBCTest.topNStoresWithHighestSales()
    val result = topStoresWithHighestSales.select("store_num", "TotalSales")
      .where(col("store_num").isin(List("8142", "6973", "9807", "1396"):_*))
      .collect.map(_.getValuesMap(Seq("store_num", "TotalSales")))

    result should contain allOf(
      Map("store_num" -> 8142, "TotalSales" -> "531,989.22"),
      Map("store_num" -> 6973, "TotalSales" -> "514,892.50"),
      Map("store_num" -> 9807, "TotalSales" -> "288,156.60"),
      Map("store_num" -> 1396, "TotalSales" -> "200,716.25")
    )
  }

  test("Test top categories with highest sales") {
    val topCategoriesWithHighestSales = RBCTest.topNCategoriesWithHighestSales()
    val result = topCategoriesWithHighestSales.select("category", "TotalSales")
      .where(col("category").isin(List("a8a688f9", "687ed9e3", "fe148072", "ffcec4a7"):_*))
      .collect.map(_.getValuesMap(Seq("category", "TotalSales")))

    result should contain allOf(
      Map("category" -> "a8a688f9", "TotalSales" -> "81,216.75"),
      Map("category" -> "687ed9e3", "TotalSales" -> "53,517.81"),
      Map("category" -> "fe148072", "TotalSales" -> "26,523.73"),
      Map("category" -> "ffcec4a7", "TotalSales" -> "25,969.24")
    )
  }

  test("Test top stores by province with highest sales") {
    val topStoresByProvDF = RBCTest.topPerformingStoresByProvince(5)
    val result = topStoresByProvDF.select("province", "store_num", "TotalSales", "avgProvSales")
      .where(col("store_num").isin(List("7167", "7317", "9807", "4823", "8142"):_*))
      .collect.map(_.getValuesMap(Seq("province", "store_num", "TotalSales", "avgProvSales")))

    result should contain allOf(
      Map("province" -> "BRITISH COLUMBIA", "store_num" -> 7167, "TotalSales" -> "44,538.91", "avgProvSales" -> "22,587.24"),
      Map("province" -> "SASKATCHEWAN", "store_num" -> 7317, "TotalSales" -> "65,880.18", "avgProvSales" -> "35,704.80"),
      Map("province" -> "ALBERTA", "store_num" -> 9807, "TotalSales" -> "288,156.60", "avgProvSales" -> "47,782.63"),
      Map("province" -> "MANITOBA", "store_num" -> 4823, "TotalSales" -> "104,918.47", "avgProvSales" -> "35,065.83"),
      Map("province" -> "ONTARIO", "store_num" -> 8142, "TotalSales" -> "531,989.22", "avgProvSales" -> "86,268.89")
    )
  }

  test("Test top categories by department with highest sales") {
    val topCategoriesByDeptDF = RBCTest.topPerformingCategoriesByDept(5)
    val result = topCategoriesByDeptDF.select("department", "category", "TotalSales")
      .where(col("department") === "5bffa719")
      .collect().map(_.getValuesMap(Seq("department", "category", "TotalSales")))

    result should contain allOf(
      Map("department" -> "5bffa719", "category" -> "fa3a1bd8", "TotalSales" -> "5,307.62"),
      Map("department" -> "5bffa719", "category" -> "a4d52407", "TotalSales" -> "3,996.28"),
      Map("department" -> "5bffa719", "category" -> "6c504249", "TotalSales" -> "3,831.84"),
      Map("department" -> "5bffa719", "category" -> "a121fb78", "TotalSales" -> "3,708.47"),
      Map("department" -> "5bffa719", "category" -> "3ae24ac2", "TotalSales" -> "1,131.71")
    )
  }

}
