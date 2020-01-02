package com.spark.example

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, collect_list, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DFTranspose extends Logging {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("Spark DataFrame Transpose").master("local[*]").getOrCreate()
    val DataSeq = Seq(("Shirts", 10, 13, 34, 10), ("Trousers", 11, 2, 30, 20), ("Pants", 70, 43, 24, 60), ("Sweater", 101, 44, 54, 80))
    import spark.implicits._
    val productQtyDF = DataSeq.toDF("Products", "Small", "Medium", "Large", "ExLarge")

    log.info("Product Qty DataFrame (Input DataFrame)")
    productQtyDF.show(false)

    val productTypeDF = TransposeDF(productQtyDF, Seq("Small", "Medium", "Large", "ExLarge"), "Products")

    log.info("Product Type DataFrame (Transposed DataFrame)")
    productTypeDF.show(false)

  }


  def TransposeDF(df: DataFrame, columns: Seq[String], pivotCol: String): DataFrame = {
    val columnsValue = columns.map(x => "'" + x + "', " + x)
    val stackCols = columnsValue.mkString(",")
    val df_1 = df.selectExpr(pivotCol, "stack(" + columns.size + "," + stackCols + ")")
      .select(pivotCol, "col0", "col1")

    val final_df = df_1.groupBy(col("col0")).pivot(pivotCol).agg(concat_ws("", collect_list(col("col1"))))
      .withColumnRenamed("col0", pivotCol)
    final_df
  }

}
