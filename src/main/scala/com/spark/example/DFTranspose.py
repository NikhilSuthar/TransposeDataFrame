from pyspark.sql.functions import *
from pyspark.sql import SparkSession



def TransposeDF(df, columns, pivotCol):
...     columnsValue = list(map(lambda x: str("'") + str(x) + str("',")  + str(x), columns))
...     stackCols = ','.join(x for x in columnsValue)
...     df_1 = df.selectExpr(pivotCol, "stack(" + str(len(columns)) + "," + stackCols + ")")\
...              .select(pivotCol, "col0", "col1")
...     final_df = df_1.groupBy(col("col0")).pivot(pivotCol).agg(concat_ws("", collect_list(col("col1"))))\
...                    .withColumnRenamed("col0", pivotCol)
...     return final_df

def main():
    DataList = [("Shirts", 10, 13, 34, 10), ("Trousers", 11, 2, 30, 20), ("Pants", 70, 43, 24, 60), ("Sweater", 101, 44, 54, 80)]
    productQtyDF  = spark.createDataFrame(DataList, ["Products", "Small", "Medium", "Large", "ExLarge"])
    productQtyDF.show()
    #+--------+-----+------+-----+-------+
    #|Products|Small|Medium|Large|ExLarge|
    #+--------+-----+------+-----+-------+
    #|  Shirts|   10|    13|   34|     10|
    #|Trousers|   11|     2|   30|     20|
    #|   Pants|   70|    43|   24|     60|
    #| Sweater|  101|    44|   54|     80|
    #+--------+-----+------+-----+-------+



    productTypeDF = TransposeDF(productQtyDF, ["Small", "Medium", "Large", "ExLarge"], "Products")
    productTypeDF.show(truncate= False)
    #+--------+-----+------+-------+--------+
    #|Products|Pants|Shirts|Sweater|Trousers|
    #+--------+-----+------+-------+--------+
    #|Medium  |43   |13    |44     |2       |
    #|Small   |70   |10    |101    |11      |
    #|ExLarge |60   |10    |80     |20      |
    #|Large   |24   |34    |54     |30      |
    #+--------+-----+------+-------+--------+


if __name__ == '__main__':
    main()
