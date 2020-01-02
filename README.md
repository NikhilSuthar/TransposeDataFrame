# TransposeDataFrame
Example of Spark Method to Transpose input DataFrame

 <h2>What is Transpose?</h2>
  
  *The **transpose** of a `Dataframe` is a new `DataFrame` whose rows are the columns of the original DataFrame. 
  (This makes the columns of the new DataFrame the rows of the original).*
  
  Here is the DataFrame Example:
  
  Suppose we have Input DataFrame `df` as below:
  
      +--------+-----+------+-----+-------+
      |Products|Small|Medium|Large|ExLarge|
      +--------+-----+------+-----+-------+
      |Shirts  |10   |13    |34   |10     |
      |Trousers|11   |2     |30   |20     |
      |Pants   |70   |43    |24   |60     |
      |Sweater |101  |44    |54   |80     |
      +--------+-----+------+-----+-------+



  Then below DataFrame will be Transpose DataFrame of `df`
  
      +--------+-----+------+-------+--------+
      |Products|Pants|Shirts|Sweater|Trousers|
      +--------+-----+------+-------+--------+
      |Medium  |43   |13    |44     |2       |
      |Small   |70   |10    |101    |11      |
      |ExLarge |60   |10    |80     |20      |
      |Large   |24   |34    |54     |30      |
      +--------+-----+------+-------+--------+
      
 
 
 <h1>TransposeDF</h1>
 
 **TransposeDF** is the Method written to convert input DataFrame into Transposed DataFrame.
 It take three below parameters and return new Transposed DataFrame:
    
   **TransposeDF(df: DataFrame, columns: Seq[String], pivotCol: String)**
     
   * First parameter is input DataFrame (eg: `df` in above example.)
   * Second Parameter is Sequence of columns of Input DataFrame that need to transpose into rows. (eg: Seq("Small", "Medium", "Large", "ExLarge") in above example)
   * Third Parameter is pivot column (column which rows required to transpose into columns). (eg. "Products" in above example)
   
   
 <h1>How to use TransposeDF </h1>
    
   It is very easy to use. You just need to copy **TransposeDF** method from [here](https://github.com/NikhilSuthar/TransposeDataFrame/blob/master/src/main/scala/com/spark/example/DFTranspose.scala) to your code and call it as below:
    
    TransposeDF(df, Seq("Small", "Medium", "Large", "ExLarge"), "Products")
    
   OR
   
    val ColumnSeq:Seq[String] =  Seq("Small", "Medium", "Large", "ExLarge")   
    val transDF = TransposeDF(df,ColumnSeq, "Products")
    
     
