package scd

import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object SparkSCD1 {

  case class tableRecords(id:String,name:String,order:Int,pod:String)
  def main(args: Array[String]): Unit = {

//    println(Class.forName("SparkSCD1"))
    val path = args(0)

    val spark: SparkSession = SparkSession
      .builder()
      //.master("local[*]")
      .appName("oci-sample")
      .config("spark.default.parallelism", 4)
     // .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .getOrCreate()



    import spark.implicits._

    val seqOfMainRecords = Seq(
      tableRecords("1","sharad",7,"eng"),
      tableRecords("2","Raj",8,"eng"),
      tableRecords("3","Maciej",9,"eng"),
      tableRecords("4","Abhishek",10,"eng"),
      tableRecords("5","Bhupesh",11,"eng"),
      tableRecords("6","Rajat",12,"eng"),
      tableRecords("7","vishal",12,"eng"),
      tableRecords("8","Mayank",13,"eng"),
      tableRecords("9","Kajari",14,"eng"),
      tableRecords("10","Vaishali",7,"eng"),
      tableRecords("11","Arpan",7,"eng"),
    )

    val seqOfDeltaRecords = Seq(
      tableRecords("6","Rajat",6,"eng-leader"),
      tableRecords("9","shagun",9,"Marketing"),
      tableRecords("2","Raj",2,"eng-leader"),
      tableRecords("7","vishal",7,"eng-leader"),
    )

    val mainDF = seqOfMainRecords.toDF()
    val deltaDF = seqOfDeltaRecords.toDF()

    //mainDF.write.format("delta").mode("overwrite").save("/tmp/prophecy-empl")
    //val updateUsingDeltaDF = scd1MergeWithDelta("/tmp/prophecy-empl",deltaDF,"id")
    //updateUsingDeltaDF.show()
    val outputDF = scd1MergeWithoutDelta(mainDF,deltaDF,"id")
    outputDF.write.format("csv").mode("overwrite").save(path)


  }



  //Assumption natural key is present in DataSet currently one column but can easily manipulate for List
//  def scd1MergeWithDelta(deltaMainTablePath:String, deltaDF:DataFrame, naturalKey:String):DataFrame={
//    val mainTable = DeltaTable.forPath(deltaMainTablePath)
//    mainTable.as("mainTable").merge(
//      deltaDF.as("newData"),
//      "mainTable.id = newData.id").whenMatched
//      .updateAll().whenNotMatched.insertAll()
//      .execute()
//
//    mainTable.toDF
//  }


  def   scd1MergeWithoutDelta(mainDF:DataFrame,cdcDF:DataFrame,naturalKey:String): DataFrame ={
    //ideally you have to dedupe cdcDF before join so that unique records will be there
    val columnCollection = cdcDF.columns
    val joinDF = mainDF.as("main").
      join(cdcDF.as("cdc"),expr("main."+naturalKey+"="+"cdc."+naturalKey),"fullouter")
    val mergeColumnSequence = generateMergeColumnsExpression("id",columnCollection,"cdc")
    // val updatedDF=joinDF.filter(col("cdc.id").isNotNull).select(("cdc.*")).union(joinDF.filter(col("cdc.id").isNull).select(("main.*")))
    val updatedDF = joinDF.select(mergeColumnSequence:_*)
    //val  updatedDF = joinDF.select(when(col("cdc."+naturalKey).isNotNull,"cdc.*").otherwise("main.*"))
    //updatedDF.show()
    updatedDF
  }

  def generateMergeColumnsExpression(rowIdColumnName:String,columnCollection:Seq[String],prefix:String): List[Column] ={
    columnCollection.map(columnName=>expr(
      s"""if($prefix.$rowIdColumnName IS NULL, main.$columnName,$prefix.$columnName) AS $columnName"""
    )).toList
  }

}
