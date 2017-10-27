package com.batpetra.utility

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.DataTypes.{DoubleType, FloatType, IntegerType, StringType}

import scala.collection.{Map => collectionMap}
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.types.{StructType => structType}

/**
  * Created by 25548 on 7/4/2017.
  * Dinesh Irranki
  */
class GeniricFunctions {

  //To get the currentdate
  def GetTodaysDate(): String = {
    val simpleDateFormat: SimpleDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String.valueOf(simpleDateFormat.format(new Date()))
  }

  def GetbatchID(): Int = {
    val simpleDateFormat: SimpleDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd");
    (simpleDateFormat.format(new Date())).asInstanceOf[Int]
  }
  //Global Logger
  def GetLogger(): org.apache.log4j.Logger = {
    org.apache.log4j.LogManager.getRootLogger
  }

  //Create the schema for dataframe from metadatalist provided in the Property file
  def GenerateSchemaFromPropertyFile(metaDataType:collectionMap[String, String], sparkContext: SparkContext): structType = {
    new org.apache.spark.sql.types.StructType(metaDataType.map(x => new org.apache.spark.sql.types.StructField(x._1, GetDataType(x._2), nullable = true)).toArray)
  }

  //To get the actual datatype from the proprtyfile
  def GetDataType(p: String): org.apache.spark.sql.types.DataType = {
    var dataType: String = ""
    if (p.contains("]")) dataType = p.replace("]", "") else dataType = p
    val columnType: org.apache.spark.sql.types.DataType = dataType match {

      case "StringType" => StringType
      case "IntegerType" => IntegerType
      case "FloatType" => FloatType
      case "DoubleType" => DoubleType
      case _ => StringType
    }
    return columnType
  }

  def getProperyMap(path: String, section: String, sc: SparkContext,pos:Tuple2[Int,Int]): scala.collection.Map[String, String] = {
    val rdd: List[String] = sc.textFile(path).toLocalIterator.toList
    val sectionMap = scala.collection.mutable.LinkedHashMap[String, String]()
    rdd.filter(x => x.contains(section.toString())).map(y => sectionMap.put(y.split(":=")(pos._1).trim(), y.split(":=")(pos._2).trim()))
    sectionMap
  }

  def ValidateForLineLength(length: Int,cond:Boolean,lineSize:Int): Boolean =  {
    if(cond) {
      if (length != lineSize) true else false
    }
    else
    {
      if (length != lineSize) false else true
    }
  }

  def ValidateForLineLength(length: Int,lineSize:Int) = if(length == lineSize) true else false



  def ConvertRowtoTuple(row: Array[String], metadata: List[String]) = {
    var bRef = true
    var message:String = null
    val length = metadata.size
    var singleRowElements: ArrayBuffer[Any] = new ArrayBuffer[Any]()

    try {
      for (i <- 0 until metadata.size) {
        val rowData =StringToDataType(row(i), metadata(i))
        singleRowElements.append(rowData)
      }
    }
    catch {
      case e: NumberFormatException => {println(e.printStackTrace()); bRef = false; message = "Exception " + e.getMessage() }
      case e: ClassCastException => {println(e.printStackTrace()); bRef = false; message = "Exception " + e.getMessage() }
      case e: SimpleDateFormat => {println(e.printStackTrace()); bRef = false; message = "Exception " + e.getMessage() }
      case e: Exception => {println(e.printStackTrace()); bRef = false; message = "Exception " + e.getMessage() }

    }

    if(bRef) org.apache.spark.sql.Row.fromSeq(singleRowElements)
    else {
      val updateRow: Array[String] = row :+ message
      //UpdateExceptionRows(updateRow)
      org.apache.spark.sql.Row.fromSeq(updateRow)
    }
  }

  def StringToDataType(rowElement: String, rowElementType: String): Any = {
    val data: Any = rowElementType match {
      case "StringType" => rowElement.toString()
      case "IntegerType" => rowElement.toInt
      case "FloatType" => rowElement.toFloat
      case "DoubleType" => rowElement.toDouble
      case _ => rowElement.toString()
    }
    data
  }

  def validateNonNullableColumns(line:Array[String], nonNullableColumnsList:List[String]): Boolean = {
    //test
    var bRef:Boolean = true
    for (i <- 0 until nonNullableColumnsList.size) {
      var index = nonNullableColumnsList(i)
      if( line(index.toInt).isEmpty() || line(index.toInt).equalsIgnoreCase("null")){
        bRef = false
      }
    }
    return bRef
  }

  def GetFolderName(): String = {
    val dateString: String = new java.text.SimpleDateFormat("yyyy/MM/dd").format(new Date()).asInstanceOf[String]
    val cal : java.util.Calendar = java.util.Calendar.getInstance()
    val simpleDateFormat: SimpleDateFormat = new java.text.SimpleDateFormat("HH-mm-ss");
    val TimeString : String = (simpleDateFormat.format(cal.getTime())).asInstanceOf[String]
    dateString + "/" + TimeString
  }

  def GetQueryFromPropertyFile(pathToFile: String, sparkContext: SparkContext, queryName: String): String = {
    var queryValue: String = ""
    var PropertyFile: List[String] = sparkContext.textFile(pathToFile).toLocalIterator.toList
    PropertyFile.foreach { case line => {
      if (line.split(":=")(0).equalsIgnoreCase(queryName)) queryValue = line.split(":=")(1)
    }
    }
    return queryValue
  }

  val currentDate: String = new java.text.SimpleDateFormat("yyyy/MM/dd").format(new Date()).asInstanceOf[String]

}























//Read property file and split it based on the
//def ReadPropertyFile(pathToFile: String, sparkContext: SparkContext): Map[String, String] = {
//  val PropertyFile = sparkContext.textFile(pathToFile).toLocalIterator.toList
//  Map(PropertyFile.map(p => (p.split(":=")(0), p.split(":=")(1))): _*)
//}

//def GenerateSchema(schemaTupleList: List[(String, org.apache.spark.sql.types.DataType)]) = {
//  new org.apache.spark.sql.types.StructType(schemaTupleList.map(x => new org.apache.spark.sql.types.StructField(x._1, x._2, nullable = false)).toArray)
//}


//def UpdateExceptionRows(row:Array[String]) = {println("Exception in rows"); ProductDimention.exceptionRecords.append(row)}