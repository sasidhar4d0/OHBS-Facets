package com.batpetra.rawtointegrate

import java.util.regex.Pattern
import com.batpetra.rawtointegrate.Constants.ProductConstants
import com.batpetra.utility.GeniricFunctions
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext => petraSqlContext}
import org.apache.spark.sql.types.{StructType => structType}
import org.apache.spark.rdd.{RDD => mainRDD}
import scala.collection.{Map => collectionMap}
/**
  * Created by 25548 on 7/4/2017.
  */
object ProductDimention {

  //Global Variables
  var recordsRead:String = _
  var recordsWrite:String = _
  var recordsRejected:String = _
  var metadata:  scala.collection.Map[String,String] = null
  var dataMap :  scala.collection.Map[String,String] = null
  var queryMap:  scala.collection.Map[String,String] = null
  var scdMap: scala.collection.Map[String,String]    = null
  var factMapDimentionPaths:scala.collection.Map[String,String]    = null
  var factMapDimentionColumns:scala.collection.Map[String,String]    = null
  var dataQuality:scala.collection.Map[String,String]= null
  var nonNullableColumns:scala.collection.Map[String,String]= null
  var errorLogSchema:scala.collection.Map[String,String] =null
  var auditLogSchema:scala.collection.Map[String,String] =null
  var datatypesList: List[String] = null

  val GeniricFunctions = new GeniricFunctions()
  var exceptionRecords:RDD[String] = _
  //~Global Variables


  def CreateSourceDataFrame(sc:SparkContext,sqlContext:petraSqlContext,inputRDD:mainRDD[Array[String]], schema:structType,tempTable:String,datatypesList:List[String],rawInput:mainRDD[Array[String]],batchid:String) ={

    val rowLength = metadata.size
    val mainQuery = queryMap(ProductConstants.MAIN_QUERY)
    val transInputRDD:RDD[Row] = inputRDD.map(row=> GeniricFunctions.ConvertRowtoTuple(row,datatypesList))
    val transformedInputRDD= transInputRDD.filter((row => GeniricFunctions.ValidateForLineLength(row.length,rowLength)))
    exceptionRecords = transInputRDD.filter(row => row.mkString("").contains(ProductConstants.EXCEPTION)).map(x => x.mkString(","))
    val sourceDataFrame = sqlContext.applySchema(transformedInputRDD, schema)
    val tempTable = queryMap(ProductConstants.TEMP_TABLE)
    sourceDataFrame.registerTempTable(tempTable)
    var integrateDataFrame = sqlContext.emptyDataFrame
    //Creation of DataFrame for fact and dimention tables
    if(factMapDimentionPaths.size > 0 && factMapDimentionColumns.size > 0 ){
      integrateDataFrame = FactHandler.CreateFactDataFrame(factMapDimentionPaths,factMapDimentionColumns,sqlContext,sourceDataFrame,mainQuery)
    }
    else{
      integrateDataFrame = sqlContext.sql(mainQuery)
    }
    val createdDate = GeniricFunctions.GetTodaysDate()
    //~Creation of DataFrame for fact and dimention tables

    //Audit Columns for integrated Data
    integrateDataFrame.withColumn("lets_ld_dt", lit(createdDate))
      .withColumn("lets_ld_upd_dt",lit(""))
      .withColumn("lets_ld_bch_id", lit(batchid))
      .withColumn("lets_ld_upd_ bch_id", lit(""))
      .withColumn("lets_eff_strt_dt", lit(createdDate))
      .withColumn("lets_eff_end_dt",lit(""))
      //      .withColumn("lets_src_sys_nm", lit("SAP"))
      .withColumn("lets_atv_flg",lit("Y"))
    //~Audit Columns for integrated Data


  }




  def CreateErrorLog(inputRDD:mainRDD[Array[String]], dataQuality:collectionMap[String,String],sc:SparkContext,sqlContext:petraSqlContext,length:Int,logScheme:collectionMap[String, String])={

    val exceptionRecordsRDD  = exceptionRecords
    val invalidRecordsRdd    = inputRDD.filter { row => if (row.length != length) true else false }
    val invalidLogRDD        = invalidRecordsRdd.map(invalidRecord => Array(dataQuality(ProductConstants.BATCHID),dataQuality(ProductConstants.ACTIVITYID),invalidRecord(0),GeniricFunctions.GetTodaysDate(),ProductConstants.INVALID_ERROR_MESSAGE))
    val exceptionLogRDD      =  exceptionRecordsRDD.map(x => x.split(",")).map(exceptionRecord => Array(dataQuality(ProductConstants.BATCHID),dataQuality(ProductConstants.ACTIVITYID),exceptionRecord(0),GeniricFunctions.GetTodaysDate(),exceptionRecord.last))
    val errorLogRDD          = invalidLogRDD.union(exceptionLogRDD)
    errorLogRDD.map(x => x.mkString(","))
  }

  def CreateAuditLog(auditRecord:ArrayBuffer[String],sc:SparkContext)={
    val record = auditRecord.mkString(",")
    sc.parallelize(List(record))
  }


  def main(args: Array[String]): Unit = {
//    if(args.length != 3)
//    {
//      println("All Parameters not provided. Arguments to be passed are ");
//      System.exit(0)
//    }

   //Input Parameters
    val schemaPropertyPath ="adl://batadlspdnepetradev01.azuredatalakestore.net/Development/Config/rusianmetadata.txt"//"adl://batadlspdnepetradev01.azuredatalakestore.net/PetraPOC/PropertyFiles/ActualAssortmentFact.txt"   //args(0)  //args(0) // //
    val rawInputPath       ="adl://batadlspdnepetradev01.azuredatalakestore.net/Development/Input/rusiandata.csv"//"adl://batadlspdnepetradev01.azuredatalakestore.net/PetraPOC/Raw/ActualAssortment/*"  //args(1)
    var outputPath         ="adl://batadlspdnepetradev01.azuredatalakestore.net/Development/Fact7"//args(2)
    val errorRecordsPath   ="adl://batadlspdnepetradev01.azuredatalakestore.net/PetraPOC/Error/FactError7"  //args(3)
    val auditRecordsPath   ="adl://batadlspdnepetradev01.azuredatalakestore.net/PetraPOC/Audit/FactAudit7"//args(4)
    //~Input Parameters

    //Spark Parameters
    //val sparkConf = new SparkConf().setAppName("MyLocalApp").setMaster("local[*]")
    val sparkConf          = new SparkConf().setAppName("DimentionHandler")
    val sc                 = new SparkContext(sparkConf)
    val sqlContext         = new SQLContext(sc)
    //~Spark Parameters


    //Date Retrieved from the Property File
    metadata                = GeniricFunctions.getProperyMap(schemaPropertyPath,ProductConstants.METADATA,sc,(1,2))
    dataMap                 = GeniricFunctions.getProperyMap(schemaPropertyPath,ProductConstants.DATA,sc,(1,2))
    queryMap                = GeniricFunctions.getProperyMap(schemaPropertyPath,ProductConstants.QUERY,sc,(1,2))
    scdMap                  = GeniricFunctions.getProperyMap(schemaPropertyPath,ProductConstants.SCD,sc,(1,2))
    factMapDimentionPaths   = GeniricFunctions.getProperyMap(schemaPropertyPath,ProductConstants.FACTDATA,sc,(1,2))
    factMapDimentionColumns = GeniricFunctions.getProperyMap(schemaPropertyPath,ProductConstants.FACTDATA,sc,(1,3))
    errorLogSchema          = GeniricFunctions.getProperyMap(schemaPropertyPath,ProductConstants.ERROR_SCHEMA,sc,(1,2))
    auditLogSchema          = GeniricFunctions.getProperyMap(schemaPropertyPath,ProductConstants.AUDIT_SCHEMA,sc,(1,2))
    dataQuality             = GeniricFunctions.getProperyMap(schemaPropertyPath,ProductConstants.DATAQUALITY,sc,(1,2))
    nonNullableColumns      = GeniricFunctions.getProperyMap(schemaPropertyPath,ProductConstants.NONNULLABLE,sc,(1,2))
    datatypesList           = metadata.map(x => x._2).toList
    var nonNullableColumnsList:List[String] =  nonNullableColumns.map(x => x._2).toList
    //~Date Retrieved from the Property File



      if(factMapDimentionPaths.size > 0 && factMapDimentionColumns.size > 0 ){
        outputPath = outputPath + "/" + GeniricFunctions.currentDate
      }
        else{
        outputPath = outputPath
      }

    //Local variables
    val schema              = GeniricFunctions.GenerateSchemaFromPropertyFile(metadata, sc)
    val startTime           = GeniricFunctions.GetTodaysDate()
    val separator           = dataMap(ProductConstants.DATASEPARATOR)
    val rowLength           = metadata.size
    val query               = queryMap(ProductConstants.TEMP_TABLE)
    val batchId             = dataQuality(ProductConstants.BATCHID)
    val activityId          = dataQuality(ProductConstants.ACTIVITYID)
    val errorLogPath        = errorLogSchema(ProductConstants.PATH)
    val auditLogPath        = auditLogSchema(ProductConstants.PATH)
    //~Local Variables

    //Data Processing Functions
    val rawInputRDD         = sc.textFile(rawInputPath)
    val inputRDD            = rawInputRDD.map(line => line.split(Pattern.quote(separator),-1))

    val validatedRecordsRDD = inputRDD.filter(line => {
      nonNullableColumnsList.foreach(println)
      GeniricFunctions.ValidateForLineLength(line.length,rowLength) && GeniricFunctions.validateNonNullableColumns(line,nonNullableColumnsList)
    }
    )

    val sourceDataFrame     = CreateSourceDataFrame(sc,sqlContext,validatedRecordsRDD,schema,query,datatypesList,inputRDD,batchId)
    val endTime             = GeniricFunctions.GetTodaysDate()
    val rejectedRecordsRDD  = CreateErrorLog(inputRDD,dataQuality,sc,sqlContext,rowLength,errorLogSchema)
    //~Data Processing Functions

    //Metrics
    recordsRead             = inputRDD.count().toString()
    recordsWrite            = sourceDataFrame.count().toString()
    recordsRejected         = rejectedRecordsRDD.count().toString()
    //~Metrics

    //Metrics calculation
    var auditLogArray: ArrayBuffer[String]  = new ArrayBuffer[String]()
    auditLogArray.append(batchId, activityId, ProductConstants.INPROGRESS, startTime ,endTime , recordsRead ,recordsWrite,recordsRejected)
    val auditLogRdd = CreateAuditLog(auditLogArray,sc)
    //~Metrics calculation

    //Outputs
    //sourceDataFrame.write.option(ProductConstants.HEADER,true).option(ProductConstants.DELIMITER,separator).csv(outputPath)
    sourceDataFrame.write.option(ProductConstants.HEADER,true).mode("overwrite").option(ProductConstants.DELIMITER,separator).csv(outputPath)
    rejectedRecordsRDD.coalesce(1).saveAsTextFile(errorRecordsPath)
    auditLogRdd.coalesce(1).saveAsTextFile(auditRecordsPath)
    //~Outputs
  }

}





























//
//
////val newRdd = myRdd.map(row => row ++ Array((row(1).toLong * row(199).toLong).toString)
//
//
//
//
////Including Header.....
//
////    val errorLogSchema = logScheme
////    println(errorLogSchema.toString())
////    println("Reached till errorlogschema")
////    val errorSchema = GeniricFunctions.GenerateSchemaFromPropertyFile(errorLogSchema,sc)
////    val errorLogRowRDD = errorLogRDD.map(row => Row(row(0),row(1),row(2),row(3),row(4)))
////    val df = sqlContext.createDataFrame(errorLogRowRDD,errorSchema)
////    df.write.csv("adl://batpetradlanalyticsadls.azuredatalakestore.net/HIVEBI/PetraSpark67")
//
//
////eliminating duplicate records
////val exceptionRecordsRDD = sc.parallelize(exceptionlogRecords).map(x => (x(0),x)).reduceByKey((v1,v2) => v1).values
//
//
//
////    val schemaPropertyPath = "adl://batpetradlanalyticsadls.azuredatalakestore.net/HIVEBI/GenricImpl/UOM_Schema.txt"
////    val rawInputPath = "adl://batpetradlanalyticsadls.azuredatalakestore.net/HIVEBI/GenricImpl/Uom_dimensions.csv
//
////     val schemaPropertyPath = "file:///C:\\Users\\25548\\Desktop\\Jars\\UOM_Schema.txt"
////     val rawInputPath = "file:///C:\\Users\\25548\\Desktop\\Jars\\Uom_dimensions.csv"
////     val sparkConf = new SparkConf().setAppName("MyLocalApp").setMaster("local[*]")