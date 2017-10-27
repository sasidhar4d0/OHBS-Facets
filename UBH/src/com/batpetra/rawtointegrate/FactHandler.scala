package com.batpetra.rawtointegrate

import com.batpetra.rawtointegrate.Constants.ProductConstants
import com.batpetra.utility.GeniricFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object FactHandler {
  val GeniricFunctions = new GeniricFunctions()
  var factDataFrames: ArrayBuffer[org.apache.spark.sql.DataFrame] = new ArrayBuffer[org.apache.spark.sql.DataFrame]()
  def CreateFactDataFrame(factMapDimentionPaths:scala.collection.Map[String,String],factMapDimentionColumns:scala.collection.Map[String,String],sQLContext: SQLContext,sourceDF:org.apache.spark.sql.DataFrame,mainQuery:String):org.apache.spark.sql.DataFrame = {
    factMapDimentionPaths.foreach(x => CreateDataFrame(x._1,x._2,factMapDimentionColumns,sQLContext))
    sQLContext.sql(mainQuery)
  }
  def CreateDataFrame(name: String, path: String, cols:scala.collection.Map[String,String], sqlContext: SQLContext) = {
    println(name + path + cols)
    val columns = cols(name)
    val tempDf1 = sqlContext.read.option("header",true).option("delimiter","|").csv(path)
      val query = "select " + columns + " from " + name
      tempDf1.createOrReplaceTempView(name)
      val tempDf2 = sqlContext.sql(query)
      tempDf2.createOrReplaceTempView(name)

    }
  }
