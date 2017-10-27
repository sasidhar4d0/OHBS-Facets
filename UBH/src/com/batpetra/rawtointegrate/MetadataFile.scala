package com.batpetra.rawtointegrate

import com.batpetra.utility.GeniricFunctions
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by 25548 on 8/21/2017.
  */
object MetadataFile {
  def main(args: Array[String]): Unit = {

    var metaDataList: List[String] = null
    val rawInputPath = "file:///C:\\Users\\25548\\Desktop\\Jars\\rusianmetadata.txt"
    val sparkConf = new SparkConf().setAppName("MyLocalApp").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)


    var productMetadata: ArrayBuffer[String]  = new ArrayBuffer[String]()
    var customerMetadata: ArrayBuffer[String]  = new ArrayBuffer[String]()
    var retailAuditMetadata: ArrayBuffer[String]  = new ArrayBuffer[String]()

    val ProductRecordCount = sc.textFile("adl://batadlspdnepetradev01.azuredatalakestore.net/MetaData/Product/*").count()
    val customerRecordCount = sc.textFile("adl://batadlspdnepetradev01.azuredatalakestore.net/MetaData/Customer/*").count()
    val retailAuditRecordsCount = sc.textFile("adl://batadlspdnepetradev01.azuredatalakestore.net/MetaData/RetailAudit/*").count()

    productMetadata.append("Product", ProductRecordCount.toString(), new GeniricFunctions().GetTodaysDate())
    customerMetadata.append("Customer", customerRecordCount.toString(), new GeniricFunctions().GetTodaysDate())
    retailAuditMetadata.append("RetailAudit", retailAuditRecordsCount.toString(), new GeniricFunctions().GetTodaysDate())

    val auditLog = CreateAuditLog(productMetadata,sc).union(CreateAuditLog(customerMetadata,sc)).union(CreateAuditLog(retailAuditMetadata,sc))
    auditLog.repartition(1).saveAsTextFile("adl://batadlspdnepetradev01.azuredatalakestore.net/MetaData/PocMetadatardd3")


//    import sqlContext.implicits._
//    val fromTuples = sc.parallelize(List(("Product", Product, new GeniricFunctions().GetTodaysDate()), ("Customer", Customer, new GeniricFunctions().GetTodaysDate()), ("RetailAudit", RetailAudit, new GeniricFunctions().GetTodaysDate())))
//    fromTuples.repartition(1).saveAsTextFile("adl://batadlspdnepetradev01.azuredatalakestore.net/MetaData/PocMetadatardd2")
//   // fromTuples.toDF().write.mode(SaveMode.Overwrite).csv("adl://batadlspdnepetradev01.azuredatalakestore.net/MetaData/PocMetadata")

  }


  def CreateAuditLog(auditRecord:ArrayBuffer[String],sc:SparkContext)={
    val record = auditRecord.mkString("|")
    sc.parallelize(List(record))
  }

}
