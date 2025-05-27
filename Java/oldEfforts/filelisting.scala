package com.jio.pulse.plugins

import com.jio.pulse.genericUtility.{operation_debug}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import com.jio.pulse.globalVariables._
import com.google.auth.oauth2.ServiceAccountCredentials
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, LocatedFileStatus, Path, RemoteIterator}

import java.io.ByteArrayInputStream
import com.jio.utilities.ReadInput
import com.azure.storage.blob.BlobServiceClientBuilder
import com.azure.storage.blob.models.{BlobContainerItem, BlobItem, BlobStorageException}
import com.jio.encrypt.DES3.decryptKey
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object FileListing {
  def file_listing_workflow(workflow_id: Long, inputDF: DataFrame): DataFrame ={
    var df=spark.emptyDataFrame
    val rows = file_listing_array.filter(x => x.getAs("WORKFLOW_ID").toString.toLong == workflow_id)
    val file_listing_id = rows(0).getAs("FILELISTING_ID").toString.toLong
    val connection_string_id = rows(0).getAs("CONNECTION_STRING_ID").toString.toLong
    val input_df_column = rows(0).getAs[String]("INPUT_DF_COLUMN")
    val storage_account = ReadInput.readInput(spark, "(" + connection_with + connection_string_id + ") x", url, username, password, driver)
    val storage_account_type = storage_account.head().getString(0)
    var blobs = scala.collection.mutable.MutableList[String]()
    println("before is column n")
    file_listing_add_attr_array.foreach(r=>println(r))
    var file_listing_details:scala.collection.mutable.Map[String,Any]=scala.collection.mutable.Map()
    file_listing_add_attr_array.filter(x=>x.getAs("FILELISTING_ID").toString.toLong==file_listing_id && x.getAs("IS_COLUMN").toString.equalsIgnoreCase("N")).foreach(row=>file_listing_details=file_listing_details++Map(row.getAs[String]("KEY")->row.getAs[String]("VALUE")))
    var attribute_map_N:scala.collection.mutable.Map[String,Any]=file_listing_details
    file_listing_details.clear()
    println("before is column y")
    file_listing_add_attr_array.filter(x=>x.getAs("FILELISTING_ID").toString.toLong==file_listing_id && x.getAs("IS_COLUMN").toString.equalsIgnoreCase("Y")).foreach(row=>file_listing_details=file_listing_details++Map(row.getAs[String]("KEY")->row.getAs[String]("VALUE")))
    var attribute_map_Y:scala.collection.mutable.Map[String,Any]=file_listing_details
    if(attribute_map_Y.isEmpty){
      df=list_files(workflow_id, inputDF,attribute_map_N)
    }else{
      val column_list = attribute_map_Y.map(x=>x._2.toString).toList
      var base_filer_cond = ""
      column_list.foreach { col =>
        base_filer_cond += col + " is not null and "
      }
      base_filer_cond=base_filer_cond.substring(0,base_filer_cond.lastIndexOf(" and "))
      val temp_df=inputDF.filter(base_filer_cond)
      temp_df.cache()
      val distinct_attribute_rows=temp_df.select(column_list.map(c=>col(c)):_*).distinct.collect()
      var dfArray:Array[DataFrame]=Array()
      distinct_attribute_rows.foreach{row=>
        attribute_map_Y.foreach{x=>
          attribute_map_N = attribute_map_N ++ Map(x._1 -> row.getAs[String](x._2.toString))
        }
        dfArray=dfArray:+list_files(workflow_id,inputDF,attribute_map_N)
      }
      df=com.jio.pulse.genericUtility.dfUnion(dfArray)
      temp_df.unpersist()
    }
    if (configuration.getBoolean("debug.filelisting.operation")) {
      if (configuration.getBoolean("debug.filelisting.show.input")) operation_debug("File Listing", workflow_id, df, inputDF)
      else operation_debug("File Listing", workflow_id, df)
    }
    df
  }
  def list_files(workflow_id: Long, inputDF: DataFrame,add_attr_map:scala.collection.mutable.Map[String,Any]): DataFrame = {
    println(add_attr_map)
    val rows = file_listing_array.filter(x => x.getAs("WORKFLOW_ID").toString.toLong == workflow_id)
    val file_listing_id=rows(0).getAs("FILELISTING_ID").toString.toLong
    val connection_string_id=rows(0).getAs("CONNECTION_STRING_ID").toString.toLong
    val input_df_column=rows(0).getAs[String]("INPUT_DF_COLUMN")
    val storage_account=ReadInput.readInput(spark, "("+connection_with+connection_string_id+") x", url, username, password, driver)
    val storage_account_type=storage_account.head().getString(0)
    var blobs = scala.collection.mutable.MutableList[String]()
    //val add_attr_details = file_listing_add_attr_array.filter(x => x.getAs("FILELISTING_ID").toString.toLong == file_listing_id)
    val connection_dtls = connection_array.filter { row => row.getAs("connection_string_id").toString.toLong == connection_string_id } // && (row.getAs[String]("key") == "account_key" || row.getAs[String]("key") == "account_name")}
    if(storage_account_type.equalsIgnoreCase("Azure_Storage")){
      println("--------------Entered for Azure FileListing-------------")
      val account_key = decryptKey(connection_dtls.find(_.getAs[String]("key") == "account_key").map(_.getAs[String]("value")).getOrElse(""))
      val account_name = connection_dtls.find(_.getAs[String]("key") == "account_name").map(_.getAs[String]("value")).getOrElse("")
//      val container_name = add_attr_details.find(_.getAs[String]("KEY") == "container_name").map(_.getAs[String]("VALUE")).getOrElse("")
      val container_name = add_attr_map("container_name").toString
//      var file_path = add_attr_details.find(_.getAs[String]("KEY") == "file_path").map(_.getAs[String]("VALUE")).getOrElse("")
      var file_path = add_attr_map("file_path").toString
      val conf = new Configuration()
      conf.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      conf.set(s"fs.azure.account.key.$account_name.blob.core.windows.net", account_key)
      if(file_path.equalsIgnoreCase(""))file_path="/"
      val basePath = s"wasbs://$container_name@$account_name.blob.core.windows.net" + file_path
      var fileStatuses: Array[FileStatus] = Array()
      try {
        val fileSystem = FileSystem.get(new Path(basePath).toUri, conf)
        val fileStatuses: Array[FileStatus] = fileSystem.globStatus(new Path(basePath))
        fileStatuses.foreach { fileStatus =>
          blobs += fileStatus.getPath.toString
        }
      } catch {
        case e: Exception =>
          println(s"Error message: ${e.getStackTrace}")
      }
    } else if(storage_account_type.equalsIgnoreCase("GCP_Storage_Account")){
      println("--------------Entered for GCP FileListing-------------")
      val projectID = connection_dtls.find(_.getAs[String]("key") == "projectID").map(_.getAs[String]("value")).getOrElse("")
      val service_account_email = connection_dtls.find(_.getAs[String]("key") == "service_account_email").map(_.getAs[String]("value")).getOrElse("")
      val service_account_private_key_id = connection_dtls.find(_.getAs[String]("key") == "service_account_private_key_id").map(_.getAs[String]("value")).getOrElse("")
      val service_account_private_key = connection_dtls.find(_.getAs[String]("key") == "service_account_private_key").map(_.getAs[String]("value")).getOrElse("")
      var bucketName = com.jio.pulse.plugins.Loop.loop_variables_replace(add_attr_map("bucketName").toString)
      bucketName = bucketName.trim.replaceAll("^'|'$", "")
      println("bucketName "+bucketName)
      //val basePath = add_attr_details.find(_.getAs[String]("KEY") == "basePath").map(_.getAs[String]("VALUE")).getOrElse("/*")
      val basePath = if(add_attr_map.contains("basePath")) add_attr_map("basePath").toString else "/*"
      val path = s"gs://$bucketName" + basePath
      val conf = new Configuration()
      conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
      conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      conf.set("google.cloud.auth.service.account.enable", "true")
      conf.set("fs.gs.auth.service.account.email", service_account_email)
      conf.set("fs.gs.auth.service.account.private.key.id", service_account_private_key_id)
      conf.set("fs.gs.auth.service.account.private.key", service_account_private_key)
      conf.set("fs.gs.project.id", projectID)

      try{
        val fileSystem = FileSystem.get(new Path(path).toUri, conf)
        val fileStatuses: Array[FileStatus] = fileSystem.globStatus(new Path(path))
        fileStatuses.foreach { fileStatus =>
          blobs += fileStatus.getPath.toString
        }
      }
      catch {
        case e: Exception =>
          println(s"Error message: ${e.getStackTrace}")
      }
    }
    else {
      val conf = new Configuration()
      val fs = FileSystem.get(conf)
//      val file_path = add_attr_details.find(_.getAs[String]("KEY") == "path").map(_.getAs[String]("VALUE")).getOrElse("")
      val file_path = if(add_attr_map.contains("paath"))add_attr_map("path").toString else "/"
      val directoryPath = new Path(file_path)
      try {
        val fileList: RemoteIterator[LocatedFileStatus] = fs.listFiles(directoryPath, true)
        while (fileList.hasNext) {
          val fileStatus = fileList.next()
          blobs += fileStatus.getPath.toString
        }
      }catch {
        case e:Exception=>
          println("==========>Error while reading from File System")
          println(s"==========>Error message: ${e.getMessage}")
          println(s"==========>Stack Trace : ${e.getStackTrace}")
      }
      fs.close()
    }

    val schema = StructType(Array(
      StructField(input_df_column, StringType, nullable = true)
    ))

    val rows_1 = blobs.map(x => Row(x))
    val df1: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(rows_1), schema)
    val result = inputDF.withColumn("rn", monotonically_increasing_id())
      .join(df1.withColumn("rn", monotonically_increasing_id()), Seq("rn"), "right_outer")
      .drop("rn")

    result
  }
}
