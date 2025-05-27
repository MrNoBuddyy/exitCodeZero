package com.jio.pulse

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.jio.utilities.ReadInput
import com.jio.encrypt.DES3.decryptKey
import scala.util.Try
import com.jio.utilities.StoreOutput
import com.jio.pulse.globalVariables._
import com.jio.utilities.BatchProcessKafka.getData
import com.jio.utilities.StoreOutput.writeToES
import java.sql.DriverManager
import scala.collection.mutable.ListBuffer

object ReadWriteUtility {
  def castMapValuesToString(map: scala.collection.mutable.Map[String, Any]): scala.collection.mutable.Map[String, String] = {
    map.map(x => (x._1, x._2.toString))
  }

  def read(source: String, map: scala.collection.mutable.Map[String, Any], debug: Boolean = false): (DataFrame, Boolean) = {
    var df = spark.emptyDataFrame
    var isSuccess: Boolean = false
    if (source.equalsIgnoreCase("oracle")) {
      var attribute_map = castMapValuesToString(map)
      var username = attribute_map("userid")
      var password = decryptKey(attribute_map("password"))
      var driver = if (attribute_map.contains("driver")) attribute_map("driver") else com.jio.pulse.globalVariables.driver
      var ipaddress = attribute_map("ipaddress")
      var port = attribute_map("port")
      var database = attribute_map("database")
      var url = if (attribute_map.contains("url")) attribute_map("url") else s"jdbc:oracle:thin:@$ipaddress:$port/$database"
      var tablename = attribute_map("table_name_or_query")
      tablename=com.jio.pulse.plugins.Loop.loop_variables_replace(tablename)
      try {
        df = ReadInput.readInput(spark, "(" + tablename + ") x", url, username, password, driver)
        isSuccess = true
      } catch {
        case ex: Exception => {
          isSuccess = false
          println("====> Error while Reading the dataframe")
          println(ex.getMessage)
          println("====> Stack trace")
          println(ex.getStackTrace.mkString("\n"))
          StoreOutput.exceptionWrite(spark, healthmonitor_kafka_topic, healthmonitor_kafka_bootstrapserver, healthmonitor_kafka_ssl_map, ex)
        }
      }
    } else if (source.equalsIgnoreCase("mysql")) {
      var attribute_map = castMapValuesToString(map)
      var username = attribute_map("userid")
      var password = decryptKey(attribute_map("password"))
      var driver = if (attribute_map.contains("driver")) attribute_map("driver") else com.jio.pulse.globalVariables.mysqldriver
      var ipaddress = attribute_map("ipaddress")
      var port = attribute_map("port")
      var database = attribute_map("database")
      var url = if (attribute_map.contains("url")) attribute_map("url") else s"jdbc:mysql://$ipaddress:$port/$database"
      var tablename = attribute_map("table_name_or_query")
      try {
        df = ReadInput.readInput(spark, "(" + tablename + ") x", url, username, password, driver)
        isSuccess = true
      } catch {
        case ex: Exception => {
          isSuccess = false
          println("====> Error while Reading the dataframe")
          println(ex.getMessage)
          println("====> Stack trace")
          println(ex.getStackTrace.mkString("\n"))
          StoreOutput.exceptionWrite(spark, healthmonitor_kafka_topic, healthmonitor_kafka_bootstrapserver, healthmonitor_kafka_ssl_map, ex)
        }
      }
    } else if (source.equalsIgnoreCase("postgresql")) {
      var attribute_map = castMapValuesToString(map)
      var username = attribute_map("userid")
      var password = decryptKey(attribute_map("password"))
      var driver = if (attribute_map.contains("driver")) attribute_map("driver") else com.jio.pulse.globalVariables.postgresqldriver
      var ipaddress = attribute_map("ipaddress")
      var port = attribute_map("port")
      var database = attribute_map("database")
      var url = if (attribute_map.contains("url")) attribute_map("url") else s"jdbc:postgresql://$ipaddress:$port/$database"
      var tablename = attribute_map("table_name_or_query")
      try {
        df = ReadInput.readInput(spark, "(" + tablename + ") x", url, username, password, driver)
        isSuccess = true
      } catch {
        case ex: Exception => {
          isSuccess = false
          println("====> Error while Reading the dataframe")
          println(ex.getMessage)
          println("====> Stack trace")
          println(ex.getStackTrace.mkString("\n"))
          StoreOutput.exceptionWrite(spark, healthmonitor_kafka_topic, healthmonitor_kafka_bootstrapserver, healthmonitor_kafka_ssl_map, ex)
        }
      }
    } else if (source.equalsIgnoreCase("sqlserver")) {
      var attribute_map = castMapValuesToString(map)
      var username = attribute_map("userid")
      var password = decryptKey(attribute_map("password"))
      var driver = if (attribute_map.contains("driver")) attribute_map("driver") else "com.microsoft.sqlserver.jdbc.SQLServerDriver"
      var ipaddress = attribute_map("ipaddress")
      var port = attribute_map("port")
      var database = attribute_map("database")
      var url = if (attribute_map.contains("url")) attribute_map("url") else s"jdbc:sqlserver://$ipaddress:$port;database=$database;encrypt=true;trustServerCertificate=true;"
      var tablename = attribute_map("table_name_or_query")
      try {
        df = ReadInput.readInput(spark, "(" + tablename + ") x", url, username, password, driver)
        isSuccess = true
      } catch {
        case ex: Exception => {
          isSuccess = false
          println("====> Error while Reading the dataframe")
          println(ex.getMessage)
          println("====> Stack trace")
          println(ex.getStackTrace.mkString("\n"))
          StoreOutput.exceptionWrite(spark, healthmonitor_kafka_topic, healthmonitor_kafka_bootstrapserver, healthmonitor_kafka_ssl_map, ex)
        }
      }
    } else if (source.equalsIgnoreCase("hdfs")) {
      var attribute_map = castMapValuesToString(map)
      var url = attribute_map("url")
      var paths = attribute_map("path").split(",")
      var format = attribute_map("format")
      val atts = "multiline,header,sep,mode,ignoreLeadingWhiteSpace,ignoreTrailingWhiteSpace,wholeFile,quote,escape".split(",")
      var optionsMap: Map[String, String] = Map()
      atts.foreach { att =>
        if (attribute_map.contains(att)) {
          optionsMap ++= Map(att -> attribute_map(att))
        }
      }
      try {
        df = spark.read.format(format).options(optionsMap).load(paths.map(url + _): _*)
        isSuccess = true
      } catch {
        case ex: Exception => {
          isSuccess = false
          println("====> Error while Reading the dataframe")
          println(ex.getMessage)
          println("====> Stack trace")
          println(ex.getStackTrace.mkString("\n"))
          StoreOutput.exceptionWrite(spark, healthmonitor_kafka_topic, healthmonitor_kafka_bootstrapserver, healthmonitor_kafka_ssl_map, ex)
        }
      }
    } else if (source.equalsIgnoreCase("hive")) {
      var attribute_map = castMapValuesToString(map)
      var query = attribute_map("query")
      try {
        df = com.jio.pulse.genericUtility.hiveContext.sql(query)
        isSuccess = true
      } catch {
        case ex: Exception => {
          isSuccess = false
          println("====> Error while Reading the dataframe")
          println(ex.getMessage)
          println("====> Stack trace")
          println(ex.getStackTrace.mkString("\n"))
          StoreOutput.exceptionWrite(spark, healthmonitor_kafka_topic, healthmonitor_kafka_bootstrapserver, healthmonitor_kafka_ssl_map, ex)
        }
      }
    } else if (source.equalsIgnoreCase("elasticsearch")) {
      var attribute_map = castMapValuesToString(map)
      var optionsMap: scala.collection.mutable.Map[String, String] = attribute_map.filter(x => x._1.contains("es.")).map(x => if (x._1.contains(".pass")) (x._1 -> decryptKey(x._2)) else x)
      var node = attribute_map("node")
      var port = attribute_map("port")
      var username = attribute_map("username")
      var password = decryptKey(attribute_map("password"))
      var index = attribute_map("index")
      var indexTimestamp = if (attribute_map.contains("index_timestamp")) attribute_map("index_timestamp") else ""
      var current_date = ReadInput.getDate("yyyy.MM.dd")
      if (indexTimestamp.equalsIgnoreCase("current_timestamp")) index = index + "-" + current_date
      try {
        df = spark.read.format("org.elasticsearch.spark.sql")
          .option("es.nodes", node)
          .option("es.port", port)
          .option("es.net.http.auth.user", username)
          .option("es.net.http.auth.pass", password)
          .options(optionsMap)
          .option("pushdown", "true")
          .load(index)
        isSuccess = true
      } catch {
        case ex: Exception => {
          isSuccess = false
          println("====> Error while Reading the dataframe")
          println(ex.getMessage)
          println("====> Stack trace")
          println(ex.getStackTrace.mkString("\n"))
          StoreOutput.exceptionWrite(spark, healthmonitor_kafka_topic, healthmonitor_kafka_bootstrapserver, healthmonitor_kafka_ssl_map, ex)
        }
      }
    } else if (source.equalsIgnoreCase("opensearch")) {
      var attribute_map = castMapValuesToString(map)
      var optionsMap: scala.collection.mutable.Map[String, String] = attribute_map.filter(x => x._1.contains("opensearch.")).map(x => if (x._1.contains(".pass")) (x._1 -> decryptKey(x._2)) else x)
      var node = attribute_map("node")
      var port = attribute_map("port")
      var username = attribute_map("username")
      var password = decryptKey(attribute_map("password"))
      var index = attribute_map("index")
      var indexTimestamp = if (attribute_map.contains("index_timestamp")) attribute_map("index_timestamp") else ""
      var current_date = ReadInput.getDate("yyyy.MM.dd")
      if (indexTimestamp.equalsIgnoreCase("current_timestamp")) index = index + "-" + current_date
      try {
        df = spark.read.format("org.opensearch.spark.sql")
          .option("opensearch.nodes", node)
          .option("opensearch.port", port)
          .option("opensearch.net.http.auth.user", username)
          .option("opensearch.net.http.auth.pass", password)
          .options(optionsMap)
          .option("pushdown", "true")
          .load(index)
        isSuccess = true
      } catch {
        case ex: Exception => {
          isSuccess = false
          println("====> Error while Reading the dataframe")
          println(ex.getMessage)
          println("====> Stack trace")
          println(ex.getStackTrace.mkString("\n"))
          StoreOutput.exceptionWrite(spark, healthmonitor_kafka_topic, healthmonitor_kafka_bootstrapserver, healthmonitor_kafka_ssl_map, ex)
        }
      }
    } else if (source.equalsIgnoreCase("kafka")) {
      var attribute_map = castMapValuesToString(map)
      var bootstrap_servers = attribute_map("bootstrap_servers")
      var topics = attribute_map("topics")
      var group_id = attribute_map("group_id")
      var data_format = attribute_map("data_format")
      if (cl_args.length >= 8) {
        val offset_details = cl_args(2)
        val es_index = cl_args(3)
        val es_nodes = cl_args(4)
        val es_port = cl_args(5)
        val es_user = cl_args(6)
        val es_password = decryptKey(cl_args(7))

        try {
          val result = getData(spark, topics, bootstrap_servers, offset_details, group_id)
          df = result._1
          df = df.withColumnRenamed("value", "__data__")
          if (data_format.equalsIgnoreCase("json")) {
            var schema = spark.read.json(df.select("__data__").rdd.map(x => x(0).toString)).schema
            df = df.withColumn("__data__", from_json(col("__data__"), schema))
            df = df.select(col("__data__.*"), col("*")).drop("__data__")
          }

          val updated_offset = result._2

          val es_record = spark.read.format("org.elasticsearch.spark.sql")
            .option("es.nodes", es_nodes)
            .option("es.port", es_port)
            .option("es.net.http.auth.user", es_user)
            .option("es.net.http.auth.pass", es_password)
            .load(es_index + "/_doc")
            .filter(col("JobName") === lit(spark_app_name))
            .withColumn("Details", lit(updated_offset))

          writeToES(es_record, es_index, es_nodes, es_port, es_user, es_password, doctype = "_doc", operation = "upsert", mapping_id = "JobName")
          isSuccess = true
        } catch {
          case ex: Exception => {
            isSuccess = false
            println("====> Error while Reading the dataframe")
            println(ex.getMessage)
            println("====> Stack trace")
            println(ex.getStackTrace.mkString("\n"))
            StoreOutput.exceptionWrite(spark, healthmonitor_kafka_topic, healthmonitor_kafka_bootstrapserver, healthmonitor_kafka_ssl_map, ex)
          }
        }

      } else {
        println("Insufficient Number of Arguments - Number of arguments given = " + cl_args.length)
      }
    } else if (source.equalsIgnoreCase("api")) {
      var header: Map[String, String] = Map()
      if (map.contains("header")) header = map("header").asInstanceOf[Map[String, String]]
      var attribute_map = castMapValuesToString(map.-("header"))
      var method = attribute_map("method")
      var payload = attribute_map("payload")
      var base_url = attribute_map("url")
      var username = attribute_map("username")
      var password = decryptKey(attribute_map("password"))
      var api_id = attribute_map("api_id").toInt
      try {
        df = com.jio.pulse.genericUtility.genericJsonParsing(-1, com.jio.pulse.genericUtility.FetchApiData(api_id, spark.emptyDataFrame), api_id)
        /*if (username.equals("") && password.equals("")) {
          df = com.jio.pulse.genericUtility.genericJsonParsing(-1, ReadInput.getDatabyURL(spark, method, base_url, payload, header), api_id)
        } else {
          df = com.jio.pulse.genericUtility.genericJsonParsing(-1, ReadInput.getDatabyURL(spark, method, base_url, payload, header, username, password), api_id)
        }*/
        isSuccess = true
      } catch {
        case ex: Exception => {
          isSuccess = false
          println("====> Error while Reading the dataframe")
          println(ex.getMessage)
          println("====> Stack trace")
          println(ex.getStackTrace.mkString("\n"))
          StoreOutput.exceptionWrite(spark, healthmonitor_kafka_topic, healthmonitor_kafka_bootstrapserver, healthmonitor_kafka_ssl_map, ex)
        }
      }
    } else if (source.equalsIgnoreCase("gcp_bigquery")) {
      var attribute_map = castMapValuesToString(map)
      var tablename_or_query = attribute_map("tablename_or_query")
      tablename_or_query=com.jio.pulse.plugins.Loop.loop_variables_replace(tablename_or_query)
      val atts = "parentProject,proxyAddress,credentialsFile,credentials,materializationDataset,viewsEnabled".split(",")
      var optionsMap: Map[String, String] = Map()
      atts.foreach { att =>
        if (attribute_map.contains(att)) {
          optionsMap ++= Map(att -> attribute_map(att))
        }
      }
      try {
        df = spark.read.format("bigquery").options(optionsMap).load(tablename_or_query)
        isSuccess = true
      } catch {
        case ex: Exception => {
          isSuccess = false
          println("====> Error while Reading the dataframe")
          println(ex.getMessage)
          println("====> Stack trace")
          println(ex.getStackTrace.mkString("\n"))
          StoreOutput.exceptionWrite(spark, healthmonitor_kafka_topic, healthmonitor_kafka_bootstrapserver, healthmonitor_kafka_ssl_map, ex)
        }
      }
    } else if (source.equalsIgnoreCase("gcp_storagebucket")) {
      val attribute_map = castMapValuesToString(map)
      var path = com.jio.pulse.plugins.Loop.loop_variables_replace(attribute_map("path"))
      path = path.trim.replaceAll("^'|'$", "")
      val format = attribute_map("format")
      val service_account_email = attribute_map("service.account.email")
      val service_account_private_key_id = attribute_map("service.account.private.key.id")
      val service_account_private_key = attribute_map("service.account.private.key")
      val project_id = attribute_map("project.id")
	    val proxy = attribute_map.get("proxy.address").getOrElse("")
      val atts = "multiline,header,sep,mode,ignoreLeadingWhiteSpace,ignoreTrailingWhiteSpace,wholeFile,quote,escape".split(",")
      var optionsMap: Map[String, String] = Map()
      atts.foreach { att =>
        if (attribute_map.contains(att)) {
          optionsMap ++= Map(att -> attribute_map(att))
        }
      }
      spark.sparkContext.hadoopConfiguration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
      spark.sparkContext.hadoopConfiguration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      spark.sparkContext.hadoopConfiguration.set("google.cloud.auth.service.account.enable", "true")
      spark.sparkContext.hadoopConfiguration.set("fs.gs.auth.service.account.email", service_account_email)
      spark.sparkContext.hadoopConfiguration.set("fs.gs.auth.service.account.private.key.id", service_account_private_key_id)
      spark.sparkContext.hadoopConfiguration.set("fs.gs.auth.service.account.private.key", service_account_private_key)
      spark.sparkContext.hadoopConfiguration.set("fs.gs.project.id", project_id)
	    if (!proxy.equals("")) {
		    spark.sparkContext.hadoopConfiguration.set("fs.gs.proxy.address", proxy)
	    }
      try {
        df = spark.read.format(format).options(optionsMap).load(path)
        isSuccess = true
      } catch {
        case ex: Exception => {
          isSuccess = false
          println("====> Error while Reading the dataframe")
          println(ex.getMessage)
          println("====> Stack trace")
          println(ex.getStackTrace.mkString("\n"))
          StoreOutput.exceptionWrite(spark, healthmonitor_kafka_topic, healthmonitor_kafka_bootstrapserver, healthmonitor_kafka_ssl_map, ex)
        }
      }
    } else if (source.equalsIgnoreCase("aws_s3")) {
      val attribute_map = castMapValuesToString(map)
      val path = attribute_map("path").split(",")
      val format = attribute_map("format")
      val bucket = attribute_map("bucket")
      val access_key_id = attribute_map("aws_access_key_id")
      val aws_secret_access_key = decryptKey(attribute_map("aws_secret_access_key"))
      val endpoint = attribute_map("fs.s3a.endpoint")
      val proxyhost = attribute_map.get("proxy.address").getOrElse("")
      val proxyport = attribute_map.get("proxy.port").getOrElse("")
      val atts = "multiline,header,sep,mode,ignoreLeadingWhiteSpace,ignoreTrailingWhiteSpace,wholeFile,quote,escape".split(",")
      var optionsMap: Map[String, String] = Map()
      atts.foreach { att =>
        if (attribute_map.contains(att)) {
          optionsMap ++= Map(att -> attribute_map(att))
        }
      }
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", access_key_id)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", aws_secret_access_key)
      if (!endpoint.equals("")) {
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", endpoint)
      }
      if (!proxyhost.equals("") && !proxyport.equals("")) {
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.proxy.host", proxyhost)
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.proxy.port", proxyport)
      }
      try {
        val file_path = path.map(path => s"s3a://$bucket/" + path)
        df = spark.read.format(format).options(optionsMap).load(file_path: _*)
        isSuccess = true
      } catch {
        case ex: Exception => {
          isSuccess = false
          println("====> Error while Reading the dataframe for aws")
          println(ex.getMessage)
          println("====> Stack trace")
          println(ex.getStackTrace.mkString("\n"))
          StoreOutput.exceptionWrite(spark, healthmonitor_kafka_topic, healthmonitor_kafka_bootstrapserver, healthmonitor_kafka_ssl_map, ex)
        }
      }
    } else if (source.equalsIgnoreCase("Azure_Blob_Storage")) {
      val attribute_map = castMapValuesToString(map)
      val path = attribute_map("path").split(",")
      val format = attribute_map("format")
      val storage_account_name = attribute_map("storage_account_name")
      val storage_account_key = decryptKey(attribute_map("storage_account_key"))
      val container_name=attribute_map("container_name")
      val proxyhost = attribute_map.get("proxy.address").getOrElse("")
      val proxyport = attribute_map.get("proxy.port").getOrElse("")
      val atts = "multiline,header,sep,mode,ignoreLeadingWhiteSpace,ignoreTrailingWhiteSpace,wholeFile,quote,escape".split(",")
      var optionsMap: Map[String, String] = Map()
      atts.foreach { att =>
        if (attribute_map.contains(att)) {
          optionsMap ++= Map(att -> attribute_map(att))
        }
      }
      spark.sparkContext.hadoopConfiguration.set(s"fs.azure.account.key.$storage_account_name.blob.core.windows.net", storage_account_key)
      if (!proxyhost.equals("") && !proxyport.equals("")) {
        spark.sparkContext.hadoopConfiguration.set("fs.azure.http.proxy.host", proxyhost)
        spark.sparkContext.hadoopConfiguration.set("fs.azure.http.proxy.port", proxyport)
      }
      val file_path = path.map(path => s"wasbs://$container_name@$storage_account_name.blob.core.windows.net/" + path)
      try {
        df = spark.read.format(format).options(optionsMap).load(file_path: _*)
        isSuccess = true
      } catch {
        case ex: Exception => {
          isSuccess = false
          println("====> Error while Reading the dataframe for Azure Blob Storage")
          println(ex.getMessage)
          println("====> Stack trace")
          println(ex.getStackTrace.mkString("\n"))
          StoreOutput.exceptionWrite(spark, healthmonitor_kafka_topic, healthmonitor_kafka_bootstrapserver, healthmonitor_kafka_ssl_map, ex)
        }
      }
    } else if (source.equalsIgnoreCase("mongodb")) {
        val attribute_map = castMapValuesToString(map)
        var ipaddress = attribute_map("ip")
        var port = attribute_map("port")
        var username = attribute_map("username")
        var password = decryptKey(attribute_map("password"))
        var database = attribute_map("database")
        var uri = s"mongodb://$ipaddress:$port"
        var collection = attribute_map("collection")
        try {
          df = spark.read.format("mongo")
            .option("uri", uri)
            .option("database", database)
            .option("collection", collection)
            .option("username", username)
            .option("password", password)
            .load()
          isSuccess = true
        } catch {
          case ex: Exception => {
            isSuccess = false
            println("====> Error while Reading the dataframe")
            println(ex.getMessage)
            println("====> Stack trace")
            println(ex.getStackTrace.mkString("\n"))
            StoreOutput.exceptionWrite(spark, healthmonitor_kafka_topic, healthmonitor_kafka_bootstrapserver, healthmonitor_kafka_ssl_map, ex)
          }
        }
      } else if (source.equalsIgnoreCase("CouchDB")) {
          val attribute_map = castMapValuesToString(map)
          var host = attribute_map("host")
          var username = attribute_map("username")
          var password = decryptKey(attribute_map("password"))
          var database = attribute_map("database")
          var protocol = attribute_map("protocol")
          var view = attribute_map("view")

        try {
          if (view.isEmpty) {
            df = spark.read
              .format("org.apache.bahir.cloudant")
              .option("cloudant.host", host)
              .option("cloudant.username", username)
              .option("cloudant.password", password)
              .option("database", database)
              .option("cloudant.protocol", protocol)
              .load()
          }
          else {
            df = spark.read
              .format("org.apache.bahir.cloudant")
              .option("cloudant.host", host)
              .option("cloudant.username", username)
              .option("cloudant.password", password)
              .option("database", database)
              .option("cloudant.protocol", protocol)
              .option("view", view)
              .load()
          }
          isSuccess = true
  //        df.show()
        } catch {
          case ex: Exception => {
            isSuccess = false
            println("====> Error while Reading the dataframe")
            println(ex.getMessage)
            println("====> Stack trace")
            println(ex.getStackTrace.mkString("\n"))
            StoreOutput.exceptionWrite(spark, healthmonitor_kafka_topic, healthmonitor_kafka_bootstrapserver, healthmonitor_kafka_ssl_map, ex)
          }
        }
    }else if (source.equalsIgnoreCase("Redis")) {
      val attribute_map = castMapValuesToString(map)
      var host = attribute_map("host")
      var port = attribute_map("port")
      var password = decryptKey(attribute_map("password"))
      var table = attribute_map("table")
      try {
        df = spark.read
          .format("org.apache.spark.sql.redis")
          .option("host", host)
          .option("port", port)
          .option("password", password)
          .option("inferSchema", "true")
          .option("table", table)
          .load()
        isSuccess = true
//        df.show()
      } catch {
        case ex: Exception => {
          isSuccess = false
          println("====> Error while Reading the dataframe")
          println(ex.getMessage)
          println("====> Stack trace")
          println(ex.getStackTrace.mkString("\n"))
          StoreOutput.exceptionWrite(spark, healthmonitor_kafka_topic, healthmonitor_kafka_bootstrapserver, healthmonitor_kafka_ssl_map, ex)
        }
      }
    }else if (source.equalsIgnoreCase("neo4j")) {
              val attribute_map = castMapValuesToString(map)
              val ipaddress = attribute_map("ipaddress")
              val port = attribute_map("port")
              val username = attribute_map("username")
              val password = decryptKey(attribute_map("password"))
              val url = if (attribute_map.contains("url")) attribute_map("url") else s"bolt://${username}:$password@$ipaddress:$port/browser"
              val query = attribute_map("query")
              try {
              df = spark.read
              .format("org.neo4j.spark.DataSource")
              .option("url", url)
              .option("authentication.basic.username", username)
              .option("authentication.basic.password", password)
              .option("query", query)
              .load()
              isSuccess = true
                } catch {
                  case ex: Exception => {
                    isSuccess = false
                    println("====> Error while Reading the dataframe")
                    println(ex.getMessage)
                    println("====> Stack trace")
                    println(ex.getStackTrace.mkString("\n"))
                    StoreOutput.exceptionWrite(spark, healthmonitor_kafka_topic, healthmonitor_kafka_bootstrapserver, healthmonitor_kafka_ssl_map, ex)
                }
                }
    } else if (source.equalsIgnoreCase("cassandra")) {
      val attribute_map = castMapValuesToString(map)
      var ipaddress = attribute_map("host")
      var port = attribute_map("port")
      var username = attribute_map("username")
      var password = decryptKey(attribute_map("password"))
      var keyspace = attribute_map("keyspace")
      var table = attribute_map("table")
      try {
        df = spark.read.format("org.apache.spark.sql.cassandra")
          .option("spark.cassandra.connection.host", ipaddress)
          .option("spark.cassandra.connection.port", port)
          .options(Map("keyspace" -> keyspace, "table" -> table))
          .option("spark.cassandra.auth.username", username)
          .option("spark.cassandra.auth.password", password)
          .load()

        //        df.show()
        isSuccess = true
      } catch {
        case ex: Exception => {
          isSuccess = false
          println("====> Error while Reading the dataframe")
          println(ex.getMessage)
          println("====> Stack trace")
          println(ex.getStackTrace.mkString("\n"))
          StoreOutput.exceptionWrite(spark, healthmonitor_kafka_topic, healthmonitor_kafka_bootstrapserver, healthmonitor_kafka_ssl_map, ex)
        }
      }
    } else if (source.equalsIgnoreCase("CockroachDB")) {
      val attribute_map = castMapValuesToString(map)
      val url = attribute_map("url")
      val query = attribute_map("query")
      try {
        df = spark.read
          .format("jdbc")
          .option("url", url)
          .option("query", query)
          .load()
        isSuccess = true
      } catch {
        case ex: Exception => {
          isSuccess = false
          println("====> Error while Reading the dataframe")
          println(ex.getMessage)
          println("====> Stack trace")
          println(ex.getStackTrace.mkString("\n"))
          StoreOutput.exceptionWrite(spark, healthmonitor_kafka_topic, healthmonitor_kafka_bootstrapserver, healthmonitor_kafka_ssl_map, ex)
        }
      }
    } else if (source.equalsIgnoreCase("SQLite")) {
        val attribute_map = castMapValuesToString(map)
        var path = attribute_map("path")
        var jdbcUrl = "jdbc:sqlite:" + path
        var table = attribute_map("table")
        try {
          df = spark.read
            .format("jdbc")
            .option("url", jdbcUrl)
            .option("dbtable", table)
            .load()

          isSuccess = true
      } catch {
        case ex: Exception => {
          isSuccess = false
          println("====> Error while Reading the dataframe")
          println(ex.getMessage)
          println("====> Stack trace")
          println(ex.getStackTrace.mkString("\n"))
          StoreOutput.exceptionWrite(spark, healthmonitor_kafka_topic, healthmonitor_kafka_bootstrapserver, healthmonitor_kafka_ssl_map, ex)
        }
      }
    }
    else if (source.equalsIgnoreCase("arangoDB")) {
      val attribute_map = castMapValuesToString(map)
     
      var database = attribute_map("database")
      var table = attribute_map("table")
      var user = attribute_map("user")
      var password = decryptKey(attribute_map("password"))
      var endpoints = attribute_map("endpoints")

      try {
        df = spark.read.format("com.arangodb.spark")
          .options(Map(
            "database" -> database,
            "table" -> table,
            "user" -> user,
            "password" -> password,
            "endpoints" -> endpoints))
          .load()
        

        isSuccess = true
      } catch {
        case ex: Exception => {
          isSuccess = false
          println("====> Error while Reading the dataframe")
          println(ex.getMessage)
          println("====> Stack trace")
          println(ex.getStackTrace.mkString("\n"))
          StoreOutput.exceptionWrite(spark, healthmonitor_kafka_topic, healthmonitor_kafka_bootstrapserver, healthmonitor_kafka_ssl_map, ex)
        }
      }
    }
    else if (source.equalsIgnoreCase("SCYLLADB")) {

      val attribute_map = castMapValuesToString(map)
      var host = attribute_map("host")
      var port = attribute_map("port")
      var username = attribute_map("username")
      var password = decryptKey(attribute_map("password"))
      var keyspace = attribute_map("keyspace")
      var table = attribute_map("table")
      // var uri = s"mongodb://$ipaddress:$port"
      //      var collection = attribute_map("collection")
      try {

        df = spark.read
          .format("org.apache.spark.sql.cassandra")
          .option("spark.cassandra.connection.host", host)
          .option("spark.cassandra.connection.port", port)
          .option("spark.cassandra.auth.username", username)
          .option("spark.cassandra.auth.password", password)
          .options(Map("keyspace" -> keyspace, "table" -> table, "cluster" -> "ScyllaCluster"))
          .load()

        //scyllaDF.show()

        isSuccess = true
      } catch {
        case ex: Exception => {
          isSuccess = false
          println("====> Error while Reading the dataframe")
          println(ex.getMessage)
          println("====> Stack trace")
          println(ex.getStackTrace.mkString("\n"))
          StoreOutput.exceptionWrite(spark, healthmonitor_kafka_topic, healthmonitor_kafka_bootstrapserver, healthmonitor_kafka_ssl_map, ex)
        }
      }
    }else if(source.equalsIgnoreCase("Azure_Data_Lake_Storage")){
      val attribute_map = castMapValuesToString(map)
      val path = attribute_map("path").split(",")
      val format = attribute_map("format")
      val storageAccountName = attribute_map("storage_account_name")
      val containerName = attribute_map("container_name")
      val azureAccountKey = decryptKey(attribute_map("azure_account_key"))
      val atts = "multiline,header,sep,mode,ignoreLeadingWhiteSpace,ignoreTrailingWhiteSpace,wholeFile,quote,escape".split(",")
      var optionsMap: Map[String, String] = Map()
      atts.foreach { att =>
        if (attribute_map.contains(att)) {
          optionsMap ++= Map(att -> attribute_map(att))
        }
      }
      spark.sparkContext.hadoopConfiguration.set(s"fs.azure.account.key.$storageAccountName.dfs.core.windows.net", azureAccountKey)
      try {
        val file_path = path.map(path => s"abfss://$containerName@$storageAccountName.dfs.core.windows.net/" + path)
        df = spark.read.format(format).options(optionsMap).load(file_path: _*)
        isSuccess = true
      } catch {
        case ex: Exception => {
          isSuccess = false
          println("====> Error while Reading the dataframe from Azure Data Lake Storage ")
          println(ex.getMessage)
          println("====> Stack trace")
          println(ex.getStackTrace.mkString("\n"))
          StoreOutput.exceptionWrite(spark, healthmonitor_kafka_topic, healthmonitor_kafka_bootstrapserver, healthmonitor_kafka_ssl_map, ex)
        }
      }
    }

    if (debug) {
      try {
        println("====> Printing the dataframe Read")
        df.show
      } catch {
        case ex: Exception => {
          isSuccess = false
          println("====> Error while Reading the dataframe")
          println(ex.getMessage)
          println("====> Stack trace")
          println(ex.getStackTrace.mkString("\n"))
          StoreOutput.exceptionWrite(spark, healthmonitor_kafka_topic, healthmonitor_kafka_bootstrapserver, healthmonitor_kafka_ssl_map, ex)
        }
      }
    }

    if (isSuccess) (df, isSuccess) else (spark.emptyDataFrame, isSuccess)
  } 

  def write(destination: String, map: scala.collection.mutable.Map[String, String], outdf: DataFrame) = {
    var df = checkMandatoryColumns(outdf, map)
    if (!df.schema.isEmpty) {
      if (map.contains("select_columns")) {
        df.createOrReplaceTempView("final_df")
        df = spark.sql(s"select ${map("select_columns")} from final_df")
      }

      if (destination.equalsIgnoreCase("oracle")) {
        var username = map("userid")
        var password = decryptKey(map("password"))
        var driver = com.jio.pulse.globalVariables.driver //tableDetails("driver")
        var ipaddress = map("ipaddress")
        var port = map("port")
        var database = map("database")
        var url = s"jdbc:oracle:thin:@$ipaddress:$port/$database"
        var tablename = map("table_name_or_query")
        /*if (map.contains("select_columns")) {
          df.createOrReplaceTempView("final_df")
          df = spark.sql(s"select ${map("select_columns")} from final_df")
        }*/
        if (!map.contains("table_name_stg")) {
          var trunc = map("truncate")
          var mode = map("mode")
          StoreOutput.writeOracle(df, mode, tablename, url, username, password, driver, trunc)
        } else {
          var table_name_stg = map("table_name_stg")
         
         /**
           * create table before merge
           */
          createTempTable(tablename: String, table_name_stg, driver, url, username, password)

          var master_update_columns = map.get("master_update_columns").getOrElse("")
          var temp_update_columns = map.get("temp_update_columns").getOrElse("")
          var master_insert_columns = map.get("master_insert_columns").getOrElse("")
          var temp_insert_columns = map.get("temp_insert_columns").getOrElse("")
          var merge_on_columns = map("merge_on_columns")
          var merge_where_condition = map.get("merge_where_condition").getOrElse("")
          StoreOutput.mergeToOracle(df, url, username, password, driver, tablename, table_name_stg, master_update_columns, temp_update_columns, master_insert_columns, temp_insert_columns, merge_on_columns, merge_where_condition)
        }
      } else if (destination.equalsIgnoreCase("mysql")) {
        var username = map("userid")
        var password = decryptKey(map("password"))
        var driver = com.jio.pulse.globalVariables.mysqldriver
        var ipaddress = map("ipaddress")
        var port = map("port")
        var database = map("database")
        var url = s"jdbc:mysql://$ipaddress:$port/$database"
        var tablename = map("table_name_or_query")
        var trunc = map("truncate")
        var mode = map("mode")
        StoreOutput.writeOracle(df, mode, tablename, url, username, password, driver, trunc)
      } else if (destination.equalsIgnoreCase("postgresql")) {
        var username = map("userid")
        var password = decryptKey(map("password"))
        var driver = com.jio.pulse.globalVariables.postgresqldriver
        var ipaddress = map("ipaddress")
        var port = map("port")
        var database = map("database")
        var url = s"jdbc:postgresql://$ipaddress:$port/$database"
        var tablename = map("table_name_or_query")
        /*var trunc = map("truncate")
        var mode = map("mode")
        StoreOutput.writeOracle(df, mode, tablename, url, username, password, driver, trunc)*/
        if (!map.contains("table_name_stg")) {
          var trunc = map("truncate")
          var mode = map("mode")
          var string_type = map.get("stringtype").getOrElse("")
          StoreOutput.writeOracle(df, mode, tablename, url, username, password, driver, trunc,string_type)
        } else {
          var table_name_stg = map("table_name_stg")
          var master_update_columns = map.get("master_update_columns").getOrElse("")
          var temp_update_columns = map.get("temp_update_columns").getOrElse("")
          var master_insert_columns = map.get("master_insert_columns").getOrElse("")
          var temp_insert_columns = map.get("temp_insert_columns").getOrElse("")
          var merge_on_columns = map("merge_on_columns")
          var merge_where_condition = map.get("merge_where_condition").getOrElse("")
          var string_type = map.get("stringtype").getOrElse("")
          println("attMap",map)
          StoreOutput.mergeToPostgresql(df, url, username, password, driver, tablename, table_name_stg, master_update_columns, temp_update_columns, master_insert_columns, temp_insert_columns, merge_on_columns, merge_where_condition,string_type)
        }
      } else if (destination.equalsIgnoreCase("sqlserver")) {
        var username = map("userid")
        var password = decryptKey(map("password"))
        var driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        var ipaddress = map("ipaddress")
        var port = map("port")
        var database = map("database")
        var url = s"jdbc:sqlserver://$ipaddress:$port;database=$database;encrypt=true;trustServerCertificate=true;"
        var tablename = map("table_name_or_query")
        /*var trunc = map("truncate")
        var mode = map("mode")
        StoreOutput.writeOracle(df, mode, tablename, url, username, password, driver, trunc)*/
        if (!map.contains("table_name_stg")) {
          var trunc = map("truncate")
          var mode = map("mode")
          StoreOutput.writeOracle(df, mode, tablename, url, username, password, driver, trunc)
        } else {
          var table_name_stg = map("table_name_stg")
          var master_update_columns = map.get("master_update_columns").getOrElse("")
          var temp_update_columns = map.get("temp_update_columns").getOrElse("")
          var master_insert_columns = map.get("master_insert_columns").getOrElse("")
          var temp_insert_columns = map.get("temp_insert_columns").getOrElse("")
          var merge_on_columns = map("merge_on_columns")
          var merge_where_condition = map.get("merge_where_condition").getOrElse("")
          StoreOutput.mergeToSqlserver(df, url, username, password, driver, tablename, table_name_stg, master_update_columns, temp_update_columns, master_insert_columns, temp_insert_columns, merge_on_columns, merge_where_condition)
        }
      } else if (destination.equalsIgnoreCase("clickhouse")) {
        var driver = "ru.yandex.clickhouse.ClickHouseDriver"
        var ipaddress = map("ipaddress")
        var port = map("port")
        var database = map("dbname")
        var url = s"jdbc:clickhouse://$ipaddress:$port/$database"
        var dbtable = map("tablename")
        var mode = map("mode")
        /*var createTableOptions = map("createTableOptions")
        var user = map.get("user").getOrElse("")
        var password = map.get("password").getOrElse("")
        if (user.equals("") && password.equals("")) {
          df.write.format("jdbc").option("url", url).option("dbtable", dbtable).option("driver", driver).option("createTableOptions", createTableOptions).mode(mode).save()
        } else {
          df.write.format("jdbc").option("url", url).option("dbtable", dbtable).option("driver", driver).option("user",user).option("password", password).option("createTableOptions", createTableOptions).mode(mode).save()
        }*/
        val atts = "createTableOptions,user,password,numPartitions,socket_timeout,batchsize,rewriteBatchedStatements".split(",")
        var optionsMap: Map[String, String] = Map()
        atts.foreach { att =>
          if (map.contains(att)) {
            optionsMap ++= Map(att -> map(att))
          }
        }
        df.write.format("jdbc").option("url", url).option("dbtable", dbtable).option("driver", driver).options(optionsMap).mode(mode).save()
      } else if (destination.equalsIgnoreCase("mongodb")) {
        var ipaddress = map("ip")
        var port = map("port")
        var username = map("username")
        var password = decryptKey(map("password"))
        var database = map("database")
        var uri = s"mongodb://$ipaddress:$port"
        var collection = map("collection")
        var mode = map("mode")
        df.write.format("mongo").
          mode("append").
          option("uri", uri).
          option("database", database).
          option("collection", collection).
          option("username", username).
          option("password", password).
          save()
      } else if (destination.equalsIgnoreCase("kafka")) {
        var bootstrap_server = map("bootstrap_servers")
        var topic = map("topics")
        var contains_write_as_json = map.contains("write_as_json")
        var write_as_json = true
        if (contains_write_as_json) {
          write_as_json = map("write_as_json").equalsIgnoreCase("true")
        }
        val isSSL = map.contains("security.protocol")
        var kafkaConfSSLMap: Map[String, String] = Map()
        if (isSSL) {
          if (map.contains("security.protocol")) {
            kafkaConfSSLMap = kafkaConfSSLMap ++ Map("kafka.security.protocol" -> map("security.protocol"))
          }
          if (map.contains("ssl.truststore.location")) {
            kafkaConfSSLMap = kafkaConfSSLMap ++ Map("kafka.ssl.truststore.location" -> map("ssl.truststore.location"))
          }
          if (map.contains("ssl.truststore.password")) {
            kafkaConfSSLMap = kafkaConfSSLMap ++ Map("kafka.ssl.truststore.password" -> decryptKey(map("ssl.truststore.password")))
          }
          if (map.contains("ssl.keystore.location")) {
            kafkaConfSSLMap = kafkaConfSSLMap ++ Map("kafka.ssl.keystore.location" -> map("ssl.keystore.location"))
          }
          if (map.contains("ssl.keystore.password")) {
            kafkaConfSSLMap = kafkaConfSSLMap ++ Map("kafka.ssl.keystore.password" -> decryptKey(map("ssl.keystore.password")))
          }
          if (map.contains("ssl.truststore.type")) {
            kafkaConfSSLMap = kafkaConfSSLMap ++ Map("kafka.ssl.truststore.type" -> map("ssl.truststore.type"))
          }
          if (map.contains("ssl.endpoint.identification.algorithm")) {
            kafkaConfSSLMap = kafkaConfSSLMap ++ Map("kafka.ssl.endpoint.identification.algorithm" -> map("ssl.endpoint.identification.algorithm"))
          }
        }
        if (write_as_json) {
          if (isSSL) {
            df.toJSON.toDF("value").write.format("kafka").options(kafkaConfSSLMap).option("kafka.bootstrap.servers", bootstrap_server).option("topic", topic).save()
          } else {
            df.toJSON.toDF("value").write.format("kafka").option("kafka.bootstrap.servers", bootstrap_server).option("topic", topic).save()
          }
        } else {
          var select_column = map("column_name")
          if (isSSL) {
            df.select(select_column).toDF("value").withColumn("value", col("value").cast("string")).write.format("kafka").options(kafkaConfSSLMap).option("kafka.bootstrap.servers", bootstrap_server).option("topic", topic).save()
          } else {
            df.select(select_column).toDF("value").withColumn("value", col("value").cast("string")).write.format("kafka").option("kafka.bootstrap.servers", bootstrap_server).option("topic", topic).save()
          }
        }
      } else if (destination.equalsIgnoreCase("hdfs")) {
        var url = map("url")
        var path = map("path")
        var format = map("format")
        var mode = if (!map.contains("mode")) "overwrite" else map("mode")

        var optionsMap = scala.collection.Map(
          "multiline" -> { if (map.contains("multiline")) "true" else "false" },
          "header" -> { if (map.contains("header")) "true" else "false" })

        if (format.equals("csv")) {
          optionsMap = optionsMap ++ scala.collection.Map("sep" -> map.get("sep").getOrElse(","))
        }

        if (map.contains("compression")) {
          optionsMap = optionsMap ++ scala.collection.Map("compression" -> map("compression"))
        }

        if (map.contains("partitionby")) {
          df.write.partitionBy(map("partitionby").split(","): _*).format(format).options(optionsMap).mode(mode).save(path)
        } else {
          df.write.format(format).options(optionsMap).mode(mode).save(url + path)
        }
      } else if (destination.equalsIgnoreCase("gcp_storagebucket")) {
        val service_account_email = map("service.account.email")
        val service_account_private_key_id = map("service.account.private.key.id")
        val service_account_private_key = map("service.account.private.key")
        val project_id = map("project.id")
	      val proxy = map.get("proxy.address").getOrElse("")
        spark.sparkContext.hadoopConfiguration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        spark.sparkContext.hadoopConfiguration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        spark.sparkContext.hadoopConfiguration.set("google.cloud.auth.service.account.enable", "true")
        spark.sparkContext.hadoopConfiguration.set("fs.gs.auth.service.account.email", service_account_email)
        spark.sparkContext.hadoopConfiguration.set("fs.gs.auth.service.account.private.key.id", service_account_private_key_id)
        spark.sparkContext.hadoopConfiguration.set("fs.gs.auth.service.account.private.key", service_account_private_key)
        spark.sparkContext.hadoopConfiguration.set("fs.gs.project.id", project_id)
	      if (!proxy.equals("")) {
		      spark.sparkContext.hadoopConfiguration.set("fs.gs.proxy.address", proxy)
	      }
        var url = map.get("url").getOrElse("")
        var path = map("path")
        var format = map("format")
        var mode = if (!map.contains("mode")) "overwrite" else map("mode")

        var optionsMap = scala.collection.Map(
          "multiline" -> { if (map.contains("multiline")) "true" else "false" },
          "header" -> { if (map.contains("header")) "true" else "false" })

        if (format.equals("csv")) {
          optionsMap = optionsMap ++ scala.collection.Map("sep" -> map.get("sep").getOrElse(","))
        }

        if (map.contains("compression")) {
          optionsMap = optionsMap ++ scala.collection.Map("compression" -> map("compression"))
        }

        if (map.contains("partitionby")) {
          df.write.partitionBy(map("partitionby").split(","): _*).format(format).options(optionsMap).mode(mode).save(path)
        } else {
          df.write.format(format).options(optionsMap).mode(mode).save(url + path)
        }
      } else if (destination.equalsIgnoreCase("aws_s3")) {
        val access_key_id = map("aws_access_key_id")
        val aws_secret_access_key = decryptKey(map("aws_secret_access_key"))
        val endpoint = map("fs.s3a.endpoint")
        val bucket = map("bucket")
        val proxyhost = map.get("proxy.address").getOrElse("")
        val proxyport = map.get("proxy.port").getOrElse("")
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", access_key_id)
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", aws_secret_access_key)
        if (!proxyhost.equals("") && !proxyport.equals("")) {
          spark.sparkContext.hadoopConfiguration.set("fs.s3a.proxy.host", proxyhost)
          spark.sparkContext.hadoopConfiguration.set("fs.s3a.proxy.port", proxyport)
        }
        if (!endpoint.equals("")) {
          spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", endpoint)
        }
        val url = map.get("url").getOrElse("")
        val path = map("path")
        val format = map("format")
        val mode = if (!map.contains("mode")) "overwrite" else map("mode")
        var optionsMap = scala.collection.Map(
          "multiline" -> {
            if (map.contains("multiline")) "true" else "false"
          },
          "header" -> {
            if (map.contains("header")) "true" else "false"
          })

        if (format.equals("csv")) {
          optionsMap = optionsMap ++ scala.collection.Map("sep" -> map.get("sep").getOrElse(","))
        }

        if (map.contains("compression")) {
          optionsMap = optionsMap ++ scala.collection.Map("compression" -> map("compression"))
        }
        val file_path = s"s3a://$bucket/$path"
        if (map.contains("partitionby")) {
          df.write.partitionBy(map("partitionby").split(","): _*).format(format).options(optionsMap).mode(mode).save(file_path)
        } else {
          df.write.format(format).options(optionsMap).mode(mode).save(file_path)
        }
      } else if (destination.equalsIgnoreCase("Azure_Data_Lake_Storage")) {
        val path = map("path")
        val format = map("format")
        val storageAccountName = map("storage_account_name")
        val containerName = map("container_name")
        val azureAccountKey = decryptKey(map("azure_account_key"))
        spark.sparkContext.hadoopConfiguration.set(s"fs.azure.account.key.$storageAccountName.dfs.core.windows.net", azureAccountKey)
        val mode = if (!map.contains("mode")) "append" else map("mode")
        val dir_path=s"abfss://$containerName@$storageAccountName.dfs.core.windows.net/"+path
        var optionsMap = scala.collection.Map(
          "header" -> {
            if (map.contains("header")) "true" else "false"
          })
        df.coalesce(1).write.format(format).options(optionsMap).mode(mode).save(dir_path)
      } else if (destination.equalsIgnoreCase("Azure_Blob_Storage")) {
        val path = map("path")
        val format = map("format")
        val storage_account_name = map("storage_account_name")
        val storage_account_key = decryptKey(map("storage_account_key"))
        val container_name = map("container_name")
        val proxyhost = map.get("proxy.address").getOrElse("")
        val proxyport = map.get("proxy.port").getOrElse("")
        val mode = if (!map.contains("mode")) "append" else map("mode")
        val atts = "multiline,header,sep,mode,ignoreLeadingWhiteSpace,ignoreTrailingWhiteSpace,wholeFile,quote,escape".split(",")
        var optionsMap: Map[String, String] = Map()
        atts.foreach { att =>
          if (map.contains(att)) {
            optionsMap ++= Map(att -> map(att))
          }
        }
        spark.sparkContext.hadoopConfiguration.set(s"fs.azure.account.key.$storage_account_name.blob.core.windows.net", storage_account_key)
        if (!proxyhost.equals("") && !proxyport.equals("")) {
          spark.sparkContext.hadoopConfiguration.set("fs.azure.http.proxy.host", proxyhost)
          spark.sparkContext.hadoopConfiguration.set("fs.azure.http.proxy.port", proxyport)
        }
        val blobPath = s"wasbs://$container_name@$storage_account_name.blob.core.windows.net/$path"
        df.coalesce(1).write.format(format).options(optionsMap).mode(mode).save(blobPath)
      } else if (destination.equalsIgnoreCase("hive")) {
        var database = map("database")
        var table = map("table_name")
        if (map.contains("partitionby"))
          df.write.format("hive").partitionBy(map("partitionby").split(","): _*).saveAsTable(database + "." + table)
        else
          df.write.format("hive").saveAsTable(database + "." + table)
      } else if (destination.equalsIgnoreCase("elasticsearch")) {
        var optionsMap: scala.collection.mutable.Map[String, String] = map.filter(x => x._1.contains("es.")).map(x => if (x._1.contains(".pass")) (x._1 -> decryptKey(x._2)) else x)
        var node = map("node")
        var port = map("port")
        var index = map("index")
        var username = map("username")
        var password = decryptKey(map("password"))
        var mode = map("mode")
        var indexTimestamp = if (map.contains("index_timestamp")) map("index_timestamp") else ""
        var current_date = ReadInput.getDate("yyyy.MM.dd")
        if (indexTimestamp.equalsIgnoreCase("current_timestamp")) index = index + "-" + current_date
        df.write.format("org.elasticsearch.spark.sql")
          .option("es.nodes", node)
          .option("es.port", port)
          .option("es.net.http.auth.user", username)
          .option("es.net.http.auth.pass", password)
          .option("es.write.operation", "index")
          .options(optionsMap)
          .option("es.batch.write.refresh", false)
          .mode(mode)
          .save(index + "/_doc")
      } else if (destination.equalsIgnoreCase("opensearch")) {
        var optionsMap: scala.collection.mutable.Map[String, String] = map.filter(x => x._1.contains("opensearch.")).map(x => if (x._1.contains(".pass")) (x._1 -> decryptKey(x._2)) else x)
        var node = map("node")
        var port = map("port")
        var index = map("index")
        var username = map("username")
        var password = decryptKey(map("password"))
        var mode = map("mode")
        var indexTimestamp = if (map.contains("index_timestamp")) map("index_timestamp") else ""
        var current_date = ReadInput.getDate("yyyy.MM.dd")
        if (indexTimestamp.equalsIgnoreCase("current_timestamp")) index = index + "-" + current_date
        df.write.format("org.opensearch.spark.sql")
          .option("opensearch.nodes", node)
          .option("opensearch.port", port)
          .option("opensearch.net.http.auth.user", username)
          .option("opensearch.net.http.auth.pass", password)
          .option("opensearch.write.operation", "index")
          .options(optionsMap)
          .option("opensearch.batch.write.refresh", false)
          .mode(mode)
          .save(index )
      } else if (destination.equalsIgnoreCase("CouchDB")) {
        var host = map("host")
        var username = map("username")
        var database = map("database")
        var password = decryptKey(map("password"))
        var protocol = map("protocol")

        df.write
          .format("org.apache.bahir.cloudant")
          .option("cloudant.host", host)
          .option("cloudant.username", username)
          .option("cloudant.password", password)
          .option("database", database)
          .option("cloudant.protocol", protocol)
          .save()
      }else if (destination.equalsIgnoreCase("Redis")) {
        var host = map("host")
        var port = map("port")
        var table = map("table")
        var password = decryptKey(map("password"))
        var SaveMode = map("SaveMode")
        df.write
          .format("org.apache.spark.sql.redis")
          .option("host", host)
          .option("port", port)
          .option("table", table)
          .option("password", password)
          .mode(SaveMode)
          .save()
      }else if (destination.equalsIgnoreCase("neo4j")) {
                var username = map("username")
        var password = decryptKey(map("password"))
        var ipaddress = map("ipaddress")
        var port = map("port")
        var url = s"bolt://${ipaddress}:${port}"
        var Mode = map("SaveMode")
        val query=map("query")
        val batchSize=map("batchSize")
        df.write.format("org.neo4j.spark.DataSource")
            .mode(Mode)
            .option("url", url)
            .option("authentication.basic.username", username)
            .option("authentication.basic.password", password)
            .option("batch.size", batchSize)
            .option("query",query)
            .save()
      }
      else if (destination.equalsIgnoreCase("cassandra")) {
        var ipaddress = map("host")
        var port = map("port")
        var username = map("username")
        var password = decryptKey(map("password"))
        var keyspace = map("keyspace")
        var table = map("table")
        var savemode = map("savemode")

        val writeDF = df
          .write
          .format("org.apache.spark.sql.cassandra")
          .mode(savemode)

        if (savemode.equalsIgnoreCase("overwrite")) {
          writeDF.option("confirm.truncate", "true")
        }

        writeDF
          .option("spark.cassandra.connection.host", ipaddress)
          .option("spark.cassandra.connection.port", port)
          .option("spark.cassandra.auth.username", username)
          .option("spark.cassandra.auth.password", password)
          .options(Map("keyspace" -> keyspace, "table" -> table))
          .save()
      } else if (destination.equalsIgnoreCase("CockroachDB")) {
        val url = map("url")
        val dbtable = map("dbtable")
        val saveMode = map("SaveMode")
        df.write
          .format("jdbc")
          .option("url", url)
          .option("dbtable", dbtable)
          .mode(saveMode)
          .save()
      } else if (destination.equalsIgnoreCase("sqlite")) {
        var path = map("path")
        var jdbcUrl = "jdbc:sqlite:" + path
        var table = map("table")
        var savemode = map("savemode")

        df.write
          .format("jdbc")
          .mode(savemode)
          .option("url", jdbcUrl)
          .option("dbtable", table)
          .save()
      }
      else if (destination.equalsIgnoreCase("arangoDB")) {
        var database = map("database")
        var table = map("table")
        var user = map("user")
        var password = decryptKey(map("password"))
        var endpoints = map("endpoints")
        var savemode = map("savemode")

        val writeDF = df
          .write
          .format("com.arangodb.spark")
          .mode(savemode)

        if (savemode.equalsIgnoreCase("overwrite")) {
          writeDF.option("confirmTruncate", "true")
        }

        writeDF
          
          .options(Map(
            "database" -> database,
            "table" -> table,
            "user" -> user,
            "password" -> password,
            "endpoints" -> endpoints))
          .save()
      }
      else if (destination.equalsIgnoreCase("SCYLLADB")) {

        var host = map("host")
        var port = map("port")
        var username = map("username")
        var password = decryptKey(map("password"))
        var keyspace = map("keyspace")
        var table = map("table")
        var savemode = map("savemode")

        val writeDF = df
          .write
          .format("org.apache.spark.sql.cassandra")
          .mode(savemode)

        if (savemode.equalsIgnoreCase("overwrite")) {
          writeDF.option("confirm.truncate", "true")
        }

        writeDF
          .option("spark.cassandra.connection.host", host)
          .option("spark.cassandra.connection.port", port)
          .option("spark.cassandra.auth.username", username)
          .option("spark.cassandra.auth.password", password)
          .options(Map("keyspace" -> keyspace, "table" -> table, "cluster" -> "ScyllaCluster"))
          .save()
      }
    }
  }

  def checkMandatoryColumns(outdf: DataFrame, map: scala.collection.mutable.Map[String, String]): DataFrame = {
    var df = outdf
    if (map.contains("mandatory_columns")) {
      val dfcolumns = df.columns.map(_.toLowerCase())
      var filter_condition = ""
      map("mandatory_columns").split(",").map(_.trim.toLowerCase).foreach { column =>
        filter_condition += s"$column is not null and "
        if (!dfcolumns.contains(column)) {
          df = df.withColumn(column, lit(null))
        }
      }
      filter_condition = filter_condition.substring(0, filter_condition.lastIndexOf("and"))
      val validdf = df.filter(filter_condition)
      val errodf = df.except(validdf)

      val error_kafka_map = healthmonitor_kafka_ssl_map ++ Map("kafka.bootstrap.servers" -> healthmonitor_kafka_bootstrapserver, "topic" -> healthmonitor_kafka_topic)
      val appid = spark.sparkContext.getConf.get("spark.app.id")
      val appname = spark.sparkContext.getConf.get("spark.app.name")
      errodf.toJSON
        .withColumn("error", expr(s"concat('Error: Missing Mandatory Fields ${map("mandatory_columns")}',': data: ', value)"))
        .withColumn("Technology", lit("Application"))
        .withColumn("SubTech", lit("Application"))
        .withColumn("appId", lit(appid))
        .withColumn("appName", lit(appname))
        .withColumn("insert_timestamp", expr("from_unixtime(unix_timestamp(current_timestamp,'yyyy-MM-dd HH:mm:ss'),'dd-MMM-yyyy hh:mm.ss a')"))
        .withColumn("errorData", col("error"))
        .withColumn("type", lit("error"))
        .withColumn("job_instance_id", lit(job_instance_id))
        .drop("value")
        .toJSON.toDF("value").write.format("kafka").options(error_kafka_map).save()

      df = if (validdf.head(1).isEmpty) spark.emptyDataFrame else validdf
    }
    df
  }
 
  def createTempTable(masterTable: String, temTamble: String, driver: String, url: String, username: String, pass: String): Map[String, String] = {
    var map = Map[String, String]()
    Class.forName(driver)

    val connection = DriverManager.getConnection(url, username, pass)
    val statement = connection.createStatement()
    try {
      val columnsList = ListBuffer.empty[String]
      val rs = statement.executeQuery(s"select * from $masterTable");
      val rsmd = rs.getMetaData();
      println(s"table name : $temTamble")
      // println(s" url :  $url  , username: $username, password : $pass")
      var query = s"create table $temTamble ("
      for (i <- 1 to rsmd.getColumnCount()) {
        map += (rsmd.getColumnName(i).toLowerCase() -> rsmd.getColumnTypeName(i).toLowerCase())
        if (rsmd.getColumnTypeName(i).equalsIgnoreCase("VARCHAR2") || rsmd.getColumnTypeName(i).equalsIgnoreCase("VARCHAR")) {
          query = query + s" ${rsmd.getColumnName(i)} ${rsmd.getColumnTypeName(i)}(${rsmd.getColumnDisplaySize(i)}), "
          columnsList += rsmd.getColumnName(i).toUpperCase()
        } else {
          query = query + s" ${rsmd.getColumnName(i)} ${rsmd.getColumnTypeName(i)} ,"
          columnsList += rsmd.getColumnName(i).toUpperCase()
        }
      }
      query = query.substring(0, query.lastIndexOf(",")) + ")"

      println("table query :" + query)
      statement.executeUpdate(query)
      println("table created")
      statement.close()
      connection.close()
    } catch {
      case ex: Exception => {
        println(ex.printStackTrace())
        statement.close()
        connection.close()
      }
    }
    map
  }

}