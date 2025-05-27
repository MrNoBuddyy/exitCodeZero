package com.jio.pulse

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import com.jio.encrypt.DES3._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import com.jio.utilities.{JsonParser, ReadInput, StoreOutput, Utility}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.PreferConsistent
import org.apache.spark.streaming.kafka010.Subscribe
import org.apache.spark.streaming.dstream.DStream
import com.jio.utilities.ReadInput.getDate

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}
import scala.util.parsing.json.JSON.parseFull
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import com.typesafe.config.ConfigFactory

import java.io.File
import com.jio.pulse.globalVariables._

import scala.collection.mutable.WrappedArray
import scalaj.http.Http
import scalaj.http.HttpOptions
import scalaj.http.HttpResponse

import scala.collection.immutable.HashMap
import org.json._
import com.jio.utilities.BatchProcessKafka.getData
import com.jio.utilities.StoreOutput.writeToES
import com.jio.utilities.dataframeUtilities._
import com.jio.pulse.ReadWriteUtility._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import com.jio.encrypt.CMS
import com.jio.pulse.plugins.Loop.loop_workflow
import com.jio.pulse.plugins.FileListing.file_listing_workflow
import com.jio.pulse.plugins.SchemaValidation.schema_validation_workflow
import com.jio.pulse.plugins.FileMigration.file_migration_workflow
import com.jio.pulse.plugins.{FileRename, StructuredStreaming}

import java.io.ByteArrayInputStream
import java.util.zip.GZIPInputStream
import org.apache.commons.io.IOUtils
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.sql.expressions.UserDefinedFunction

import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

object batchTime {
  var batch_time: Long = 0
}

object maxRecordedTime {
  var max_rec_time = "1970-01-01 00:00:00"
}

object genericUtility extends App {

  /*System.setProperty("hadoop.home.dir", "Z:\\CommonRepo\\winutils")
  //val conf = new SparkConf().set("spark.streaming.kafka.consumer.cache.enabled", "false").setMaster("local[*]").setAppName("Generic_Processing_Utility")
  val sc = spark.sparkContext
  //val spark = SparkSession.builder().config(conf).config("spark.sql.codegen.wholeStage", "false").config("spark.sql.crossJoin.enabled", "true").getOrCreate()
  sc.setLogLevel("error")
  //import spark.implicits._
  def get_filesystem(): FileSystem = {
    val conf = new Configuration
//    conf.addResource(new Path("Z:\\Dinesh_Patil\\DP\\hdfs_conf\\core-site.xml"))
//    conf.addResource(new Path("Z:\\Dinesh_Patil\\DP\\hdfs_conf\\hdfs-site.xml"))
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val fs = FileSystem.get(conf)
    fs
  }
  var connection: FileSystem = get_filesystem()*/

  //val conf = new SparkConf().set("spark.streaming.kafka.consumer.cache.enabled", "false")
  //val sc = new SparkContext(conf)
  //val spark = SparkSession.builder().config(conf).config("spark.sql.codegen.wholeStage", "false").config("spark.sql.crossJoin.enabled", "true").getOrCreate()
  val sc = spark.sparkContext

  spark.sparkContext.setLogLevel("error")
  var connection: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  //spark.sparkContext.setCheckpointDir("/spark/workdirectory/spark/GP/checkpoint")

  job_instance_id = args(0) //64 //57 //
  file_path = args(1) //"""C:\Users\praneet.pulivendula\generic_parser_workspace\gp-standalone-withAPI\resources\config.properties""" //
  if (args.length > 2) {
    auto_offset_reset = args(2)
  }
  cl_args = args.clone()
  spark_app_name = spark.sparkContext.getConf.get("spark.app.name")
  configuration = ConfigFactory.parseFile(new File(file_path)) //.load("config.properties")

  url = configuration.getString("oracle.url")
  username = configuration.getString("oracle.username")
  password = decryptKey(configuration.getString("oracle.password"))
  driver = configuration.getString("oracle.driver")
  job_configuration_query = configuration.getString("pulse.job.configuration").replace("$job_instance_id", job_instance_id)

  mysqldriver = configuration.getString("mysql.driver")
  postgresqldriver = configuration.getString("postgresql.driver")

  lazy val hiveContext = new org.apache.spark.sql.hive.HiveContext(spark.sparkContext)

  spark.sqlContext.udf.register("ApiCallingFunction", ApiUDF.triggerApi _)

  /*
   * Setting all the required configurations
   */

  /**
   * The array_intersect udf takes two columns as input and returns a column which is a resultant of intersection of the two arrays
   *
   * @author praneet.pulivendula
   * @param arr1 1st array column
   * @param arr2 2nd array column
   * @return arr1 intersection arr2
   */

  val array_intersect = udf { (arr1: WrappedArray[String], arr2: WrappedArray[String]) => {
    arr1.intersect(arr2)
  }
  }
  spark.sqlContext.udf.register("array_intersect", array_intersect)

  /**
   * The toArray user defined function takes a array which is in a string format and the data type of the elements of the array and returns array
   *
   * @author praneet.pulivendula
   * @param data     array column which is in string format
   * @param datatype data type of the elements of the array
   * @return array column
   */

  val toArray = udf { (data: String, datatype: String) => {
    var jsonArray = new JSONArray()
    var arr: Array[String] = Array()
    try {
      jsonArray = new JSONArray(data)
      if (datatype.equalsIgnoreCase("array")) {
        val objects = (0 until jsonArray.length).map(x => jsonArray.getJSONArray(x))
        objects.foreach { elem =>
          arr :+= elem.toString
        }
      } else if (datatype.equalsIgnoreCase("json")) {
        val objects = (0 until jsonArray.length).map(x => jsonArray.getJSONObject(x))
        objects.foreach { elem =>
          arr :+= elem.toString
        }
      } else if (datatype.equalsIgnoreCase("string")) {
        val objects = (0 until jsonArray.length).map(jsonArray.getString)
        objects.foreach { elem =>
          arr :+= elem.toString
        }
      } else if (datatype.equalsIgnoreCase("number")) {
        val objects = (0 until jsonArray.length).map(jsonArray.getDouble)
        objects.foreach { elem =>
          arr :+= elem.toString
        }
      } else if (datatype.equalsIgnoreCase("integer")) {
        val objects = (0 until jsonArray.length).map(jsonArray.getInt)
        objects.foreach { elem =>
          arr :+= elem.toString
        }
      }
    } catch {
      case e: Exception => {
        println("Had a Parseing Exception while trying to parse the Json: " + data)
        null
      }
    }
    arr
  }
  }
  spark.sqlContext.udf.register("toArray", toArray)

  val arrayUnion = udf { (arr1: scala.collection.mutable.WrappedArray[String], arr2: scala.collection.mutable.WrappedArray[String]) =>
    arr1.toSet.union(arr2.toSet).toArray
  }

  spark.sqlContext.udf.register("arrayUnion", arrayUnion)

  val getValueFromJson = udf((jsonObj: String, path: String) => {
    implicit val formats = DefaultFormats // set up the JSON formats

    // Parse the JSON object
    val json = JsonMethods.parse(jsonObj)

    // Extract the value at the given path
    val value = (json \ path).extractOpt[String]

    // Return the extracted value
    value.getOrElse(null)
  })
  spark.sqlContext.udf.register("getValueFromJson", getValueFromJson)

  val getValueFromStruct = udf((struct: Row, path: String) => {
    // Extract the value of the specified path from the struct
    try {
      val pathParts = path.split("\\.")
      var currentValue: Any = struct
      for (part <- pathParts) {
        currentValue = currentValue.asInstanceOf[Row].getAs[Any](part)
      }
      currentValue.toString
    } catch {
      case e: Exception => {
        //   e.printStackTrace()
        // println("Had a Parseing Exception while trying to parse the Json: " + data)
        null
      }
    }
  })

  spark.sqlContext.udf.register("getValueFromStruct", getValueFromStruct)


  val replaceVariableFields = udf((resource_variables: String, struct: Row, variables: String) => {
    val vars = variables.split(",")
    var temp = resource_variables
    vars.foreach(x => {
      try {
        temp = temp.replace("{" + x + "}", struct.asInstanceOf[Row].getAs[Any](x).toString)
      } catch {
        case e: Exception => {
          //   e.printStackTrace()
          // println("Had a Parseing Exception while trying to parse the Json: " + data)
          temp
        }
      }
    }
    )
    temp
  })

  spark.sqlContext.udf.register("replaceVariableFields", replaceVariableFields)

  val gunzip = udf((input: Array[Byte]) => {
    val inputStream = new GZIPInputStream(new ByteArrayInputStream(input))
    val outputStream = new ByteArrayOutputStream()
    IOUtils.copy(inputStream, outputStream)
    inputStream.close()
    outputStream.close()
    outputStream.toString("UTF-8")
  })

  spark.sqlContext.udf.register("gunzip", gunzip)

  val jsonToStruct = udf((jsonString: String, key_col: String, value_col: String) => {
    val keyValuePairs = collection.mutable.Map[String, String]()
    try {
      val jsonArray = new JSONArray(jsonString)

      for (i <- 0 until jsonArray.length()) {
        val jsonObj = jsonArray.getJSONObject(i)
        val key = jsonObj.getString(key_col)
        val valueObject = jsonObj.getJSONObject(value_col)
        val value = valueObject.keys().next() match {
          case "stringValue" => valueObject.getString("stringValue")
          case "intValue" => valueObject.getString("intValue")
          case _ => ""
        }
        keyValuePairs += (key -> value)
      }

      keyValuePairs.toMap
    } catch {
      case ex: Exception =>
        //println(ex.getMessage.substring(0, math.min(1000, ex.getMessage.size)))
        keyValuePairs.toMap
    }
  })

  spark.sqlContext.udf.register("jsonToStruct", jsonToStruct)

  /**
   * Loads all the configuration queries from the PLS_JOB_CONF table
   */

  def loadJobConfiguration = {
    val job_configuration = ReadConfiguration(url, driver, username, password, job_configuration_query)

    json_parsing_fields_details = job_configuration.getString("pulse.json.parsing")
    json_field_replacment_details = job_configuration.getString("pulse.field.replacement.details")
    json_kpi_filter_master = job_configuration.getString("pulse.kpi.filter.master")
    dyn_enrichment_details = job_configuration.getString("pulse.dynamic.enrichment")

    dyn_aggregation_details = job_configuration.getString("pulse.dynamic.aggregation")
    agg_filter = job_configuration.getString("pulse.aggregation.filter")
    agg_grouping = job_configuration.getString("pulse.aggregation.grpcols")
    agg_function = job_configuration.getString("pulse.aggregation.function")

    delimiter_metadata = job_configuration.getString("pulse.delimiter.metadata")
    contro_point_table = job_configuration.getString("pulse.controlpoint")
    workflow_query = job_configuration.getString("pulse.job.workflow")
    refresh = job_configuration.getString("configuration.refresh.intervals")
    errorLogTable = job_configuration.getString("error.table")

    startTimeEndTimeFormat = job_configuration.getString("startTimeEndTimeFormat")
    ngo_health_monitor = job_configuration.getString("health.monitor")

    connection_master = job_configuration.getString("pulse.connection.string.master")
    read_write_details = job_configuration.getString("pulse.read.write.details")
    join_details = job_configuration.getString("pulse.join")
    enrichment_read_details = job_configuration.getString("pulse.read.enrichment")

    field_transformation_details = job_configuration.getString("pulse.field.transformation.details")
    expression_evaluation_details = job_configuration.getString("pulse.expression.evaulation.details")

    workflow_filter_condition_details = job_configuration.getString("pulse.filter.condition.details")

    coalesce_explode_details = job_configuration.getString("pulse.coalesce.explode.details")

    /*
     * Read API Variables
     */
    read_api_dependency_details = job_configuration.getString("read.api.dependency.details")
    read_api_details = job_configuration.getString("read.api.details")
    read_payload_details = job_configuration.getString("read.payload.details")
    read_header_details = job_configuration.getString("read.header.details")
    read_url_parameter_details = job_configuration.getString("read.url.parameters.details")
    read_json_parsing_details = job_configuration.getString("read.api.json.parsing.details")
    read_add_fields_details = job_configuration.getString("read.add.fields.details")
    read_dml = job_configuration.getString("read.dml")
    read_dml_details = job_configuration.getString("read.dml.details")

    api_cookie_details = job_configuration.getString("read.api.cookie.details")

    api_additional_att_details = job_configuration.getString("read.api.add.att.details")

    /*
     * End of Read API Variables
     */

    /*
     * API Related Details
     */
    api_read_details = job_configuration.getString("pulse.api.read")
    api_header_details = job_configuration.getString("pulse.api.header.details")
    api_json_parsing_details = job_configuration.getString("pulse.api.json.parsing")

    enrich_json_parsing_details = job_configuration.getString("pulse.enrich.json.parsing")

    /*
     * Health Monitor Kafka Details
     */
    healthmonitor_kafka_bootstrapserver = job_configuration.getString("healthmonitor.kafka.bootstrap.servers")
    healthmonitor_kafka_topic = job_configuration.getString("healthmonitor.kafka.topic")
    healthmonitor_kafka_isSSL = job_configuration.getString("healthmonitor.kafka.isSSL")
    if (healthmonitor_kafka_isSSL.equalsIgnoreCase("true")) {
      healthmonitor_kafka_ssl_map += ("kafka.security.protocol" -> job_configuration.getString("kafka.security.protocol"))
      healthmonitor_kafka_ssl_map += ("kafka.ssl.truststore.location" -> job_configuration.getString("kafka.ssl.truststore.location"))
      healthmonitor_kafka_ssl_map += ("kafka.ssl.truststore.password" -> decryptKey(job_configuration.getString("kafka.ssl.truststore.password")))
      healthmonitor_kafka_ssl_map += ("kafka.ssl.keystore.location" -> job_configuration.getString("kafka.ssl.keystore.location"))
      healthmonitor_kafka_ssl_map += ("kafka.ssl.keystore.password" -> decryptKey(job_configuration.getString("kafka.ssl.keystore.password")))
      healthmonitor_kafka_ssl_map += ("kafka.ssl.truststore.type" -> job_configuration.getString("kafka.ssl.truststore.type"))
      healthmonitor_kafka_ssl_map += ("kafka.ssl.endpoint.identification.algorithm" -> "")
    }

    /*
     * Pivot operation details
     */

    pivot_details = job_configuration.getString("pulse.pivot.details")

    /*
     * Column Rename details
     */

    column_rename_details = job_configuration.getString("pulse.column.rename.details")

    /*
     * expression evaluation details
     */

    expr_eval_dtl = job_configuration.getString("pulse.expr.eval.dtls.details")

    /*
     * CMS Details
     */

    cms_url = job_configuration.getString("cms.url")
    cms_user = job_configuration.getString("cms.user")
    cms_pass = decryptKey(job_configuration.getString("cms.pass"))
    cms_aes_iv = job_configuration.getString("cms.aes.iv")
    cms_aes_key = job_configuration.getString("cms.aes.key")
    cms = CMS(cms_url, cms_user, cms_pass, cms_aes_iv, cms_aes_key)

    /*
 * Spark Sql Exec Details
 */

    spark_sql_exec_details = job_configuration.getString("pulse.spark.sql.exec")

    /*
     * DML Execution Details
     */

    dml_exec = job_configuration.getString("pulse.dml.exec")
    dml_exec_details = job_configuration.getString("pulse.dml.details.exec")

    /*
     * Stream Dynamic Read Details
     */

    stream_dynamic_read_att = job_configuration.getString("pulse.stream.dynamic.read.att")
    stream_dynamic_read_con = job_configuration.getString("pulse.stream.dynamic.read.con")

    /*
     * Enrich fields
     */
    enrich_fields = job_configuration.keys().filter(x => x.startsWith("enrich."))
    enrich_fields.foreach { field =>
      enrich_fields_values :+= (field.split("\\.")(1), job_configuration.getString(field))
    }
    /*
     * Conditional Workflow
     */
    conditional = job_configuration.getString("pulse.conditional")
    conditional_details = job_configuration.getString("pulse.conditional.dtls")

    /*
     * Repartition and Coalesce details
     */
    repartition_details = job_configuration.getString("pulse.repartition")
    coalesce_details = job_configuration.getString("pulse.coalesce")

    /*
     * Dynamic SQL details
     */
    dyn_sql_details = job_configuration.getString("pulse.dyn.sql")
    dyn_sql_dist_col_details = job_configuration.getString("pulse.dyn.sql.dist.col")
    dyn_sql_op_info_details = job_configuration.getString("pulse.dyn.sql.op.info")
    dyn_sql_add_attr_details = job_configuration.getString("pulse.dyn.sql.add.attr")
    dyn_sql_json_parsing_details = job_configuration.getString("pulse.dyn.sql.json.parsing")

    /**
     * GP Variables
     */
    variable_details = job_configuration.getString("pulse.gp.variables")

    /**
     * Loop Details
     */
    loop_details = job_configuration.getString("pulse.loop")
    loop_col_details = job_configuration.getString("pulse.loop.dtls")
    /**
     * File Handeling
     */
    file_listing_details = job_configuration.getString("pulse.file.listing")
    schema_validation_details = job_configuration.getString("pulse.schema.validation")
    file_migration_details = job_configuration.getString("pulse.file.migration")
    file_listing_add_attr_details = job_configuration.getString("pulse.file.listing.add.attr")
    schema_validation_add_attr_details = job_configuration.getString("pulse.schema.validation.add.attr")
    file_migration_add_attr_details = job_configuration.getString("pulse.file.migration.add.attr")

    /**
     * fil rename
     */
    file_rename_workflow = job_configuration.getString("pulse.filerename.workflow")
    file_rename_detail = job_configuration.getString("pulse.filerename.detail")
    file_rename_add_attr_details = job_configuration.getString("pulse.filerename.add_att")

  }

  loadJobConfiguration

  /**
   * The function loadConfituration loads all the required configuration tables required for the processing into the
   * above defined Global variables
   *
   * @author praneet.pulivendula
   */

  def loadConfituration = {
    println("===> Loading Configuration")
    var finalworkflowDF = ReadInput.readInput(spark, "(" + workflow_query + job_instance_id + ") x", url, username, password, driver).na.fill("")
    finalworkflowDF = finalworkflowDF.select(finalworkflowDF.columns.map(c => col(c).as(c.toLowerCase)): _*)
    finalworkflowDF = finalworkflowDF.withColumn("dependency_id", when(col("dependency_id").isNull, lit(0)).otherwise(col("dependency_id")))
   var partcl = org.apache.spark.sql.expressions.Window.partitionBy(col("dependency_id"), col("is_exception"))
    finalworkflowDF = finalworkflowDF.join(
        finalworkflowDF.filter("is_exception='N'").select(col("dependency_id").as("next_dep_id"), first(col("workflow_id")).over(partcl.orderBy(lit(1))).as("next_id")).distinct.as("nextdf"),
        finalworkflowDF.col("workflow_id") === col("next_dep_id"),
        "left").join(
        finalworkflowDF.filter("is_exception='Y'").select(col("dependency_id").as("next_exe_id"), first(col("workflow_id")).over(partcl.orderBy(lit(1))).as("exception_id")).distinct.as("exdf"),
        finalworkflowDF.col("workflow_id") === col("next_exe_id"),
        "left")
      .withColumn("next_id", when(col("next_id").isNull, lit(-1)).otherwise(col("next_id")))
      .withColumn("exception_id", when(col("exception_id").isNull, lit(-1)).otherwise(col("exception_id")))
      .drop("next_dep_id")
      .drop("next_exe_id")
    finalworkflowDF = finalworkflowDF.withColumn("fork", (count(col("*")) over partcl).cast(types.IntegerType)).orderBy("workflow_id", "dependency_id")
    finalworkflowDF.show
    println("====> Loading Workflow Configuration")
    workflow_array = finalworkflowDF.collect
    workflow_array_persist = workflow_array

    println("====> Loading Connection Details Configuration")
    var connection_details_df = ReadInput.readInput(spark, "(" + connection_master + ") x", url, username, password, driver).na.fill("")
    connection_array = connection_details_df.select(connection_details_df.columns.map(c => col(c).as(c.toLowerCase)): _*)
     .withColumn("value", when(col("is_cms").equalTo('Y'), getCMSCred(cms_url, cms_user, cms_pass, cms_aes_iv, cms_aes_key)(col("value"),lit("base64"))).otherwise(col("value")))
    .collect //.cache
    println("====> Loading Read Write Configuration")
    var read_write_details_df = ReadInput.readInput(spark, "(" + read_write_details + job_instance_id + ") x", url, username, password, driver).na.fill("")
    read_write_array = read_write_details_df.select(read_write_details_df.columns.map(c => col(c).as(c.toLowerCase)): _*)
    .withColumn("value", when(col("is_cms").equalTo('Y'), getCMSCred(cms_url, cms_user, cms_pass, cms_aes_iv, cms_aes_key)(col("value"),lit("base64"))).otherwise(col("value")))
    .collect //.cache

    stream_dynamic_read_att_array = ReadInput.readInput(spark, "(" + stream_dynamic_read_att + job_instance_id + ") x", url, username, password, driver).na.fill("")
    .withColumn("VALUE", when(col("IS_CMS").equalTo('Y'), getCMSCred(cms_url, cms_user, cms_pass, cms_aes_iv, cms_aes_key)(col("VALUE"),lit("base64"))).otherwise(col("VALUE")))
    .collect
    stream_dynamic_read_con_array = ReadInput.readInput(spark, "(" + stream_dynamic_read_con + job_instance_id + ") x", url, username, password, driver).na.fill("").collect

    var typeSet: Set[String] = Set()
    workflow_array.foreach { row => typeSet += row.getAs[String]("type").toLowerCase }

    /*
     * The following conditions are necessary to be checked while loading the configuration tables as it is a heavy operation and
     * only the required tables need to be loaded
     */

    if (typeSet.contains("parsing")) {
      println("====> Loading Parsing Configuration")
      json_parsing_array = ReadInput.readInput(spark, "(" + json_parsing_fields_details + job_instance_id + ") x", url, username, password, driver).na.fill("").collect //.cache
      delimiter_parsing_array = ReadInput.readInput(spark, "(" + delimiter_metadata + job_instance_id + ") x", url, username, password, driver).na.fill("").collect
    }

    if (typeSet.contains("field evaluation")) {
      println("====> Loading Field Evaluation Configuration")
      expression_evaluation_array = ReadInput.readInput(spark, "(" + expression_evaluation_details + job_instance_id + ") y", url, username, password, driver).na.fill("").collect //.cache
      expr_eval_dtl_array = ReadInput.readInput(spark, "(" + expr_eval_dtl + job_instance_id + ") y", url, username, password, driver).na.fill("").collect //.cache
      kpi_filter_array = ReadInput.readInput(spark, "(" + json_kpi_filter_master + job_instance_id + ") y", url, username, password, driver).na.fill("").collect //.cache
    }

    if (typeSet.contains("field transformation")) {
      println("====> Loading Field Transformation Configuration")
      field_transformation_array = ReadInput.readInput(spark, "(" + field_transformation_details + job_instance_id + ") y", url, username, password, driver).na.fill("").collect //.cache
      char_replace_array = ReadInput.readInput(spark, "(" + json_field_replacment_details + job_instance_id + ") x", url, username, password, driver).na.fill("").collect //.cache
    }

    if (typeSet.contains("enrichment")) {
      println("====> Loading Enrichment Configuration")
      enrichment_array = ReadInput.readInput(spark, "(" + dyn_enrichment_details + job_instance_id + ") x", url, username, password, driver).na.fill("").na.fill(0).collect //.cache
      var enrichment_read_details_df = ReadInput.readInput(spark, "(" + enrichment_read_details + job_instance_id + ") x", url, username, password, driver).na.fill("")
      enrichment_read_array = enrichment_read_details_df.select(enrichment_read_details_df.columns.map(c => col(c).as(c.toLowerCase)): _*)
      .withColumn("value", when(col("is_cms").equalTo('Y'), getCMSCred(cms_url, cms_user, cms_pass, cms_aes_iv, cms_aes_key)(col("value"),lit("base64"))).otherwise(col("value")))
      .collect //.cache

      //enrich_json_parsing_details
      enrich_json_parsing_array = ReadInput.readInput(spark, "(" + enrich_json_parsing_details + job_instance_id + ") x", url, username, password, driver).na.fill("").collect
    }

    if (typeSet.contains("join")) {
      println("====> Loading Join Configuration")
      join_array = ReadInput.readInput(spark, "(" + join_details + job_instance_id + ") x", url, username, password, driver).na.fill("").collect
    }

    if (typeSet.contains("aggregation")) {
      println("====> Loading Aggregation Configuration")
      aggregation_df = ReadInput.readInput(spark, "(" + dyn_aggregation_details + job_instance_id + ") x", url, username, password, driver) //.filter("aggregation_id between 10 and 130")
      aggregation_df = aggregation_df.withColumn("default_aggregation_id", when(col("default_aggregation_id").isNull, col("aggregation_id")).otherwise(col("default_aggregation_id")))
        .withColumn("insert_timestamp_col_name", when(col("insert_timestamp_col_name").isNull, lit("insert_ts")).otherwise(col("insert_timestamp_col_name")))
        .withColumn("insert_timestamp_format", when(col("insert_timestamp_format").isNull, lit("yyyy-MM-dd HH:mm:ss")).otherwise(col("insert_timestamp_format"))).na.fill("")
      aggregation_df = aggregation_df.select(aggregation_df.columns.map(c => col(c).as(c.toUpperCase)): _*).cache
      agg_master_array = aggregation_df.collect

      agg_filter_df = ReadInput.readInput(spark, "(" + agg_filter + job_instance_id + ") x", url, username, password, driver).na.fill("")
      agg_grouping_df = ReadInput.readInput(spark, "(" + agg_grouping + job_instance_id + ") x", url, username, password, driver).na.fill("")
      agg_function_df = ReadInput.readInput(spark, "(" + agg_function + job_instance_id + ") x", url, username, password, driver).na.fill("")

      agg_filter_df = agg_filter_df.select(agg_filter_df.columns.map(c => col(c).as(c.toLowerCase)): _*).cache

      agg_filter_array = agg_filter_df.select(agg_filter_df.columns.map(c => col(c).as(c.toLowerCase)): _*).collect //.cache
      agg_grouping_array = agg_grouping_df.select(agg_grouping_df.columns.map(c => col(c).as(c.toLowerCase)): _*).collect //.cache
      agg_function_array = agg_function_df.select(agg_function_df.columns.map(c => col(c).as(c.toLowerCase)): _*).collect //.cache

      groupingColumnsArray = agg_grouping_array.map(x => x.getAs[String]("col_name")).toSet.toArray
      aggregationFunctionColumnsArray = agg_function_array.map(x => x.getAs[String]("col_name")).toSet.toArray
    }

    if (typeSet.contains("filter")) {
      println("====> Loading Filter Configuration")
      workflow_filter_condition_array = ReadInput.readInput(spark, "(" + workflow_filter_condition_details + job_instance_id + ") x", url, username, password, driver).na.fill("").collect //.cache
    }
    //control_point_df = ReadInput.readInput(spark, "(" + contro_point_table + ") x", url, username, password, driver).cache

    if (typeSet.contains("coalesce") || typeSet.contains("explode")) {
      println("====> Loading Coalesce or Explode Configuration")
      coalesce_explode_array = ReadInput.readInput(spark, "(" + coalesce_explode_details + job_instance_id + ") x", url, username, password, driver).na.fill("").collect //.cache
    }

    /*
     * API Related Configuration
     */
    val api_df_ = ReadInput.readInput(spark, "(" + api_read_details + job_instance_id + ") x", url, username, password, driver).na.fill("").cache
    api_read_array = api_df_.collect //ReadInput.readInput(spark, "(" + api_read_details + job_instance_id + ") x", url, username, password, driver).na.fill("").collect
    api_header_array = ReadInput.readInput(spark, "(" + api_header_details + job_instance_id + ") x", url, username, password, driver).na.fill("").collect
    api_json_parsing_array = ReadInput.readInput(spark, "(" + api_json_parsing_details + job_instance_id + ") x", url, username, password, driver).na.fill("").collect

    /**
     * Load API Read Operation Related Configuration If we have read from API
     */
    val isEnrichApi: Boolean = enrichment_array.map(x => x.getAs[String]("TABLE_TYPE").toLowerCase()).toList.contains("api")
    val isOpSubtypeApi: Boolean = workflow_array.filter(x => x.getAs[String]("type").equalsIgnoreCase("read") && x.getAs[String]("subtype").equalsIgnoreCase("api")).length > 0 //(0).getAs[String]("subtype").equalsIgnoreCase("api")
    //println(isOpSubtypeApi)

    if (isOpSubtypeApi || isEnrichApi) {
      println("====> Loading API Read Configuration")
      var api_id_list = api_df_.collect.map(x => x.getAs("API_ID").toString.toLong)
      //println("API_ID_LIST = ", api_id_list.toList)
      //.filter("read_enrichment_id=(-1)")
      full_dep_df = ReadInput.readInput(spark, "(" + read_api_dependency_details + ") x", url, username, password, driver).na.fill("").cache

      val full_dep_df_ = full_dep_df.filter("indep_api_id is not null")

      api_id_list.foreach { dep_api_id =>
        val indep_api_id_list = ApiUtils.getApiIds(dep_api_id, full_dep_df_).distinct

        api_id_list = api_id_list ++ indep_api_id_list
      }

      val api_id_list_str = api_id_list.mkString(",")

      //println("final api_id list", api_id_list_str)

      api_df = ReadInput.readInput(spark, s"($read_api_details ($api_id_list_str)) x", url, username, password, driver)
      //api_df.show

      payload_df = ReadInput.readInput(spark, s"($read_payload_details ($api_id_list_str)) x", url, username, password, driver).na.fill("")
      payload_df = payload_df.groupBy("API_ID").
        agg(collect_list("FIELD_NAME").as("FIELD_NAME"), collect_list("FIELDVALUE_COL").as("FIELDVALUE_COL"),
          collect_list(when(col("DATATYPE").isNull, lit("")).otherwise(col("DATATYPE"))).as("DATATYPE"))

      //payload_df.show

      header_df = ReadInput.readInput(spark, s"($read_header_details ($api_id_list_str)) x", url, username, password, driver).na.fill("")
      header_df = header_df.groupBy("API_ID").
        agg(collect_list("HEADER_KEY").as("HEADER_KEY"), collect_list("HEADER_FIXED_VALUE").as("HEADER_FIXED_VALUE"),
          collect_list(when(col("HEADER_ENRICH_VALUE").isNull, lit("")).otherwise(col("HEADER_ENRICH_VALUE"))).as("HEADER_ENRICH_VALUE"))

      //header_df.show

      urlparam_df = ReadInput.readInput(spark, s"($read_url_parameter_details ($api_id_list_str)) x", url, username, password, driver).na.fill("")
      urlparam_df = urlparam_df.groupBy("API_ID").
        agg(collect_list("PARAM_NAME").as("PARAM_NAME"), collect_list("PARAM_VALUE").as("PARAM_VALUE"))

      //urlparam_df.show

      add_fields_df = ReadInput.readInput(spark, s"($read_add_fields_details ($api_id_list_str)) x", url, username, password, driver).na.fill("")
      add_fields_df = add_fields_df.groupBy("API_ID").agg(
        collect_list(when(col("ADD_FIELD").isNull, lit("")).otherwise(col("ADD_FIELD"))).as("ADD_FIELD"),
        collect_list(when(col("IS_FIXED_VALUE").isNull, lit("")).otherwise(col("IS_FIXED_VALUE"))).as("IS_FIXED_VALUE"),
        collect_list(when(col("ADD_FIELD_VALUE").isNull, lit("")).otherwise(col("ADD_FIELD_VALUE"))).as("ADD_FIELD_VALUE"),
        collect_list(when(col("DATE_FORMAT").isNull, lit("")).otherwise(col("DATE_FORMAT"))).as("DATE_FORMAT"),
        collect_list(when(col("DATA_TYPE").isNull, lit("")).otherwise(col("DATA_TYPE"))).as("DATA_TYPE"))

      //add_fields_df.show

      cookie_df = ReadInput.readInput(spark, s"($api_cookie_details ($api_id_list_str)) x", url, username, password, driver).na.fill("")
      cookie_df = cookie_df.groupBy("API_ID").
        agg(collect_list("COOKIE_KEY").as("COOKIE_KEY"), collect_list("COOKIE_INDEX").as("COOKIE_INDEX"))

      //cookie_df.show

      api_additional_att_df = ReadInput.readInput(spark, s"($api_additional_att_details ($api_id_list_str)) x", url, username, password, driver).na.fill("")
      .withColumn("VALUE", when(col("IS_CMS").equalTo('Y'), getCMSCred(cms_url, cms_user, cms_pass, cms_aes_iv, cms_aes_key)(col("VALUE"),lit("base64"))).otherwise(col("VALUE")))
      val addAttExist: Boolean = !api_additional_att_df.isEmpty

      //api_additional_att_df.show

      dep_df = full_dep_df.filter(s"dep_api_id in ($api_id_list_str)") //ReadInput.readInput(spark, "(" + api_dependency_details + job_instance_id + ") x", url, username, password, driver)
      dep_df = dep_df.select(dep_df.columns.map(x => col(x).as(x.toLowerCase())): _*).na.fill("").na.fill(0)

      //dep_df.show

      /*
       * POST DML
       */
      dml_df = ReadInput.readInput(spark, s"($read_dml ($api_id_list_str)) x", url, username, password, driver).na.fill("")

      val dml_id_list = dml_df.select("dml_id").collect().map(x => x(0).toString).mkString(",")

      if (!dml_id_list.equalsIgnoreCase("")) {
        dml_details_df = ReadInput.readInput(spark, s"($read_dml_details ($dml_id_list)) x", url, username, password, driver).na.fill("")
          .groupBy("dml_fk").agg(
          collect_list(when(col("PARAM").isNotNull, col("PARAM")).otherwise(lit(""))).as("PARAM"),
          collect_list(when(col("SUBPARAM").isNotNull, col("SUBPARAM")).otherwise(lit(""))).as("SUBPARAM"),
          collect_list(when(col("PARAMNAME").isNotNull, col("PARAMNAME")).otherwise(lit(""))).as("PARAMNAME"),
          collect_list(when(col("PARAMVALUE").isNotNull, col("PARAMVALUE")).otherwise(lit(""))).as("PARAMVALUE"))

        dml_df = dml_df.join(dml_details_df, col("dml_id") === col("dml_fk"), "left")
          .groupBy("API_ID").agg(collect_list(col("DML_QUERY")).as("DML_QUERY"), collect_list("EXECUTION").as("EXECUTION"), first("DLM_CON_STR_ID").as("DLM_CON_STR_ID"),
          collect_list(col("PARAM")).as("PARAM"),
          collect_list(col("SUBPARAM")).as("SUBPARAM"),
          collect_list(col("PARAMNAME")).as("PARAMNAME"),
          collect_list(col("PARAMVALUE")).as("PARAMVALUE"))
      }

      apiDataset = api_df.join(payload_df, Seq("API_ID"), "left")
        .join(header_df, Seq("API_ID"), "left")
        .join(urlparam_df, Seq("API_ID"), "left")
        .join(add_fields_df, Seq("API_ID"), "left")
        .join(cookie_df, Seq("API_ID"), "left")
        .join(dml_df, Seq("API_ID"), "left")

      var addatt_df = spark.emptyDataFrame
      if (addAttExist) {
        addatt_df = convertKeyValuesToRows(spark, api_additional_att_df, "api_id")
        //addatt_df.show
        //println(addatt_df.columns.toList)
        addatt_df = addatt_df.select(addatt_df.columns.map(x => col(x).as(x.toUpperCase())): _*)
        //addatt_df.show
        apiDataset = apiDataset.join(addatt_df, Seq("API_ID"), "left")
      }

      //apiDataset.show

      api_json_parsing_array = api_json_parsing_array ++ ReadInput.readInput(spark, s"($read_json_parsing_details ($api_id_list_str)) x", url, username, password, driver).na.fill("").collect

    }
    api_df_.unpersist

    /*
     * End of Load API Read Operation Related Configuration
     */

    if (typeSet.contains("pivot")) {
      println("====> Loading Pivot Configuration")
      pivot_array = ReadInput.readInput(spark, "(" + pivot_details + job_instance_id + ") x", url, username, password, driver).na.fill("").collect
    }

    if (typeSet.contains("column rename")) {
      println("====> Loading Column Rename Configuration")
      column_rename_array = ReadInput.readInput(spark, "(" + column_rename_details + job_instance_id + ") x", url, username, password, driver).na.fill("").collect
    }

    if (typeSet.contains("sparksql")) {
      println("====> Loading Spark Sql Exec Configuration")
      spark_sql_exec_array = ReadInput.readInput(spark, "(" + spark_sql_exec_details + job_instance_id + ") x", url, username, password, driver).na.fill("").collect
    }

    if (typeSet.contains("dml execution")) {
      println("====> Loading Dml Execution Configuration")
      dml_exec_df = ReadInput.readInput(spark, "(" + dml_exec + job_instance_id + ") x", url, username, password, driver).na.fill("")
      val dml_id_list = dml_exec_df.select("dml_id").collect().map(x => x(0).toString).mkString(",")
      if (!dml_id_list.equalsIgnoreCase("")) {
        dml_exec_details_df = ReadInput.readInput(spark, s"($dml_exec_details ($dml_id_list)) x", url, username, password, driver).na.fill("")
          .groupBy("dml_fk").agg(
          collect_list(when(col("PARAM").isNotNull, col("PARAM")).otherwise(lit(""))).as("PARAM"),
          collect_list(when(col("SUBPARAM").isNotNull, col("SUBPARAM")).otherwise(lit(""))).as("SUBPARAM"),
          collect_list(when(col("PARAMNAME").isNotNull, col("PARAMNAME")).otherwise(lit(""))).as("PARAMNAME"),
          collect_list(when(col("PARAMVALUE").isNotNull, col("PARAMVALUE")).otherwise(lit(""))).as("PARAMVALUE"))

        dml_exec_df = dml_exec_df.join(dml_exec_details_df, col("dml_id") === col("dml_fk"), "left")
          .groupBy("WORKFLOW_ID").agg(collect_list(col("DML_QUERY")).as("DML_QUERY"), collect_list("EXECUTION").as("EXECUTION"), first("DLM_CON_STR_ID").as("DLM_CON_STR_ID"),
          collect_list(col("PARAM")).as("PARAM"),
          collect_list(col("SUBPARAM")).as("SUBPARAM"),
          collect_list(col("PARAMNAME")).as("PARAMNAME"),
          collect_list(col("PARAMVALUE")).as("PARAMVALUE"))
      }
    }

    if (typeSet.contains("conditional")) {
      println("====> Loading Conditional Workflow Configuration")
      cond_array = ReadInput.readInput(spark, "(" + conditional + job_instance_id + ") x", url, username, password, driver).na.fill("").collect
      val cond_ids = cond_array.map(x => x.getAs("CONDITION_ID").toString.toLong).mkString(",")
      cond_dtls_array = ReadInput.readInput(spark, s"($conditional_details ($cond_ids)) x", url, username, password, driver).na.fill("").collect
    }

    if (typeSet.contains("repartition")) {
      println("====> Loading Repartition Workflow Configuration")
      repartition_array = ReadInput.readInput(spark, "(" + repartition_details + job_instance_id + ") x", url, username, password, driver).na.fill("").collect
    }

    if (typeSet.contains("coalesce_partition")) {
      println("====> Loading Coalesce Partition Workflow Configuration")
      coalesce_array = ReadInput.readInput(spark, "(" + coalesce_details + job_instance_id + ") x", url, username, password, driver).na.fill("").collect
    }

    if (typeSet.contains("dynamicsql")) {
      println("====> Loading Dynamic SQL Configuration")
      dyn_sql_array = ReadInput.readInput(spark, "(" + dyn_sql_details + job_instance_id + ") a", url, username, password, driver).na.fill("").collect()
      val dyn_sql_id_list = dyn_sql_array.map(x => x.getAs("DYN_SQL_ID").toString.toLong).mkString(",")
      dyn_sql_dist_col_array = ReadInput.readInput(spark, s"($dyn_sql_dist_col_details ($dyn_sql_id_list)) a", url, username, password, driver).na.fill("").collect()
      dyn_sql_op_info_array = ReadInput.readInput(spark, s"($dyn_sql_op_info_details ($dyn_sql_id_list)) a", url, username, password, driver).na.fill("").collect()
      dyn_sql_add_attr_array = ReadInput.readInput(spark, s"($dyn_sql_add_attr_details ($dyn_sql_id_list)) a", url, username, password, driver).na.fill("")
      .withColumn("VALUE", when(col("IS_CMS").equalTo('Y'), getCMSCred(cms_url, cms_user, cms_pass, cms_aes_iv, cms_aes_key)(col("VALUE"),lit("base64"))).otherwise(col("VALUE")))
      .collect()
      dyn_sql_json_parsing_array = ReadInput.readInput(spark, "(" + dyn_sql_json_parsing_details + job_instance_id + ") a", url, username, password, driver).na.fill("").collect()
    }

    var variable_df = ReadInput.readInput(spark, "(" + variable_details + job_instance_id + ") y", url, username, password, driver).na.fill("").na.fill(-1)
    variable_df = variable_df.groupBy("WORKFLOW_ID", "SPARK_SQL_QUERY", "SCALA_CODE", "CMD_LINE_ARG", "SEQUENCE").
      agg(collect_list("VARIABLE_NAME").as("VARIABLE_NAME"),
        collect_list("COLUMN_NAME").as("COLUMN_NAME"),
        collect_list("DEFAULT_VALUE").as("DEFAULT_VALUE"))
    variable_array = variable_df.collect

    plugins.Variables.set_variable_default_values

    if (typeSet.contains("loop")) {
      println("====> Loading Loop Configuration")
      loop_array = ReadInput.readInput(spark, "(" + loop_details + job_instance_id + ") a", url, username, password, driver).na.fill("").na.fill(0).collect()
      loop_dtls_array = ReadInput.readInput(spark, "(" + loop_col_details + job_instance_id + ") a", url, username, password, driver).na.fill("").na.fill(0).collect()
    }
    if (typeSet.contains("file listing")) {
      println("====> Loading File Listing Configuration")
      file_listing_array = ReadInput.readInput(spark, "(" + file_listing_details + job_instance_id + ") a", url, username, password, driver).na.fill("").na.fill(0).collect()
      val file_listing_id_list = file_listing_array.map(x => x.getAs("FILELISTING_ID").toString.toLong).mkString(",")
      file_listing_add_attr_array = ReadInput.readInput(spark, s"($file_listing_add_attr_details ($file_listing_id_list)) a", url, username, password, driver).na.fill("")
        .withColumn("VALUE", when(col("IS_CMS").equalTo('Y'), getCMSCred(cms_url, cms_user, cms_pass, cms_aes_iv, cms_aes_key)(col("VALUE"), lit("base64"))).otherwise(col("VALUE")))
        .collect()
    }
    if (typeSet.contains("schema validation")) {
      println("====> Loading Schema Validation Configuration")
      schema_validation_array = ReadInput.readInput(spark, "(" + schema_validation_details + job_instance_id + ") a", url, username, password, driver).na.fill("").na.fill(0).collect()
      val schema_validation_id_list = schema_validation_array.map(x=>x.getAs("SCHEMA_VALIDATION_ID").toString.toLong).mkString(",")
      schema_validation_add_attr_array = ReadInput.readInput(spark, s"($schema_validation_add_attr_details ($schema_validation_id_list)) a", url, username, password, driver).na.fill("")
        .withColumn("VALUE", when(col("IS_CMS").equalTo('Y'), getCMSCred(cms_url, cms_user, cms_pass, cms_aes_iv, cms_aes_key)(col("VALUE"), lit("base64"))).otherwise(col("VALUE")))
        .collect()
    }
    if (typeSet.contains("file migration")) {
      println("====> Loading File Migration Configuration")
      file_migration_array = ReadInput.readInput(spark, "(" + file_migration_details + job_instance_id + ") a", url, username, password, driver).na.fill("").na.fill(0).collect()
      val file_migration_id_list = file_migration_array.map(x=>x.getAs("FILE_MIGRATION_ID").toString.toLong).mkString(",")
      file_migration_add_attr_array = ReadInput.readInput(spark,s"($file_migration_add_attr_details ($file_migration_id_list)) a",url, username, password, driver).na.fill("")
        .withColumn("VALUE", when(col("IS_CMS").equalTo('Y'), getCMSCred(cms_url, cms_user, cms_pass, cms_aes_iv, cms_aes_key)(col("VALUE"), lit("base64"))).otherwise(col("VALUE")))
        .collect()
    }
    if (typeSet.contains("filerename")) {
      println("====> Loading FileRename Configuration")
      file_rename_array = ReadInput.readInput(spark, "(" + file_rename_workflow + job_instance_id + ") a", url, username, password, driver)
        .na.fill("").na.fill(0)
        .withColumn("CONNECTION_STRING_ID",col("CONNECTION_STRING_ID").cast("long"))
        .withColumn("PLS_FILERENAME_ID",col("PLS_FILERENAME_ID").cast("long"))
        .withColumn("WORKFLOW_ID",col("WORKFLOW_ID").cast("long"))
        .collect()

      val rename_ids = file_rename_array.map(x => x.getAs("PLS_FILERENAME_ID").toString.toLong).mkString(",")
      file_rename_Detail_array = ReadInput.readInput(spark, "(" + file_rename_detail + job_instance_id + ") a", url, username, password, driver).na.fill("").na.fill(0).collect()
      file_rename_add_attr_array = ReadInput.readInput(spark, s"($file_rename_add_attr_details ($rename_ids) ) a", url, username, password, driver).na.fill("").na.fill(0)
        .withColumn("PLS_GP_FILERENAME_ID",col("PLS_GP_FILERENAME_ID").cast("long"))
        .withColumn("VALUE", when(col("IS_CMS").equalTo('Y'), getCMSCred(cms_url, cms_user, cms_pass, cms_aes_iv, cms_aes_key)(col("VALUE"), lit("base64"))).otherwise(col("VALUE")))
        .collect()
    }
    println("===> Loaded Confituration")
  }

  loadConfituration

  /**
   * <p>The function setKafkaParameters sets the WINDOW LENGTH and the SLIDING INTERVAL for Kafka streaming.
   * <br>There are three types of WINDOW LENGTH and SLIDING INTERVAL configuration:
   * <br>1. Aggregation Level
   * <br>2. Job level filter condition
   * <br>3. Read Level
   * <p>The first preference is given to the Aggregation Level followed by Job level filter condition and Read Level.
   * Window length is max of all the given windows and Sliding interval is min of all the given intervals.
   * These values must be given in MilliSeconds
   *
   * @author praneet.pulivendula
   */

  def setKafkaParameters = {
    var max_window = -1
    var min_streaming_interval = -1
    if (agg_master_array.filter(x => !x.getAs[String]("DELTA").equals("") && !x.getAs[String]("STREAMING_INTERVAL").equals("")).length > 0) {
      Try {
        max_window = agg_master_array.filter(x => !x.getAs[String]("DELTA").equals("") && !x.getAs[String]("STREAMING_INTERVAL").equals("")).map(x => x.getAs[String]("DELTA").toInt + x.getAs[String]("STREAMING_INTERVAL").toInt).toList.max
        min_streaming_interval = agg_master_array.filter(x => !x.getAs[String]("DELTA").equals("") && !x.getAs[String]("STREAMING_INTERVAL").equals("")).map(x => x.getAs[String]("STREAMING_INTERVAL").toInt).toList.min
        isTimeBased = true
      }
    } else if (workflow_filter_condition_array.filter(x => !x.getAs[String]("DELTA").equals("") && !x.getAs[String]("STREAMING_INTERVAL").equals("")).length > 0) {
      Try {
        max_window = workflow_filter_condition_array.filter(x => !x.getAs[String]("DELTA").equals("") && !x.getAs[String]("STREAMING_INTERVAL").equals("")).map(x => x.getAs[String]("DELTA").toInt + x.getAs[String]("STREAMING_INTERVAL").toInt).toList.max
        min_streaming_interval = workflow_filter_condition_array.filter(x => !x.getAs[String]("DELTA").equals("") && !x.getAs[String]("STREAMING_INTERVAL").equals("")).map(x => x.getAs[String]("STREAMING_INTERVAL").toInt).toList.min
        isTimeBased = true
      }
    }

    if (max_window != (-1) && min_streaming_interval != (-1)) {
      window_length = max_window * 1000
      streaming_interval = min_streaming_interval
      slide_interval = streaming_interval * 1000
      if (window_length % streaming_interval == 0) run_job = true
    }
  }

  def matrix_count(matrix: Array[Array[Long]], number: Long): Long = {
    var count = 0
    matrix.foreach { i =>
      i.foreach { j =>
        if (j == number) {
          count += 1
        }
      }
    }
    count
  }

  def get_matrix_distinct(matrix: Array[Array[Long]]): Array[Long] = {
    var temp: Array[Long] = Array()
    matrix.foreach { i =>
      i.foreach { j =>
        if (!temp.contains(j)) {
          temp :+= j
        }
      }
    }
    temp
  }

  case class filter_conf(aggregation_filter_id: Long, aggregation_id: Long, column_name: String, value: String, operator: String, datatype: String)

  case class filter_conf_(aggregation_filter_id: Long, aggregation_id: Long, column_name: String, value: String, operator: String, datatype: String, agg_ids: Array[Long])

  val array_filter = udf((mainstring: String) => {
    mainstring.split(",").filter(x => !x.equals(""))
  })

  /**
   * buildSpecialFilterCondition function builds a special filter condition given some configuration constraints.
   * The constraints are as follows:
   * <br>1. The logical operator has to be 'AND'
   * <br>2. The relational operator has to be '='
   * <br>Based on the above constraints, the function creates an efficient filter condition (to improve the performance).
   */

  var filterArray = agg_filter_array
    .filter(x => x.getAs[String]("relational_operator").equals("=") &&
      (x.getAs[String]("logical_operator").equalsIgnoreCase("AND") || x.getAs[String]("logical_operator").equals("")))
    .map(x => filter_conf(
      x.getAs("aggregation_filter_id").toString.toLong,
      x.getAs("aggregation_id").toString.toLong, x.getAs[String]("column_name"),
      x.getAs[String]("value"), x.getAs[String]("relational_operator"), x.getAs[String]("col_datatype")))

  def buildSpecialFilterCondition(inp_agg_ids: Array[Long]): (Array[Long], String) = {
    var distinct_agg_id_cnt = filterArray.map(_.aggregation_id).groupBy(x => x).map(x => x._2.length).toSet
    var filter_expression1 = ""
    var agg_id_cnt_map = filterArray.map(_.aggregation_id).groupBy(x => x).map(x => x._1 -> x._2.length)
    var remaining_inp_agg_ids = inp_agg_ids
    //var remaining_array = filterArray
    distinct_agg_id_cnt.foreach { agg_id_cnt =>
      var remaining_array = filterArray.filter(x => remaining_inp_agg_ids.contains(x.aggregation_id))
      var tempfilterArray = remaining_array.filter(x => agg_id_cnt_map.filter(_._2 == agg_id_cnt).map(_._1).toArray.contains(x.aggregation_id))
      var temp = tempfilterArray.groupBy(x => x.column_name).map(x => x._1 -> x._2.map(x => x.aggregation_id))
      var agg_arr = tempfilterArray.map(x => (x.aggregation_filter_id, x.aggregation_id, x.column_name, x.value, x.operator, x.datatype, temp(x.column_name)))
      var distinct_agg_id = agg_arr.map(x => x._2).toSet.toArray
      var matrix: Array[Array[Long]] = Array()
      var temp_agg_arr: Array[filter_conf] = Array()
      distinct_agg_id.foreach { agg_id =>
        var temp_agg_arr1 = agg_arr.filter(x => x._2 == agg_id)
        var counter = 0
        var index = 0
        temp_agg_arr1.foreach { agg =>
          matrix = matrix :+ agg._7.filter(x => x != agg_id)
        }
        var flag = false
        matrix(0).foreach { id =>
          if (matrix_count(matrix, id.toLong) == agg_id_cnt) {
            flag = true
          }
        }
        if (flag) {
          temp_agg_arr = temp_agg_arr ++ temp_agg_arr1.map(x => filter_conf(x._1, x._2, x._3, x._4, x._5, x._6))
        }
        matrix = matrix.drop(matrix.length)
      }
      tempfilterArray = temp_agg_arr
      temp = tempfilterArray.groupBy(x => x.column_name + x.operator + x.value).map(x => x._1 -> x._2.map(x => x.aggregation_id))
      agg_arr = tempfilterArray.map(x => (x.aggregation_filter_id, x.aggregation_id, x.column_name, x.value, x.operator, x.datatype, temp(x.column_name + x.operator + x.value)))
      distinct_agg_id = agg_arr.map(x => x._2).toSet.toArray
      matrix = matrix.drop(matrix.length)
      var temp_agg_arr_2: Array[filter_conf_] = Array()
      distinct_agg_id.foreach { agg_id =>
        var temp_agg_arr1 = agg_arr.filter(x => x._2 == agg_id)
        var counter = 0
        var index = 0
        temp_agg_arr1.foreach { agg =>
          matrix = matrix :+ agg._7
        }
        var flag = false
        get_matrix_distinct(matrix).foreach { id =>
          if (matrix_count(matrix, id.toInt) >= agg_id_cnt - 1) {
            flag = true
          }
        }
        if (flag) {
          temp_agg_arr_2 = temp_agg_arr_2 ++ temp_agg_arr1.map(x => filter_conf_(x._1, x._2, x._3, x._4, x._5, x._6, x._7))
        }
        matrix = matrix.drop(matrix.length)
      }
      //println(temp_agg_arr_2.mkString(", "))
      var distinct_arrays = temp_agg_arr_2.map(x => x.agg_ids.sortWith(_ < _).mkString(",")).toSet.toArray.map(_.split(",")).filter(_.size > 1).sortWith(_.size < _.size)
      var temp_agg_arr_ = temp_agg_arr_2
      distinct_arrays.foreach { inaggids =>
        if (temp_agg_arr_.size > 0) {
          var inaggids_ = inaggids.map(x => x.toInt)
          var final_arr = temp_agg_arr_.filter(x => inaggids_.contains(x.aggregation_id)).groupBy(x => (x.column_name, x.operator, x.datatype)).map(x => x._1 -> x._2.map(y => y.value).toSet.toArray)
          var filter_expression = ""
          final_arr.foreach { row =>
            var column_name = row._1._1
            var relational_operator = row._1._2
            var column_data_type = row._1._3
            var values_arr = if (column_data_type.equalsIgnoreCase("string")) row._2.map(x => "'" + x + "'") else row._2

            if (values_arr.size > 1) {
              filter_expression += column_name + " in (" + values_arr.mkString(",") + ") and "
            } else {
              filter_expression += column_name + relational_operator + values_arr(0) + " and "
            }
          }
          if (!filter_expression.equals("")) {
            filter_expression = filter_expression.substring(0, filter_expression.length() - 4)
            filter_expression1 = filter_expression1 + "(" + filter_expression + ")" + " OR "
          }
          temp_agg_arr_ = temp_agg_arr_.filter(x => !inaggids_.contains(x.aggregation_id))
        }
      }
      if (distinct_arrays.size > 0) {
        //var temp_agg_arr_3 = temp_agg_arr_2.map(x => filter_conf(x.aggregation_filter_id,x.aggregation_id,x.column_name,x.value,x.operator,x.datatype))
        remaining_inp_agg_ids = remaining_inp_agg_ids.filterNot(temp_agg_arr_2.map(x => x.aggregation_id).contains(_))
        //remaining_array = remaining_array.filterNot(temp_agg_arr_3.contains(_))
      }
    }
    if (!filter_expression1.equals("")) {
      filter_expression1 = "(" + filter_expression1.substring(0, filter_expression1.length() - 3) + ")"
    }
    //println("===> Final filter expression: " + filter_expression1.substring(0, filter_expression1.length() - 4))
    //println("===> Final remaining agg ids: " + remaining_inp_agg_ids.mkString(", "))
    (remaining_inp_agg_ids, filter_expression1)
  }

  /**
   * aggregationQueryMap is a Global Map which stores the pre-calculated Aggregation query (if aggregation operation exists in the work flow).
   * The key is the combination of (aggregation_id, Sequence) and the value is a Map with key being priority and value being the SQL Query.
   * It gets refreshed as and when the configuration tables are refreshed.
   */

  var aggregationQueryMap: scala.collection.mutable.Map[(Long, Int), scala.collection.mutable.Map[Int, String]] = scala.collection.mutable.Map()

  /**
   * buildAggregationSqlQuery function calculates the aggregation query (if aggregation operation exists in the work flow) for all the
   * aggregations which are not based on window partitioning (is_window='N').
   */

  def buildAggregationSqlQuery = {
    println("buildAggregationSqlQuery")
    var nonWindowAggWfids = agg_master_array.filter(x => x.getAs[String]("IS_WINDOW").equalsIgnoreCase("N")).map(x => (x.getAs("WORKFLOW_ID").toString.toLong, x.getAs("SEQUENCE").toString.toInt)).toSet.toArray
    println(nonWindowAggWfids.mkString(","))

    nonWindowAggWfids.foreach { nonWindowAggWfid =>

      var agg_mstr_array = agg_master_array.filter(y => y.getAs("WORKFLOW_ID").toString.toLong == nonWindowAggWfid._1 && y.getAs("SEQUENCE").toString.toInt == nonWindowAggWfid._2)
      var agg_fltr_array = agg_filter_array.filter(y => y.getAs("workflow_id").toString.toLong == nonWindowAggWfid._1 && y.getAs("seq").toString.toInt == nonWindowAggWfid._2)
      var agg_grp_array = agg_grouping_array.filter(y => y.getAs("workflow_id").toString.toLong == nonWindowAggWfid._1 && y.getAs("seq").toString.toInt == nonWindowAggWfid._2)
      var agg_func_array = agg_function_array.filter(y => y.getAs("workflow_id").toString.toLong == nonWindowAggWfid._1 && y.getAs("seq").toString.toInt == nonWindowAggWfid._2)

      def return_filter_condition(aggregation_id: Long): String = {
        var filter_condition = ""
        var counter = 0
        var rows = agg_fltr_array.filter(x => x.getAs("aggregation_id").toString.toLong == aggregation_id)
        scala.util.Sorting.stableSort(rows, (e1: Row, e2: Row) => e1.getAs("sequence").toString.toInt < e2.getAs("sequence").toString.toInt)
        rows.foreach { row =>
          var logical_op = if (counter > 0) " " + row.getAs[String]("logical_operator") + " " else ""
          var value = if (row.getAs[String]("col_datatype").equalsIgnoreCase("string")) "'" + row.getAs[String]("value") + "'" else row.getAs[String]("value")
          if (!row.getAs[String]("higher_value").equals("")) value = value + " AND " + row.getAs[String]("higher_value")
          filter_condition = filter_condition + logical_op + row.getAs[String]("column_name") + row.getAs[String]("relational_operator") + value
          filter_condition = if (counter > 0) "(" + filter_condition + ")" else filter_condition
          counter += 1
        }
        filter_condition
      }

      def return_grouping_condition(grouping_col: String, expression: String, alias: String): String = {
        var aggids = agg_grp_array.filter(x =>
          x.getAs[String]("col_name").equals(grouping_col) &&
            x.getAs[String]("expression").equals(expression) &&
            x.getAs[String]("alias_name").equals(alias))
          //.map(x => "'" + x.getAs("aggregation_id").toString.toInt + "'")
          .map(x => x.getAs("aggregation_id").toString.toLong)
          .toSet
          //.mkString(",")
          .toArray
        var filter_output = buildSpecialFilterCondition(aggids) //buildSpecialFilterCondition(agg_filter_df.filter(s"aggregation_id in ($aggids)"))
        var groupstr = ""
        if (!filter_output._2.equals("")) {
          groupstr = filter_output._2 + " OR "
        }
        var leftover_aggids = filter_output._1
        if (leftover_aggids.length > 0) {
          //var leftover_aggids = arr.map(x => x.getAs("aggregation_id").toString.toInt).toSet.toArray
          agg_grp_array.filter(x =>
            leftover_aggids.contains(x.getAs("aggregation_id").toString.toInt) &&
              x.getAs[String]("col_name").equals(grouping_col) && x.getAs[String]("expression").equals(expression) &&
              x.getAs[String]("alias_name").equals(alias)).foreach { row =>
            var filter_condition = return_filter_condition(row.getAs("aggregation_id").toString.toLong)
            if (!filter_condition.equals("")) {
              groupstr = groupstr + filter_condition + " OR "
            }
          }
        }
        var grouping_col_or_expression = if (!expression.equals("")) expression else grouping_col
        if (!groupstr.equals("")) {
          groupstr = "CASE WHEN " + groupstr.substring(0, groupstr.length - 4) + " THEN " + grouping_col_or_expression + " END"
        } else {
          groupstr = grouping_col_or_expression
        }
        groupstr
      }

      def return_aggregation_condition(agg_function: String, alias: String, optional_arg_1: String, optional_arg_2: String): String = {
        var aggstr = ""
        var counter = 0
        var leftover_aggids: Array[Long] = Array()
        var agg_func_array_filtered = agg_func_array.filter(x =>
          x.getAs[String]("agg_function").equals(agg_function) &&
            x.getAs[String]("alias_name").equals(alias) &&
            x.getAs[String]("optional_arg_1").equals(optional_arg_1) &&
            x.getAs[String]("optional_arg_2").equals(optional_arg_2))

        var distinct_columns = agg_func_array_filtered.map(x => (x.getAs[String]("col_name"), x.getAs[String]("expression"))).toSet.toArray

        distinct_columns.foreach { column =>
          var col_name_or_expression = if (column._1.equals("")) column._2 else column._1
          if (col_name_or_expression.equals("*")) col_name_or_expression = "unique_col"
          var col_datatype = agg_func_array_filtered.filter(x =>
            x.getAs[String]("col_name").equals(column._1) &&
              x.getAs[String]("expression").equals(column._2))
            .map(x => x.getAs[String]("col_datatype"))
            .head
          col_datatype = if (col_datatype.equals("")) "string" else col_datatype
          var aggids = agg_func_array_filtered.filter(x =>
            x.getAs[String]("col_name").equals(column._1) &&
              x.getAs[String]("expression").equals(column._2))
            .map(x => x.getAs("aggregation_id").toString.toLong)
            .toSet
            .toArray
          //.mkString(",")
          if (aggids.size == 0) {
            leftover_aggids = leftover_aggids ++ aggids
          } else {
            var filter_output = buildSpecialFilterCondition(aggids)
            if (!filter_output._2.equals("")) {
              aggstr = "CASE WHEN " + filter_output._2 + " THEN cast(" + col_name_or_expression + " as " + col_datatype + ") ELSE "
              counter += 1
            }
            var arr = filter_output._1
            if (arr.size > 0) {
              //var a = arr.map(x => x.getAs("aggregation_id").toString.toInt).toSet.toArray
              leftover_aggids = leftover_aggids ++ arr
            }
          }
        }
        if (leftover_aggids.length > 0) {
          agg_func_array_filtered.filter(x => leftover_aggids.contains(x.getAs("aggregation_id").toString.toLong)).foreach { row =>
            var filter_condition = return_filter_condition(row.getAs("aggregation_id").toString.toLong)
            var col_name_or_expression = if (row.getAs[String]("col_name").equals("")) row.getAs[String]("expression") else row.getAs[String]("col_name")
            if (col_name_or_expression.equals("*")) col_name_or_expression = "unique_col"
            var col_datatype = if (row.getAs[String]("col_datatype").equals("")) "string" else row.getAs[String]("col_datatype")
            if (!filter_condition.equals("")) {
              aggstr = aggstr + "CASE WHEN " + filter_condition + " THEN cast(" + col_name_or_expression + " as " + col_datatype + ") ELSE "
              counter = counter + 1
            } else {
              aggstr = aggstr + "cast(" + col_name_or_expression + " as " + col_datatype + ")      "
            }
          }
        }

        if (!aggstr.equals("")) {
          aggstr = aggstr.substring(0, aggstr.length - 5)
          (1 to counter).foreach { i => aggstr = aggstr + " END" }
        }

        var optional_args =
          if (!optional_arg_1.equals("") && !optional_arg_2.equals(""))
            "," + optional_arg_1 + "," + optional_arg_2
          else if (!optional_arg_1.equals(""))
            "," + optional_arg_1
          else ""

        agg_function + "( " + aggstr + optional_args + " ) AS " + alias
      }

      var isTimebasedAgg = false
      var flag = true

      agg_mstr_array.filter(x => x.getAs("PRIORITY").toString.toInt == 1).foreach { row =>
        var streaming_interval = row.getAs[String]("STREAMING_INTERVAL")
        var delta = row.getAs[String]("DELTA")
        if (!delta.equals("") && !streaming_interval.equals("") && flag) {
          isTimebasedAgg = true
        } else {
          isTimebasedAgg = false
          flag = false
        }
      }

      var distinct_priority_arr = agg_mstr_array.map(x => x.getAs("PRIORITY").toString.toInt).toSet.toArray
      var priorityViseSqlQuery: scala.collection.mutable.Map[Int, String] = scala.collection.mutable.Map()
      if (distinct_priority_arr.length > 1) {
        distinct_priority_arr.foreach { priority =>
          var grpcolarr = agg_grp_array.filter(x => x.getAs("priority").toString.toInt == priority).map(x => (x.getAs[String]("col_name"), x.getAs[String]("expression"), x.getAs[String]("alias_name"))).toSet
          var aggcolarr = agg_func_array.filter(x => x.getAs("priority").toString.toInt == priority).map(x => (x.getAs[String]("agg_function"), x.getAs[String]("alias_name"), x.getAs[String]("optional_arg_1"), x.getAs[String]("optional_arg_2"))).toSet
          var casestr = ""
          var groupstr = ""
          var aggstr = ""

          grpcolarr.foreach { column =>
            var tempstr = return_grouping_condition(column._1, column._2, column._3)
            groupstr = groupstr + tempstr + ","
            casestr = casestr + tempstr + " AS " + column._3 + ","
          }

          aggcolarr.foreach { x =>
            aggstr = aggstr + return_aggregation_condition(x._1, x._2, x._3, x._4) + ","
          }

          if (isTimebasedAgg) {
            casestr = casestr + "ts_agg"
            groupstr = groupstr + "ts_agg"
          } else {
            casestr = casestr.substring(0, casestr.length - 1)
            groupstr = groupstr.substring(0, groupstr.length - 1)
          }
          aggstr = aggstr.substring(0, aggstr.length - 1)

          var sqlstr = "SELECT " + casestr + "," + aggstr + " FROM InputDF GROUP BY " + groupstr

          priorityViseSqlQuery ++= Map(priority -> sqlstr)
        }
      } else {
        var grpcolarr = agg_grp_array.map(x => (x.getAs[String]("col_name"), x.getAs[String]("expression"), x.getAs[String]("alias_name"))).toSet
        var aggcolarr = agg_func_array.map(x => (x.getAs[String]("agg_function"), x.getAs[String]("alias_name"), x.getAs[String]("optional_arg_1"), x.getAs[String]("optional_arg_2"))).toSet
        var casestr = ""
        var groupstr = ""
        var aggstr = ""

        grpcolarr.foreach { column =>
          var tempstr = return_grouping_condition(column._1, column._2, column._3)
          groupstr = groupstr + tempstr + ","
          casestr = casestr + tempstr + " AS " + column._3 + ","
        }

        aggcolarr.foreach { x =>
          aggstr = aggstr + return_aggregation_condition(x._1, x._2, x._3, x._4) + ","
        }

        if (isTimebasedAgg) {
          casestr = casestr + "ts_agg"
          groupstr = groupstr + "ts_agg"
        } else {
          casestr = casestr.substring(0, casestr.length - 1)
          groupstr = groupstr.substring(0, groupstr.length - 1)
        }
        aggstr = aggstr.substring(0, aggstr.length - 1)

        var sqlstr = "SELECT " + casestr + "," + aggstr + " FROM InputDF GROUP BY " + groupstr

        priorityViseSqlQuery ++= Map(-1 -> sqlstr)
      }

      aggregationQueryMap(nonWindowAggWfid) = priorityViseSqlQuery
    }
  }

  buildAggregationSqlQuery

  //println(aggregationQueryMap)

  def operation_debug(op_type: String, workflow_id: Long, df: DataFrame, inputDF: DataFrame = spark.emptyDataFrame) = {
    var key = s"debug.$op_type.filter.condition"
    var filter_condition = if (configuration.hasPath(key)) configuration.getString(key) else "true"
    println(s"===> Debugging operation $op_type with the workflow_id = $workflow_id:")
    if (!inputDF.schema.isEmpty) {
      println("====> Printing input dataframe:")
      inputDF.show()
    }
    println("====> Printing output dataframe:")
    df.filter(filter_condition).show()
  }

  /**
   * readEnrichmentTables function reads enrichment tables from the given sources in the configuration and creates or replaces a temporary view
   * for it. The table is read only if the status is active. Caching of the table is done if it is enabled.
   *
   * @author praneet.pulivendula
   * @param enrichment_id        Reference to the master configuration table PULSE_DYNAMIC_ENRICHMENT
   * @param read_enrichment_id   Reference to the master configuration table PULSE_CLIENT_READ_ENRICHMENT
   * @param connection_string_id Reference to the master configuration table PULSE_CLIENT_CONNECTION_STRING_MASTER
   * @param table_type           Source of the table (oracle, mysql, hdfs, hive...)
   * @param status               active or inactive
   * @param cache                Boolean value to be cached or no
   */

  def readEnrichmentTables(enrichment_id: Long, read_enrichment_id: Long, connection_string_id: Long, table_type: String, status: String, cache: Boolean): (DataFrame, Boolean) = {
    var enrichtable = spark.emptyDataFrame
    var isSuccess = false
    if (table_type.equalsIgnoreCase("api")) {
      var ConnectionDetails: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map()
      var api_read = api_read_array.filter(x => x.getAs("READ_ENRICHMENT_ID").toString.toLong == read_enrichment_id)(0)
      var api_id = api_read.getAs("API_ID").toString.toInt
      ConnectionDetails = ConnectionDetails ++ Map("method" -> api_read.getAs[String]("METHOD")) ++ Map("payload" -> api_read.getAs[String]("PAYLOAD")) ++ Map("url" -> api_read.getAs[String]("URL")) ++ Map("username" -> api_read.getAs[String]("AUTH_USERNAME")) ++ Map("password" -> api_read.getAs[String]("AUTH_PASSWORD")) ++ Map("api_id" -> api_id.toString)
      var header_map: Map[String, String] = Map()
      api_header_array.filter(x => x.getAs("API_ID").toString.toInt == api_id).foreach { row =>
        var key = row.getAs[String]("HEADER_KEY")
        var value = row.getAs[String]("HEADER_FIXED_VALUE")
        header_map = header_map ++ Map(key -> value)
      }
      ConnectionDetails = ConnectionDetails ++ Map("header" -> header_map)
      var read_output = read(table_type, ConnectionDetails, configuration.getBoolean("debug.show.enrichment.tables"))
      enrichtable = read_output._1
      isSuccess = read_output._2
    } else {
      var ConnectionDetails: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
      var ReadDetails: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
      connection_array.filter(x => x.getAs("connection_string_id").toString.toLong == connection_string_id).foreach(row => ConnectionDetails = ConnectionDetails ++ Map(row.getAs[String]("key") -> row.getAs[String]("value")))
      enrichment_read_array.filter(x => x.getAs("read_enrichment_id").toString.toLong == read_enrichment_id).foreach(row => ReadDetails = ReadDetails ++ Map(row.getAs[String]("key") -> row.getAs[String]("value")))
      var read_output = read(table_type, (ConnectionDetails ++ ReadDetails), configuration.getBoolean("debug.show.enrichment.tables"))
      enrichtable = read_output._1
      isSuccess = read_output._2
    }

    if (!enrich_json_parsing_array.filter(x => x.getAs("READ_ENRICHMENT_ID").toString.toLong == read_enrichment_id).isEmpty) {
      try {
        enrichtable = genericJsonParsing(-1, enrichtable, -1, read_enrichment_id)
        isSuccess = true
      } catch {
        case ex: Exception => {
          isSuccess = false
          println("====> Error while parsing Enrichment table with read_enrichment_id: " + read_enrichment_id)
          println(ex.getMessage)
          println("====> Stack trace")
          println(ex.getStackTrace.mkString("\n"))
        }
      }
      if (configuration.getBoolean("debug.show.enrichment.tables")) {
        try {
          println("====> Printing Enrichment table with read_enrichment_id: " + read_enrichment_id)
          enrichtable.show
        } catch {
          case ex: Exception => {
            isSuccess = false
            println("====> Error while parsing Enrichment table with read_enrichment_id: " + read_enrichment_id)
            println(ex.getMessage)
            println("====> Stack trace")
            println(ex.getStackTrace.mkString("\n"))
          }
        }
      }
    }

    (enrichtable, isSuccess)
  }

  def getFileUpdateTime(file_path: String): Long = {
    val path = new Path(file_path)
    val fs = FileSystem.get(path.toUri, sc.hadoopConfiguration)
    val status = fs.getFileStatus(path)
    status.getModificationTime
  }

  /**
   * cacheMap is a Global Map which stores the caching information of each table if the caching is enabled. keys are the table names and
   * the values are the time at which that particular table was cached. Depending on the cache frequency configured, the function cacheOrRefreshTables
   * re-reads or refreshes the table and caches it again.
   */

  var cacheMap: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
  val cacheDataframeMap: scala.collection.mutable.Map[String, DataFrame] = scala.collection.mutable.Map()

  def cacheOrRefreshTables(rows: Array[Row]) = {
    rows.foreach { row =>
      var enrichment_id = row.getAs("ENRICHMENT_ID").toString.toLong
      var cache = row.getAs[String]("CACHE")
      var cache_frequency = row.getAs("CACHE_FREQUENCY").toString.toLong
      var read_enrichment_id = row.getAs("READ_ENRICHMENT_ID").toString.toLong
      var connection_string_id = row.getAs("CONNECTION_STRING_ID").toString.toLong
      var table_type = row.getAs[String]("TABLE_TYPE")
      var status = row.getAs[String]("STATUS")
      if (!cacheMap.contains("table_name_" + enrichment_id)) {
        if (cache.equalsIgnoreCase("y")) {
          val result = readEnrichmentTables(enrichment_id, read_enrichment_id, connection_string_id, table_type, status, true)
          if (result._2) {
            var current_time = System.currentTimeMillis / 60000
            cacheMap = cacheMap ++ Map("table_name_" + enrichment_id -> current_time.toString)
            cacheDataframeMap += ("table_name_" + enrichment_id -> result._1)
            cacheDataframeMap("table_name_" + enrichment_id).cache
            cacheDataframeMap("table_name_" + enrichment_id).createOrReplaceTempView("table_enrichmentid_" + enrichment_id)
          }
        } else {
          val result = readEnrichmentTables(enrichment_id, read_enrichment_id, connection_string_id, table_type, status, false)
          cacheMap = cacheMap ++ Map("table_name_" + enrichment_id -> "")
          result._1.createOrReplaceTempView("table_enrichmentid_" + enrichment_id)
        }
      } else {
        if (cache.equalsIgnoreCase("y")) {
          var current_time = System.currentTimeMillis / 60000
          if (table_type.equalsIgnoreCase("hdfs") && cache_frequency == 0L) {
            var ConnectionDetails: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
            var ReadDetails: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
            connection_array.filter(x => x.getAs("connection_string_id").toString.toLong == connection_string_id).foreach(row => ConnectionDetails = ConnectionDetails ++ Map(row.getAs[String]("key") -> row.getAs[String]("value")))
            enrichment_read_array.filter(x => x.getAs("read_enrichment_id").toString.toLong == read_enrichment_id).foreach(row => ReadDetails = ReadDetails ++ Map(row.getAs[String]("key") -> row.getAs[String]("value")))
            val attr_map = ConnectionDetails ++ ReadDetails
            val file_path = attr_map.getOrElse("url", "") + attr_map.getOrElse("path", "")
            println("Enrichment file path: " + file_path)
            val update_time = getFileUpdateTime(file_path)
            println("File updated time: " + update_time)
            if (streaming_interval != (-1)) {
              if (update_time > (System.currentTimeMillis() - (streaming_interval * 1000).toLong)) {
                println("Updating the cached file data")
                val result = readEnrichmentTables(enrichment_id, read_enrichment_id, connection_string_id, table_type, status, true)
                if (result._2) {
                  var current_time = System.currentTimeMillis / 60000
                  cacheMap("table_name_" + enrichment_id) = current_time.toString
                  cacheDataframeMap("table_name_" + enrichment_id).unpersist
                  cacheDataframeMap("table_name_" + enrichment_id) = result._1
                  cacheDataframeMap("table_name_" + enrichment_id).cache
                  cacheDataframeMap("table_name_" + enrichment_id).createOrReplaceTempView("table_enrichmentid_" + enrichment_id)
                }
              }
            }
          } else if (current_time - cacheMap("table_name_" + enrichment_id).toLong > cache_frequency.toLong) {
            val result = readEnrichmentTables(enrichment_id, read_enrichment_id, connection_string_id, table_type, status, true)
            if (result._2) {
              var current_time = System.currentTimeMillis / 60000
              cacheMap("table_name_" + enrichment_id) = current_time.toString
              cacheDataframeMap("table_name_" + enrichment_id).unpersist
              cacheDataframeMap("table_name_" + enrichment_id) = result._1
              cacheDataframeMap("table_name_" + enrichment_id).cache
              cacheDataframeMap("table_name_" + enrichment_id).createOrReplaceTempView("table_enrichmentid_" + enrichment_id)
            }
          }
        } else {
          val result = readEnrichmentTables(enrichment_id, read_enrichment_id, connection_string_id, table_type, status, false)
          result._1.createOrReplaceTempView("table_enrichmentid_" + enrichment_id)
        }
      }
    }
  }

  def debug(df: DataFrame, op_subtype: String): DataFrame = {
    if (op_subtype.equalsIgnoreCase("show")) df.show(false)
    else if (op_subtype.equalsIgnoreCase("count")) print(df.count)
    else if (op_subtype.equalsIgnoreCase("printschema")) df.printSchema()
    df
  }

  def splSplit(metadata: String): List[String] = {
    var metadata_ = metadata
    var flag: Boolean = true
    var temp = ""
    var counter = 0
    var column_list: List[String] = List()
    metadata_ = metadata_ + "."
    metadata_.foreach { ch =>
      if (ch == '.' && flag) {
        column_list :+= temp
        temp = ""
      } else if (ch == '`') {
        if (counter % 2 == 0) flag = false
        else flag = true
        counter += 1
        temp = temp + ch
      } else {
        temp = temp + ch
      }
    }
    column_list
  }

  /**
   * isJSON is a User Defined Function (UDF) which returns a boolean value depending on whether the given string is a JSON or not
   *
   * @author praneet.pulivendula
   * @param str String which is to be checked
   * @return True if the parameter str is a valid JSON string else false
   */

  val isJSON = udf { (str: String) => {
    parseFull(str) match {
      case Some(json) => true
      case None => false
    }
  }
  }

  /**
   * Parses the given JSON data and extracts the required fields from it configured in the master configuration table (PULSE_JSON_PARSING)
   *
   * @author praneet.pulivendula
   * @param workflow_id workflow id of the JSON Parsing operation reference to the master table (PULSE_CLIENT_JOB_WORKFLOW)
   * @param inputDF     the DataFrame with JSON data which is to be processed
   * @return a DataFrame which has been processed with all columns given to be extracted from the JSON data in the configuration
   */

  def genericJsonParsing(workflow_id: Long, inputDF: DataFrame, api_id: Long = -1L, read_enrichment_id: Long = -1L, dyn_sql_id: Long = -1): DataFrame = {
    var df = spark.emptyDataFrame
    df = inputDF
    var isSuccess = !df.schema.isEmpty

    if (isSuccess) {
      val attributes: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
      var reqCols: Array[String] = Array()
      var isStruct = true
      var rows = if (workflow_id != (-1)) {
      /*var rows = if (api_id == (-1) && read_enrichment_id == (-1)) {*/
        json_parsing_array.filter(x => x.getAs("WORKFLOW_ID").toString.toLong == workflow_id)
      } else if (read_enrichment_id != (-1)) {
        enrich_json_parsing_array.filter(x => x.getAs("READ_ENRICHMENT_ID").toString.toLong == read_enrichment_id)
      } else if (api_id != (-1)) {
        api_json_parsing_array.filter(x => x.getAs("API_ID").toString.toLong == api_id).groupBy(_.getAs[String]("OUTPUT_COLUMN_NAME")).map { case (_, group) => group.head }.toArray
      } else {
        dyn_sql_json_parsing_array.filter(x => x.getAs("DYN_SQL_ID").toString.toLong == dyn_sql_id)
      }

      scala.util.Sorting.stableSort(rows, (e1: Row, e2: Row) => e1.getAs("SEQUENCE").toString.toInt < e2.getAs("SEQUENCE").toString.toInt)
      val drop_columns = rows.filter(x => x.getAs[String]("TO_BE_DROPPED").equalsIgnoreCase("Y")).map(x => x.getAs[String]("OUTPUT_COLUMN_NAME"))

      rows.foreach { row =>
        var output_column_name = row.getAs[String]("OUTPUT_COLUMN_NAME")
        var metadata_field = row.getAs[String]("METADATA_FIELD")
        var to_be_dropped = row.getAs[String]("TO_BE_DROPPED").equalsIgnoreCase("Y")
        var hardcode = row.getAs[String]("HARDCODE")
        var expression = row.getAs[String]("EXPRESSION")
        var cast_to = row.getAs[String]("CAST_TO")
        var round_to = row.getAs[String]("ROUND_TO")
        var is_date = row.getAs[String]("IS_DATE").equalsIgnoreCase("Y")
        var input_ts_format = row.getAs[String]("INPUT_DATE_FORMAT")
        var output_ts_format = row.getAs[String]("OUTPUT_DATE_FORMAT")
        var extract_all = row.getAs[String]("EXTRACT_ALL").equalsIgnoreCase("Y")
        var time_zone = row.getAs[String]("TIMEZONE")
        var fields_list = splSplit(metadata_field)

        attributes("expression") = expression
        val replaced_attributes = plugins.Variables.variable_replace(attributes)
        expression = replaced_attributes("expression")

        if (Try {
          fields_list(1)
        }.isSuccess) {
          try {
            isStruct = df.schema(fields_list(0)).dataType.typeName.equalsIgnoreCase("struct")
          } catch {
            case ex: Exception => {
              println(s"====> Error Occured While Checking the schema of ${fields_list(0)} field. The Field may not exist.")
              println("====> Error Message: \n" + ex.getMessage)
              isStruct = true
              df = df.withColumn(fields_list(0), lit(null))
            }
          }
          if (!isStruct) {
            try {
              //df.cache()
              var schema = spark.read.json(df.select(fields_list(0)).rdd.map(x => if (x(0) != null) x(0).toString else "")).schema
              if (!schema.isEmpty) {
                df = df.withColumn(fields_list(0), from_json(col(fields_list(0)), schema))
                isStruct = true
              } else {
                //df = spark.emptyDataFrame
                isStruct = false
              }
            } catch {
              case ex: Exception => {
                println("====> Error Occured While Getting the Schema of the Struct field Dynamically")
                println("====> Error Message: \n" + ex.getMessage)
              }
            }
          }
        }

        if (extract_all) {
          val isMetadataValid = df.schema(metadata_field.split("\\.")(0)).dataType.typeName.equalsIgnoreCase("struct")
          if (isMetadataValid) {
            spark.conf.set("spark.sql.caseSensitive", "true")
            val extra_cols = df.columns /*.map(_.toLowerCase())*/ .map(x => col("`" + x + "`"))
            val extra_cols_ = df.columns /*.map(_.toLowerCase())*/ .toSet.diff(List(metadata_field.split("\\.")(0) /*.toLowerCase()*/).toSet).toArray
            val metadata_field_cols = df.select(col(metadata_field)).columns.map(x => col(metadata_field.split("\\.")(0) + ".`" + x + "`").as(output_column_name + x))
            val select_columns = extra_cols ++ metadata_field_cols //Array(col(metadata_field))
            var temp_df = df.select(select_columns: _*)
            val df_columns = df.select(col(metadata_field)).columns.map(c => (output_column_name + c, output_column_name + c.toLowerCase()))
            val df_column_distinct = df_columns.map(x => x._2).distinct
            val amb_cols = df_columns.filter { x => df_columns.toList.count(_._2 == x._2) > 1 } //.sorted
            val distinct_amb_cols = amb_cols.map(x => x._2).distinct
            distinct_amb_cols.foreach { column =>
              val coalesce_cols = amb_cols.filter(x => x._2.equals(column)).map(x => col("`" + x._1 + "`"))
              val cc = amb_cols.filter(x => x._2.equals(column)).map(x => x._1).filter(x => !x.equals(column))
              temp_df = temp_df.withColumn(column, coalesce(coalesce_cols: _*)).drop(cc: _*)
            }
            spark.conf.set("spark.sql.caseSensitive", "false")
            df = temp_df
            reqCols = reqCols ++ df_column_distinct /*.map(x => output_column_name + x)*/ ++ extra_cols_.map(_.toLowerCase())
            if (!to_be_dropped) reqCols = reqCols :+ metadata_field.split("\\.")(0)
          } else {
            reqCols = reqCols ++ df.columns.map(x => x.toLowerCase())
            println(s"====> Invalid Metadata given for Extract all. The field '${metadata_field.split("\\.")(0)}' must be of struct type but it is of type '${df.schema(metadata_field.split("\\.")(0)).dataType.typeName}'.")
          }
        } else {
          if (!hardcode.equals("")) {
            df = df.withColumn(output_column_name, lit(hardcode))
          } else if (hardcode.equals("") && !expression.equals("")) {
            var mandatory_columns = row.getAs[String]("MANDATORY_COLUMNS")
            if (!mandatory_columns.equals("")) {
              if (mandatory_columns.split(",").map(_.toLowerCase()).toSet subsetOf (df.columns.map(_.toLowerCase()).toSet)) {
                //df = df.withColumn(output_column_name, expr(expression))
              } else {
                var open_col_arr = mandatory_columns.split(",").map(_.toLowerCase()).toSet.diff(df.columns.map(_.toLowerCase()).toSet).toArray
                open_col_arr.foreach { column =>
                  df = df.withColumn(column, lit(null))
                }
                println("===> All mandatory columns from expression evaluation do not exist")
              }
            }
            var is_metadata_correct = true

            try {
              df = df.withColumn(output_column_name, expr(expression))
            } catch {
              case ex: Exception => {
                is_metadata_correct = false
                println(s"===> The expression $expression has failed to execute.")
                println(ex.getMessage)
              }
            }
            if (!is_metadata_correct) {
              df = df.withColumn(output_column_name, lit(null))
            }
          } else {
            var tempdf = df
            var is_metadata_correct = true
            try {
              (fields_list.head +: fields_list.tail.map(x => output_column_name + "." + x))
                .foreach { field =>
                  if (field.contains("[")) {
                    if (field.count(x => x == '[') == field.count(x => x == ']')) {
                      var tempfield = field
                      (1 to field.count(x => x == '[')).foreach { _ =>
                        df = df.withColumn(output_column_name, explode(col(tempfield.replace("[", "").replace("]", ""))))
                        tempfield = output_column_name
                      }
                    } else {
                      println("===> Given metadata information is invalid")
                      is_metadata_correct = false
                    }
                  } else if (field.contains("(")) {
                    if (field.count(x => x == '(') == field.count(x => x == ')')) {
                      df = df.withColumn(output_column_name, col(field.substring(0, field.indexOf("("))).getItem(("(?<=\\().+?(?=\\))".r findAllIn field).mkString(",").split(",").head.toInt))
                    } else {
                      println("===> Given metadata information is invalid")
                      is_metadata_correct = false
                    }
                  } else {
                    var col_add_success = true
                    try {
                      df = df.withColumn(output_column_name, col(field))
                    } catch {
                      case ex: Exception => {
                        col_add_success = false
                        println("===> Given metadata information is invalid")
                        println(ex.getMessage)
                      }
                    }
                    if (!col_add_success) {
                      df = df.withColumn(output_column_name, lit(null))
                    }
                  }
                }
            } catch {
              case ex: Exception => {
                is_metadata_correct = false
                println("===> Json Parsing has failed")
                println(ex.getMessage)
              }
            }

            if (!is_metadata_correct) {
              df = tempdf
              println("===> Control point")
            }
          }
          if (df.columns.contains(output_column_name)) {
            if (is_date) {
              if (time_zone.equals(""))
                df = df.withColumn(output_column_name, from_unixtime(unix_timestamp(col(output_column_name), input_ts_format), output_ts_format))
              else
                df = df.withColumn(output_column_name, from_unixtime(unix_timestamp(concat_ws(" ", col(output_column_name), lit(time_zone)), input_ts_format), output_ts_format)) // timezone can be two types 1. name and 2. offset. for name use z and for offset use X
            }
            if (!cast_to.equals("")) {
              df = df.withColumn(output_column_name, col(output_column_name).cast(cast_to))
            }
            if (cast_to.equalsIgnoreCase("double") && !round_to.equals("")) {
              df = df.withColumn(output_column_name, round(col(output_column_name).cast("double"), round_to.toInt))
            }
          }
          if (!to_be_dropped) reqCols = reqCols :+ output_column_name
        }
      }
      reqCols = reqCols.map(_.toLowerCase()).toSet.toArray
      df = if (isSuccess) df.select(reqCols.map(x => col("`" + x + "`").as(x.toLowerCase())): _*).drop("__data__").drop(drop_columns: _*) else df
    }
    df
  }

  /**
   * Parses the Delimited data and extracts the required information based on the master configuration table PULSE_DELIMITER_METADATA
   *
   * @author praneet.pulivendula
   * @param workflow_id workflow id of the Delimiter Parsing operation reference to the master table (PULSE_CLIENT_JOB_WORKFLOW)
   * @param inputDF     the input DataFrame with delimited data that is to be processed
   * @return a DataFrame with all the columns extracted from the delimited data according to the configuration
   */

  def delimiterParsing(workflow_id: Long, inputDF: DataFrame): DataFrame = {
    var df = inputDF
    var retain_cols: List[String] = inputDF.columns.toList
    var rows = delimiter_parsing_array.filter(x => x.getAs("WORKFLOW_ID").toString.toLong == workflow_id)
    scala.util.Sorting.stableSort(rows, (e1: Row, e2: Row) => e1.getAs("SEQUENCE").toString.toInt < e2.getAs("SEQUENCE").toString.toInt)
    rows.foreach { row =>
      var retain = row.getAs[String]("RETAIN").equalsIgnoreCase("Y")
      var output_column_name = row.getAs[String]("OUTPUT_COLUMN_NAME")
      var metadata = row.getAs[String]("METADATA")
      var split_on = row.getAs[String]("SPLIT_ON")
      var index = row.getAs[String]("INDEX_NUMBER")
      var hardcode = row.getAs[String]("HARDCODE")
      var expression = row.getAs[String]("EXPRESSION")
      var cast_to = row.getAs[String]("CAST_TO")
      var round_to = row.getAs[String]("ROUND_TO")
      var is_date = row.getAs[String]("IS_DATE").equalsIgnoreCase("Y")
      var input_ts_format = row.getAs[String]("INPUT_DATE_FORMAT")
      var output_ts_format = row.getAs[String]("OUTPUT_DATE_FORMAT")
      var time_zone = row.getAs[String]("TIMEZONE")

      if (retain) retain_cols = retain_cols :+ output_column_name

      if (!hardcode.equals("")) {
        df = df.withColumn(output_column_name, lit(hardcode))
      } else if (!metadata.equals("")) {
        var isSuccess: Boolean = true
        try {
          df = if (!split_on.equals("") && !index.equals("")) df.withColumn(output_column_name, split(col("`" + metadata + "`"), split_on)(index.toInt))
          else if (!split_on.equals("")) df.withColumn(output_column_name, split(col("`" + metadata + "`"), split_on))
          else if (!index.equals("")) df.withColumn(output_column_name, col("`" + metadata + "`")(index.toInt))
          else df
        } catch {
          case ex: Exception => {
            isSuccess = false
            println("===> Given Metadata information is invalid")
            println(ex.getMessage)
          }
        }
        if (!isSuccess) {
          df = df.withColumn(output_column_name, lit(null))
        }
      }
      if (!expression.equals("")) {
        var mandatory_columns = row.getAs[String]("MANDATORY_COLUMNS")
        if (!mandatory_columns.equals("")) {
          if (mandatory_columns.split(",").toSet subsetOf (df.columns.toSet)) {
            //df = df.withColumn(output_column_name, expr(expression))
          } else {
            var open_col_arr = mandatory_columns.split(",").map(_.toLowerCase()).toSet.diff(df.columns.map(_.toLowerCase()).toSet).toArray
            open_col_arr.foreach { column =>
              df = df.withColumn(column, lit(null))
            }
            println("===> All mandatory columns from expression evaluation do not exist")
          }
        }
        df = df.withColumn(output_column_name, expr(expression))
      }
      if (df.columns.contains(output_column_name)) {
        if (is_date) {
          if (time_zone.equals(""))
            df = df.withColumn(output_column_name, from_unixtime(unix_timestamp(col("`" + output_column_name + "`"), input_ts_format), output_ts_format))
          else
            df = df.withColumn(output_column_name, from_unixtime(unix_timestamp(concat_ws(" ", col("`" + output_column_name + "`"), lit(time_zone)), input_ts_format), output_ts_format)) // timezone can be two types 1. name and 2. offset. for name use z and for offset use X
        }
        if (!cast_to.equals("")) {
          df = df.withColumn(output_column_name, col("`" + output_column_name + "`").cast(cast_to))
        }
        if (cast_to.equalsIgnoreCase("double") && !round_to.equals("")) {
          df = df.withColumn(output_column_name, round(col("`" + output_column_name + "`").cast("double"), round_to.toInt))
        }
      }
    }
    df = df.select(retain_cols.distinct.map(x => col("`" + x + "`")): _*)
    df
  }

  /**
   * For the operation Parsing, based on the operation sub-type, calls the genericJsonParsing function if operation sub-type is JSON or if
   * operation sub-type is delimiter, then delimiterParsing.
   *
   * @author praneet.pulivendula
   * @param workflow_id workflow id of the Parsing operation reference to the master table (PULSE_CLIENT_JOB_WORKFLOW)
   * @param MasterDF    input DataFrame which needs to be processed
   * @param op_subtype  operation sub-type
   * @return Processed DataFrame (JSON or Delimited)
   */

  def genericParsing(workflow_id: Long, MasterDF: DataFrame, op_subtype: String): DataFrame = {
    var df = MasterDF
    if (op_subtype.equalsIgnoreCase("json")) df = genericJsonParsing(workflow_id, df)
    else if (op_subtype.equalsIgnoreCase("delimiter")) df = delimiterParsing(workflow_id, df)
    else df = spark.emptyDataFrame
    if (configuration.getBoolean("debug.parsing.operation")) {
      if (configuration.getBoolean("debug.parsing.show.input")) operation_debug("parsing", workflow_id, df, MasterDF)
      else operation_debug("parsing", workflow_id, df)
    }
    df
  }

  /**
   * Searches for a Regular Expression pattern in a given column and discards the records with a match if the RegExp is given in discard string or
   * inserts a string into a column based on the match all on the basis of master configuration table PULSE_KPI_FILTER_MASTER
   *
   * @author praneet.pulivendula
   * @param workflow_id workflow id of the Field Evaluation operation reference to the master table (PULSE_CLIENT_JOB_WORKFLOW)
   * @param MasterDF    input DataFrame which needs to be processed
   * @return Processed DataFrame based on the configurations
   */

  def setKPIField(MasterDF: DataFrame, workflow_id: Long): DataFrame = {
    var df = MasterDF
    var isSuccess = !df.schema.isEmpty
    var insert_columns: List[String] = List()

    if (isSuccess) {
      var details_array = kpi_filter_array.filter(x => x.getAs("WORKFLOW_ID").toString.toLong == workflow_id)
      if (!details_array.isEmpty) {
        details_array.filter(x => !x.getAs[String]("DISCARD_STRING").equals("")).foreach { row =>
          var discard_string = row.getAs[String]("DISCARD_STRING")
          var search_column = row.getAs[String]("SEARCH_COLUMN")
          df = df.withColumn("nc", regexp_extract(col("`" + search_column + "`"), discard_string, 0)).filter("nc = ''").drop("nc")
        }

        details_array.filter(x => !x.getAs[String]("SEARCH_PATTERN").equals("")).foreach { row =>
          var search_column = row.getAs[String]("SEARCH_COLUMN")
          var search_pattern = row.getAs[String]("SEARCH_PATTERN")
          var insert_string = row.getAs[String]("INSERT_STRING")
          var insert_column = row.getAs[String]("INSERT_COLUMN")

          insert_columns = insert_columns :+ insert_column

          if (!df.columns.contains(insert_column)) {
            df = df.withColumn(insert_column, lit(null))
          }
          df = df.withColumn("nc", regexp_extract(col("`" + search_column + "`"), search_pattern, 0))
            .withColumn(insert_column, when(col("`" + insert_column + "`").isNotNull, col(insert_column)).otherwise(when(col("nc") !== lit(""), lit(insert_string)).otherwise(lit(null)))).drop("nc")
        }

        var discard_if_null_kpi = details_array(0).getAs[String]("DISCARD_IF_NULL_KPI")
        if (!discard_if_null_kpi.equals("")) {
          if (discard_if_null_kpi.equalsIgnoreCase("Y")) {
            insert_columns.distinct.foreach(insert_column => df = df.filter(col("`" + insert_column + "`").isNotNull))
          }
        }
      }
    }
    df
  }

  /**
   * Evaluates the given expressions configured in the master configuration table PULSE_EXPRESSION_EVALUATION on the input DataFrame based
   * on a given sequence.
   *
   * @author praneet.pulivendula
   * @param workflow_id workflow id of the Field Evaluation operation reference to the master table (PULSE_CLIENT_JOB_WORKFLOW)
   * @param MasterDF    input DataFrame which needs to be processed
   * @return a DataFrame with all the expressions evaluated for the workflow_id
   */

  def expression_evaluation(MasterDF: DataFrame, workflow_id: Long): DataFrame = {
    var df = MasterDF
    var isSuccess = !df.schema.isEmpty
    if (isSuccess) {
      var rows = expression_evaluation_array.filter(x => x.getAs("WORKFLOW_ID").toString.toLong == workflow_id)
      scala.util.Sorting.stableSort(rows, (e1: Row, e2: Row) => e1.getAs("SEQUENCE").toString.toInt < e2.getAs("SEQUENCE").toString.toInt)
      rows.foreach { row =>
        var mandatory_columns = row.getAs[String]("MANDATORY_COLUMNS")
        if (!mandatory_columns.equals("")) {
          if (!mandatory_columns.split(",").map(_.toLowerCase()).toSet.subsetOf(df.columns.map(_.toLowerCase()).toSet)) {
            var open_col_arr = mandatory_columns.split(",").toSet.diff(df.columns.toSet).toArray
            open_col_arr.foreach { column =>
              df = df.withColumn(column, lit(null))
            }
          }
        }
        var expression = row.getAs[String]("EXPRESSION")
        var retain_struct_cols = row.getAs[String]("RETAIN_STRUCT_COLS")
        var drop_cols: List[String] = List()
        if (expression.contains("(*)")) {
          var select_columns: List[String] = List()
          var rm_columns: List[String] = List()
          val discard_columns = row.getAs[String]("DISCARD_COLUMNS")
          val evaluation_id = row.getAs("EXPRESSION_EVALUATION_ID").toString.toLong
          if (!discard_columns.equals("")) {
            rm_columns = rm_columns ++ discard_columns.split(",").toList
          }
          expr_eval_dtl_array.filter(x => x.getAs("EXPRESSION_EVALUATION_ID").toString.toLong == evaluation_id).foreach { column =>
            rm_columns = rm_columns ++ getColumnsByRegex(df.columns.toList, List(column.getAs[String]("COLUMN_NAME_REGEX")))
          }
          select_columns = (df.columns.toSet diff rm_columns.toSet).toList
          drop_cols = select_columns

          expression = expression.replaceAll("\\*", select_columns.map(x => "`" + x + "`").mkString(","))
        }
        try {
          df = df.withColumn(row.getAs[String]("OUTPUT_COLUMN_NAME"), expr(expression))
        } catch {
          case ex: Exception => {
            df = df.withColumn(row.getAs[String]("OUTPUT_COLUMN_NAME"), lit(null))
            println(s"===> The expression $expression has failed to execute.")
            println(ex.getMessage)
          }
        }
        if (retain_struct_cols.equalsIgnoreCase("N")) {
          drop_cols = drop_cols ++ mandatory_columns.split(",").toList
          df = df.drop(drop_cols: _*)
        }
      }
    } else df = spark.emptyDataFrame
    df
  }

  /**
   * Calls the required Field Evaluation function based on the operation sub-type
   *
   * @author praneet.pulivendula
   * @param workflow_id workflow id of the Field Evaluation operation reference to the master table (PULSE_CLIENT_JOB_WORKFLOW)
   * @param MasterDF    input DataFrame which needs to be processed
   * @param op_subtype  operation sub-type
   * @return Processed DataFrame (KPI Field Set or Expression Evaluation)
   */

  def field_evaluations(workflow_id: Long, MasterDF: DataFrame, op_subtype: String): DataFrame = {
    var df = MasterDF
    if (op_subtype.equalsIgnoreCase("search pattern")) {
      df = setKPIField(df, workflow_id)
    } else if (op_subtype.equalsIgnoreCase("expression evaluation")) {
      df = expression_evaluation(df, workflow_id)
    } else {
      df = spark.emptyDataFrame
    }
    if (configuration.getBoolean("debug.field_evaluation.operation")) {
      if (configuration.getBoolean("debug.field_evaluation.show.input")) operation_debug("field_evaluation", workflow_id, df, MasterDF)
      else operation_debug("field_evaluation", workflow_id, df)
    }
    df
  }

  /**
   * Does the Field level character or RegExp replacements based on the master configuration table PULSE_FIELD_REPLACMENT_DETAILS
   *
   * @author praneet.pulivendula
   * @param workflow_id workflow id of the JSON Parsing operation reference to the master table (PULSE_CLIENT_JOB_WORKFLOW)
   * @param MasterDF    input DataFrame which needs to be processed
   * @return DataFrame with character replaced fields
   */

  def characterReplacementField(MasterDF: DataFrame, workflow_id: Long): DataFrame = {
    var df = MasterDF
    var isSuccess = !df.schema.isEmpty
    if (isSuccess) {
      var rows = char_replace_array.filter(x => x.getAs("WORKFLOW_ID").toString.toLong == workflow_id)
      scala.util.Sorting.stableSort(rows, (e1: Row, e2: Row) => e1.getAs("SEQUENCE").toString.toInt < e2.getAs("SEQUENCE").toString.toInt)
      rows.foreach { row =>
        df = df.withColumn(row.getAs[String]("COLUMN_NAME"), regexp_replace(col("`" + row.getAs[String]("COLUMN_NAME") + "`"), row.getAs[String]("REPLACE_THIS"), row.getAs[String]("REPLACE_WITH_THIS")))
      }
    } else df = spark.emptyDataFrame
    df
  }

  /**
   * this function does something
   *
   * @author praneet.pulivendula
   * @param MasterDF    something
   * @param workflow_id something
   * @return something
   */

  def misc_transformations(MasterDF: DataFrame, workflow_id: Long): DataFrame = {
    var df = MasterDF
    var isSuccess = !df.schema.isEmpty
    if (isSuccess) {
      var rows = field_transformation_array.filter(x => x.getAs("WORKFLOW_ID").toString.toLong == workflow_id)
      scala.util.Sorting.stableSort(rows, (e1: Row, e2: Row) => e1.getAs("SEQUENCE").toString.toInt < e2.getAs("SEQUENCE").toString.toInt)
      rows.foreach { row =>
        var output_column_name = row.getAs[String]("OUTPUT_COLUMN_NAME")
        var input_ts_format = row.getAs[String]("INPUT_DATE_FORMAT")
        var output_ts_format = row.getAs[String]("OUTPUT_DATE_FORMAT")
        if (row.getAs[String]("IS_DATE").equalsIgnoreCase("Y")) {
          if (row.getAs[String]("TIMEZONE").equals(""))
            df = df.withColumn(output_column_name, from_unixtime(unix_timestamp(col("`" + output_column_name + "`"), input_ts_format), output_ts_format))
          else
            df = df.withColumn(output_column_name, from_unixtime(unix_timestamp(concat_ws(" ", col("`" + output_column_name + "`"), lit(row.getAs[String]("TIMEZONE"))), input_ts_format), output_ts_format)) // timezone can be two types 1. name and 2. offset. for name use z and for offset use X
        }
        if (!row.getAs[String]("CAST_TO").equals("")) {
          df = df.withColumn(output_column_name, col("`" + output_column_name + "`").cast(row.getAs[String]("CAST_TO")))
        }
        if (row.getAs[String]("CAST_TO").equalsIgnoreCase("double") && !row.getAs[String]("ROUND_TO").equals("")) {
          df = df.withColumn(output_column_name, round(col("`" + output_column_name + "`").cast("double"), row.getAs[String]("ROUND_TO").toInt))
        }
      }
    } else df = spark.emptyDataFrame
    df
  }

  def field_transformations(workflow_id: Long, MasterDF: DataFrame, op_subtype: String): DataFrame = {
    var df = MasterDF
    if (op_subtype.equalsIgnoreCase("field replacement")) {
      df = characterReplacementField(df, workflow_id)
    } else if (op_subtype.equalsIgnoreCase("misc")) {
      df = misc_transformations(df, workflow_id)
    } else {
      df = spark.emptyDataFrame
    }
    if (configuration.getBoolean("debug.field_transformation.operation")) {
      if (configuration.getBoolean("debug.field_transformation.show.input")) operation_debug("field_transformation", workflow_id, df, MasterDF)
      else operation_debug("field_transformation", workflow_id, df)
    }
    df
  }

  def dynamicEnrichment(workflow_id: Long, MasterDF: DataFrame): DataFrame = {
    var df = MasterDF
    var extract_columns_arr: Array[String] = Array()
    var isSuccess: Boolean = !df.schema.isEmpty
    var enrichment_cols: Array[String] = Array()
    var rows = enrichment_array.filter(x => x.getAs("WORKFLOW_ID").toString.toLong == workflow_id)
    scala.util.Sorting.stableSort(rows, (e1: Row, e2: Row) => e1.getAs("SEQUENCE").toString.toInt < e2.getAs("SEQUENCE").toString.toInt)
    cacheOrRefreshTables(rows)
    rows.foreach { row =>
      var status = row.getAs[String]("STATUS")
      var extract_columns = row.getAs[String]("EXTRACT_COLUMNS")
      var master_join_columns = row.getAs[String]("MASTER_TABLE_JOIN_COLUMNS")
      var enrich_join_columns = row.getAs[String]("ENRICHMENT_TABLE_JOIN_COLUMNS")
      var join_type = row.getAs[String]("JOIN_TYPE")
      val mandatory_columns = row.getAs[String]("MANDATORY_COLUMNS")
      var additional_columns = row.getAs[String]("ADDITIONAL_COLUMNS_EXPRESSION")
      var output_columns_datatype = row.getAs[String]("OUTPUT_COLUMNS_DATATYPE")
      var drop_columns = row.getAs[String]("DROP_COLUMNS")
      var join_expression = row.getAs[String]("JOIN_CONDITION_EXPRESSION")
      var columns_rename = row.getAs[String]("COLUMNS_RENAME")
      var enrichmentid = row.getAs("ENRICHMENT_ID").toString().toLong
      var enrich_table = "table_enrichmentid_" + enrichmentid
      var broadcast = row.getAs[String]("BROADCAST")

      if (status.equalsIgnoreCase("A") && isSuccess) {
        isSuccess = Try {
          spark.read.table(enrich_table)
        }.isSuccess

        if (!isSuccess) {
          control_point(3, control_point_df, MasterDF)
        } else {
          extract_columns_arr = extract_columns.split(",").map(x => x.split("\\s+")(0))
          enrichment_cols = spark.read.table(enrich_table).columns
          if (!extract_columns_arr.isEmpty && (extract_columns_arr.map(_.toLowerCase).toSet subsetOf enrichment_cols.map(_.toLowerCase).toSet)) {
            spark.sql(s"select $extract_columns from $enrich_table").createOrReplaceTempView(enrich_table)
          }

          enrichment_cols = spark.read.table(enrich_table).columns

          if (((master_join_columns.split(",").map(_.toLowerCase).toSet subsetOf df.columns.map(_.toLowerCase).toSet)
            && (enrich_join_columns.split(",").map(_.toLowerCase).toSet subsetOf enrichment_cols.map(_.toLowerCase).toSet))
            || (master_join_columns.equals("") && enrich_join_columns.equals(""))) {

            df.createOrReplaceTempView("input_table")
            if (broadcast.equalsIgnoreCase("N")) {
              df = spark.sql(s"select master.*,enrich.* from input_table master $join_type join $enrich_table enrich on $join_expression")
            } else {
              df = spark.sql(s"select /*+ Broadcast(enrich) */* from input_table master $join_type join $enrich_table enrich on $join_expression")
            }

            if (!additional_columns.equals("") && !df.columns.contains(additional_columns.split(";")(0))) {
              additional_columns.split("#").foreach { value => df = df.withColumn(value.split(";")(0), expr(value.split(";")(1))) }
            }

            if (!output_columns_datatype.equals("")) {
              output_columns_datatype.split(",").foreach { dtype => df = df.withColumn(dtype.split(":")(0), col(dtype.split(":")(0)).cast(dtype.split(":")(1))) }
            }

            df = df.drop(drop_columns.split(","): _*)

            if (!columns_rename.equals("")) {
              columns_rename.split(",").foreach { column => df = df.withColumnRenamed(column.split(":")(0), column.split(":")(1)) }
            }
          } else {
            isSuccess = !df.schema.isEmpty
            control_point(2, control_point_df, MasterDF)
          }
          /*println("After enrichment " + join_expression)
          df.show()*/
        }
        /*
         * This condition is to handel the case where the enrichment fails but some columns must be present in the output
         */
        if (!mandatory_columns.equals("")) {
          val df_cols = df.columns.map(_.toLowerCase()).toSet
          val man_cols = mandatory_columns.split(",").map(_.toLowerCase()).toSet
          val cols_to_add = man_cols diff df_cols
          cols_to_add.foreach { column =>
            df = df.withColumn(column, lit(null))
          }
        }
      }
      isSuccess = !df.schema.isEmpty
    }
    if (configuration.getBoolean("debug.enrichment.operation")) {
      if (configuration.getBoolean("debug.enrichment.show.input")) operation_debug("enrichment", workflow_id, df, MasterDF)
      else operation_debug("enrichment", workflow_id, df)
    }
    df.select(df.columns.map(x => col("`" + x + "`").as(x.toLowerCase)): _*)
  }

  /*def dfUnion(dfArray: Array[DataFrame]): DataFrame = {
    var finalColumns: Set[String] = Set()
    var openMissingReqCols: Array[Column] = Array[Column]()
    dfArray.foreach { df => finalColumns = finalColumns ++ df.columns }
    var finaldf = finalColumns.toList.foldLeft(spark.emptyDataFrame)((a, b) => a.withColumn(b, lit("anyStringValue")))
    dfArray.foreach { df =>
      openMissingReqCols = finalColumns.toArray.filterNot(df.columns.toSet).map(f => lit(null).as(f.toString))
      finaldf = finaldf.unionByName(df.select(col("*") +: openMissingReqCols: _*))
    }
    finaldf
  }*/

  def convert_struct_to_json(input_df: DataFrame): DataFrame = {
    var df = input_df
    val df_columns: Array[String] = df.columns
    val df_col_schema: Array[String] = df.columns.map(column => df.schema(column).dataType.typeName)
    df_columns.zip(df_col_schema).foreach { column =>
      if (column._2.equalsIgnoreCase("struct") || column._2.equalsIgnoreCase("map")) {
        df = df.withColumn(column._1, to_json(col(column._1)))
      }
      else if (column._2.equalsIgnoreCase("array")) {
        df = df.withColumn(column._1, col(column._1).cast("string"))
      }
    }
    df
  }

  def dfUnion(dfArray: Array[DataFrame]): DataFrame = {
    var finaldf: DataFrame = spark.emptyDataFrame
    if (dfArray.length >= 2) {
      var finalColumns: Set[String] = Set()
      var openMissingReqCols: Array[Column] = Array[Column]()
      dfArray.foreach { df => finalColumns = finalColumns ++ df.columns }
      finaldf = finalColumns.toList.foldLeft(spark.emptyDataFrame)((a, b) => a.withColumn(b, lit("anyStringValue")))
      dfArray.foreach { df =>
        val df1 = convert_struct_to_json(df)
        openMissingReqCols = finalColumns.toArray.filterNot(df1.columns.toSet).map(f => lit(null).as(f.toString))
        finaldf = finaldf.unionByName(df1.select(col("*") +: openMissingReqCols: _*))
      }
    } else if (dfArray.length == 1) {
      finaldf = dfArray(0)
    }
    finaldf
  }

  def windowAggregation(MasterDF: DataFrame, row: org.apache.spark.sql.Row): DataFrame = {
    var df = MasterDF
    var isSuccess: Boolean = !df.schema.isEmpty

    if (isSuccess) {
      df.createOrReplaceTempView("INPUT_TABLE")
      var query = {
        var select_columns = if (!row.getAs[String]("SELECT_COLUMNS").equals("")) row.getAs("SELECT_COLUMNS") else "*"
        var aggregation_expression = row.getAs[String]("AGGREGATION_FUNCTION")
        var partition_by_columns = row.getAs[String]("PARTITIONBY_COLUMNS")
        var order_by_columns = row.getAs[String]("ORDERBY_COLUMNS")
        var order = row.getAs[String]("ASC_OR_DESC")
        var order_by = if (!order_by_columns.equals("")) s"ORDER BY $order_by_columns $order" else ""
        var alias = row.getAs[String]("ALIAS")
        if (partition_by_columns.split(",").map(_.toLowerCase).toSet subsetOf df.columns.map(_.toLowerCase).toSet) {
          s"SELECT $select_columns, $aggregation_expression OVER (PARTITION BY $partition_by_columns $order_by) $alias FROM INPUT_TABLE"
        } else ""
      }
      println("QUERY = " + query)
      //spark.sql(query).show
      isSuccess = true
      try {
        df = spark.sql(query)
      } catch {
        case ex: Exception => {
          isSuccess = false
          println("===> Error in window aggregation")
          println(ex.getMessage)
        }
      }
      if (!isSuccess) {
        control_point(4, control_point_df, MasterDF)
        df = spark.emptyDataFrame
      }
      df
    } else spark.emptyDataFrame
  }

  def groupingAggregation(MasterDF: DataFrame, workflow_id: Long, sequence: Int): DataFrame = {
    var agg_mstr_array = agg_master_array.filter(y => y.getAs("WORKFLOW_ID").toString.toLong == workflow_id && y.getAs("SEQUENCE").toString.toInt == sequence)
    var agg_fltr_array = agg_filter_array.filter(y => y.getAs("workflow_id").toString.toLong == workflow_id && y.getAs("seq").toString.toInt == sequence)
    var agg_grp_array = agg_grouping_array.filter(y => y.getAs("workflow_id").toString.toLong == workflow_id && y.getAs("seq").toString.toInt == sequence)
    var agg_func_array = agg_function_array.filter(y => y.getAs("workflow_id").toString.toLong == workflow_id && y.getAs("seq").toString.toInt == sequence)

    def return_filter_condition(aggregation_id: Long): String = {
      var filter_condition = ""
      var counter = 0
      var rows = agg_fltr_array.filter(x => x.getAs("aggregation_id").toString.toLong == aggregation_id)
      scala.util.Sorting.stableSort(rows, (e1: Row, e2: Row) => e1.getAs("sequence").toString.toInt < e2.getAs("sequence").toString.toInt)
      rows.foreach { row =>
        var logical_op = if (counter > 0) " " + row.getAs[String]("logical_operator") + " " else ""
        var value = if (row.getAs[String]("col_datatype").equalsIgnoreCase("string")) "'" + row.getAs[String]("value") + "'" else row.getAs[String]("value")
        if (!row.getAs[String]("higher_value").equals("")) value = value + " AND " + row.getAs[String]("higher_value")
        filter_condition = filter_condition + logical_op + row.getAs[String]("column_name") + row.getAs[String]("relational_operator") + value
        filter_condition = if (counter > 0) "(" + filter_condition + ")" else filter_condition
        counter += 1
      }
      filter_condition
    }

    var df = MasterDF.withColumn("unique_col", monotonically_increasing_id())

    var missingColumns = aggregationFunctionColumnsArray ++ groupingColumnsArray

    var addMissingColumns = missingColumns.toSet.diff(MasterDF.columns.toSet)
    addMissingColumns.foreach { column =>
      df = df.withColumn(column, lit(null))
    }

    var finaldf = spark.emptyDataFrame

    var ts_cn = ""
    var ts_cf = ""
    var isTimebasedAgg = false
    var flag = true
    var filter_cond = ""

    agg_mstr_array.filter(x => x.getAs("PRIORITY").toString.toInt == 1).foreach { row =>
      var streaming_interval = row.getAs[String]("STREAMING_INTERVAL")
      var delta = row.getAs[String]("DELTA")
      if (!delta.equals("") && !streaming_interval.equals("") && flag) {
        isTimebasedAgg = true
        var col_filtr = return_filter_condition(row.getAs("AGGREGATION_ID").toString.toLong)
        var timestamp_col_name = row.getAs[String]("TIMESTAMP_COL_NAME")
        var timestamp_format = row.getAs[String]("TIMESTAMP_FORMAT")
        var insert_timestamp_col_name = row.getAs[String]("INSERT_TIMESTAMP_COL_NAME")
        var insert_timestamp_format = row.getAs[String]("INSERT_TIMESTAMP_FORMAT")
        ts_cn = timestamp_col_name
        ts_cf = timestamp_format
        filter_cond = filter_cond + "(" + s"$col_filtr and (((unix_timestamp($timestamp_col_name,'$timestamp_format') < ${batchTime.batch_time} - $delta) and (unix_timestamp($timestamp_col_name,'$timestamp_format') >= ${batchTime.batch_time} - $delta - $streaming_interval) and ((${batchTime.batch_time} - $delta) % $streaming_interval) = 0) or (unix_timestamp($timestamp_col_name,'$timestamp_format') < cast(((${batchTime.batch_time} - $streaming_interval - $delta)/$streaming_interval) as int) * $streaming_interval and unix_timestamp($insert_timestamp_col_name,'$insert_timestamp_format') > unix_timestamp('${maxRecordedTime.max_rec_time}')))" + ") OR "
      } else {
        isTimebasedAgg = false
        flag = false
      }
    }

    if (isTimebasedAgg) {
      filter_cond = filter_cond.substring(0, filter_cond.length - 4)
      println(filter_cond)
      df = df.filter(filter_cond)
    }

    df.cache
    var dfcnt = df.count

    if (isTimebasedAgg) {
      var aggidarr = agg_mstr_array.map(x => x.getAs("AGGREGATION_ID").toString.toLong)
      var aggidStreamingintervalArr = agg_mstr_array.map(x => (x.getAs("AGGREGATION_ID").toString, x.getAs[String]("STREAMING_INTERVAL"))).filter(x => !x._2.equals(""))
      var aggidStreamingintervalMap: Map[Long, Int] = Map()
      aggidStreamingintervalArr.foreach { x => aggidStreamingintervalMap = aggidStreamingintervalMap ++ Map(x._1.toLong -> x._2.toInt) }

      df = df.withColumn("streaming_interval", lit(null))

      aggidarr.foreach { aggid =>
        var filter_cond = return_filter_condition(aggid)
        df = df.withColumn("streaming_interval", expr(s"case when streaming_interval is null then (case when $filter_cond then ${aggidStreamingintervalMap(aggid)} else streaming_interval end) else streaming_interval end"))
      }

      df = df.withColumn("ts_agg", from_unixtime(unix_timestamp(col(ts_cn), ts_cf) - (unix_timestamp(col(ts_cn), ts_cf) % col("streaming_interval")), "yyyy-MM-dd HH:mm:ss"))
    }

    if (dfcnt > 0) {
      var distinct_priority_arr = agg_mstr_array.map(x => x.getAs("PRIORITY").toString.toInt).toSet.toArray //agg_master_df.select("priority").distinct.collect.map(x => x(0).toString)

      if (distinct_priority_arr.length > 1) {

        var highpriorityaggidarr = aggregation_df.filter("workflow_id=" + workflow_id + " and sequence=" + sequence).select("aggregation_id", "default_aggregation_id")
          .withColumn("cnt", count("*").over(org.apache.spark.sql.expressions.Window.partitionBy("default_aggregation_id")))
          .filter("cnt>1").select("aggregation_id")
          .collect.map(x => x(0).toString)

        highpriorityaggidarr.foreach { aggid =>
          var filter_cond = return_filter_condition(aggid.toLong)
          df = df.withColumn("tag_" + aggid, expr(s"case when $filter_cond then $aggid else '' end"))
        }

        df = df.withColumn("row_number", row_number().over(org.apache.spark.sql.expressions.Window.partitionBy(lit(1)).orderBy(lit(1))))

        var arr = df.columns.filter(x => x.toLowerCase.contains("tag_"))

        //val array_filter = udf((mainstring: String) => { mainstring.split(",").filter(x => !x.equals("")) })

        df = df.withColumn("nc", explode_outer(array_filter(concat_ws(",", arr.map(x => col(x)): _*))))

        df = df.join(aggregation_df.filter("workflow_id=" + workflow_id + " and sequence=" + sequence).select("aggregation_id", "priority"), col("nc") === col("aggregation_id"), "left")
          .withColumn("rnk", row_number().over(org.apache.spark.sql.expressions.Window.partitionBy("row_number").orderBy(col("priority").desc)))
          .filter("rnk=1")
          .withColumn("priority", when(col("priority").isNull, lit(1)).otherwise(col("priority")))
          .drop(arr ++ Array("row_number", "nc", "aggregation_id", "rnk"): _*)

        var dfArray: Array[DataFrame] = Array()

        distinct_priority_arr.foreach { priority =>
          var sqlstr = aggregationQueryMap((workflow_id, sequence))(priority)
          //println(sqlstr)
          df.filter("priority=" + priority).createOrReplaceTempView("InputDF")
          dfArray = dfArray :+ spark.sql(sqlstr)
        }
        finaldf = dfUnion(dfArray)
      } else {
        var sqlstr = aggregationQueryMap((workflow_id, sequence))(-1)
        println("QUERY = " + sqlstr)
        df.createOrReplaceTempView("InputDF")
        finaldf = spark.sql(sqlstr)
        //println("After aggregation")
        //finaldf.show
      }
      var count_col_arr = agg_func_array.filter(x => x.getAs[String]("agg_function").equalsIgnoreCase("count")).map(x => x.getAs[String]("alias_name")).toSet.toArray
      count_col_arr.foreach { column =>
        finaldf = finaldf.withColumn(column, when(col(column) === 0, lit(null)).otherwise(col(column)))
      }
    }

    finaldf
  }

  def genericAggregation(workflow_id: Long, MasterDF: DataFrame): DataFrame = {
    var df = MasterDF
    var isSuccess: Boolean = !df.schema.isEmpty
    if (isSuccess) {
      var arr = agg_master_array.filter(x => x.getAs("WORKFLOW_ID").toString.toLong == workflow_id).map(x => (x.getAs("SEQUENCE").toString.toInt, x.getAs[String]("IS_WINDOW"))).toSet.toArray
      scala.util.Sorting.stableSort(arr, (e1: (Int, String), e2: (Int, String)) => e1._1 < e2._1)
      arr.foreach { x =>
        if (x._2.equalsIgnoreCase("Y")) {
          var row = agg_master_array.filter(y => y.getAs("WORKFLOW_ID").toString.toLong == workflow_id && y.getAs("SEQUENCE").toString.toInt == x._1)(0) //.filter("workflow_id=" + workflow_id + " and sequence=" + x._1).head
          df = windowAggregation(df, row)
        } else {
          df = groupingAggregation(df, workflow_id, x._1)
        }
      }
      if (configuration.getBoolean("debug.aggregation.operation")) {
        if (configuration.getBoolean("debug.aggregation.show.input")) operation_debug("aggregation", workflow_id, df, MasterDF)
        else operation_debug("aggregation", workflow_id, df)
      }
      df.select(df.columns.map(x => col("`" + x + "`").as(x.toLowerCase)): _*)
    } else spark.emptyDataFrame
  }

  def control_point(control_point_id: Int, control_point_df: DataFrame, errorDF: DataFrame) = {
    //var row = control_point_df.filter("cp_id=" + control_point_id).head
    println("===> Entered Control Point with ID: " + control_point_id)
  }

  /*def write(outdf: DataFrame, workflow_id: Long) = {
    var df = outdf
    var writeDetails = workflow_array.filter(x => x.getAs("workflow_id").toString.toLong == workflow_id)(0) //finalworkflowDF.filter("workflow_id=" + workflow_id).head
    var op_type = writeDetails.getAs[String]("type")
    var op_sub_type = writeDetails.getAs[String]("subtype")
    var connection_string_id = writeDetails.getAs("connection_string_id").toString.toLong
    var read_write_id = writeDetails.getAs("read_write_id").toString.toLong
    if (op_type.equalsIgnoreCase("write")) {
      if (op_sub_type.equalsIgnoreCase("oracle")) {
        println("===> Writing to oracle")
        var tableConnectionDetails: Map[String, String] = Map()
        var tableReadWriteDetails: Map[String, String] = Map()
        connection_array.filter(x => x.getAs("connection_string_id").toString.toLong == connection_string_id).foreach(row => tableConnectionDetails = tableConnectionDetails ++ Map(row.getAs[String]("key") -> row.getAs[String]("value")))
        read_write_array.filter(x => x.getAs("read_write_id").toString.toLong == read_write_id).foreach(row => tableReadWriteDetails = tableReadWriteDetails ++ Map(row.getAs[String]("key") -> row.getAs[String]("value")))
        var tableDetails = tableConnectionDetails ++ tableReadWriteDetails
        //var url = tableDetails("url")
        var username = tableDetails("userid")
        var password = decryptKey(tableDetails("password"))
        var driver = com.jio.pulse.globalVariables.driver //tableDetails("driver")
        var ipaddress = tableDetails("ipaddress")
        var port = tableDetails("port")
        var database = tableDetails("database")
        var url = s"jdbc:oracle:thin:@$ipaddress:$port/$database"
        var tablename = tableDetails("table_name_or_query")
        if (tableDetails.contains("select_columns")) {
          println("===> Entered select columns")
          df.show
          df = df.select(tableDetails("select_columns").split(",").map(x => col(x.trim())): _*)
          //df.show
        }
        if (!tableDetails.contains("table_name_stg")) {
          var trunc = tableDetails("truncate")
          var mode = tableDetails("mode")
          StoreOutput.writeOracle(df, mode, tablename, url, username, password, driver, trunc)
        } else {
          var table_name_stg = tableDetails("table_name_stg")
          var master_update_columns = if (tableDetails.contains("master_update_columns")) tableDetails("master_update_columns") else ""
          var temp_update_columns = if (tableDetails.contains("temp_update_columns")) tableDetails("temp_update_columns") else ""
          var master_insert_columns = if (tableDetails.contains("master_insert_columns")) tableDetails("master_insert_columns") else ""
          var temp_insert_columns = if (tableDetails.contains("temp_insert_columns")) tableDetails("temp_insert_columns") else ""
          var merge_on_columns = tableDetails("merge_on_columns")
          StoreOutput.mergeToOracle(df, url, username, password, driver, tablename, table_name_stg, master_update_columns, temp_update_columns, master_insert_columns, temp_insert_columns, merge_on_columns)
        }
        println("===> Written to oracle")
      } else if (op_sub_type.equalsIgnoreCase("mysql")) {
        println("===> Writing to mysql")
        var tableConnectionDetails: Map[String, String] = Map()
        var tableReadWriteDetails: Map[String, String] = Map()
        connection_array.filter(x => x.getAs("connection_string_id").toString.toLong == connection_string_id).foreach(row => tableConnectionDetails = tableConnectionDetails ++ Map(row.getAs[String]("key") -> row.getAs[String]("value")))
        read_write_array.filter(x => x.getAs("read_write_id").toString.toLong == read_write_id).foreach(row => tableReadWriteDetails = tableReadWriteDetails ++ Map(row.getAs[String]("key") -> row.getAs[String]("value")))
        var tableDetails = tableConnectionDetails ++ tableReadWriteDetails
        //var url = tableDetails("url")
        var username = tableDetails("userid")
        var password = decryptKey(tableDetails("password"))
        var driver = com.jio.pulse.globalVariables.mysqldriver //tableDetails("driver")
        var ipaddress = tableDetails("ipaddress")
        var port = tableDetails("port")
        var database = tableDetails("database")
        var url = s"jdbc:mysql://$ipaddress:$port/$database"
        var tablename = tableDetails("table_name_or_query")
        var trunc = tableDetails("truncate")
        var mode = tableDetails("mode")
        if (tableDetails.contains("select_columns")) {
          df = df.select(tableDetails("select_columns").split(",").map(x => col(x)): _*)
        }
        StoreOutput.writeOracle(df, mode, tablename, url, username, password, driver, trunc)
        println("===> Written to mysql")
      } else if (op_sub_type.equalsIgnoreCase("kafka")) {
        println("===> Writing to Kafka")
        var kafkaConnectionDetails: Map[String, String] = Map()
        var kafkaReadWriteDetails: Map[String, String] = Map()
        connection_array.filter(x => x.getAs("connection_string_id").toString.toLong == connection_string_id).foreach(row => kafkaConnectionDetails = kafkaConnectionDetails ++ Map(row.getAs[String]("key") -> row.getAs[String]("value")))
        read_write_array.filter(x => x.getAs("read_write_id").toString.toLong == read_write_id).foreach(row => kafkaReadWriteDetails = kafkaReadWriteDetails ++ Map(row.getAs[String]("key") -> row.getAs[String]("value")))
        var kafkaDetails = kafkaConnectionDetails ++ kafkaReadWriteDetails
        var bootstrap_server = kafkaDetails("bootstrap_servers")
        var topic = kafkaDetails("topics")
        var contains_write_as_json = kafkaDetails.contains("write_as_json")
        var write_as_json = true
        if (contains_write_as_json) {
          write_as_json = kafkaDetails("write_as_json").equalsIgnoreCase("true")
        }
        val isSSL = kafkaDetails.contains("security.protocol")
        var kafkaConfSSLMap: Map[String, String] = Map()
        if (isSSL) {
          if (kafkaDetails.contains("security.protocol")) {
            kafkaConfSSLMap = kafkaConfSSLMap ++ Map("kafka.security.protocol" -> kafkaDetails("security.protocol"))
          }
          if (kafkaDetails.contains("ssl.truststore.location")) {
            kafkaConfSSLMap = kafkaConfSSLMap ++ Map("kafka.ssl.truststore.location" -> kafkaDetails("ssl.truststore.location"))
          }
          if (kafkaDetails.contains("ssl.truststore.password")) {
            kafkaConfSSLMap = kafkaConfSSLMap ++ Map("kafka.ssl.truststore.password" -> kafkaDetails("ssl.truststore.password"))
          }
          if (kafkaDetails.contains("ssl.keystore.location")) {
            kafkaConfSSLMap = kafkaConfSSLMap ++ Map("kafka.ssl.keystore.location" -> kafkaDetails("ssl.keystore.location"))
          }
          if (kafkaDetails.contains("ssl.keystore.password")) {
            kafkaConfSSLMap = kafkaConfSSLMap ++ Map("kafka.ssl.keystore.password" -> kafkaDetails("ssl.keystore.password"))
          }
          if (kafkaDetails.contains("ssl.truststore.type")) {
            kafkaConfSSLMap = kafkaConfSSLMap ++ Map("kafka.ssl.truststore.type" -> kafkaDetails("ssl.truststore.type"))
          }
          if (kafkaDetails.contains("ssl.endpoint.identification.algorithm")) {
            kafkaConfSSLMap = kafkaConfSSLMap ++ Map("kafka.ssl.endpoint.identification.algorithm" -> kafkaDetails("ssl.endpoint.identification.algorithm"))
          }
        }
        //futureOperations.append(kafkaWrite(df, bootstrap_server, topic))
        //println("=====> output Count: "+df.count)
        if (write_as_json) {
          //println("Entered write as json")
          //println(isSSL)
          if (isSSL) {
            //println("Entered SSL")
            //println(kafkaConfSSLMap)
            //println(bootstrap_server,topic)
            df.toJSON.toDF("value").write.format("kafka").options(kafkaConfSSLMap).option("kafka.bootstrap.servers", bootstrap_server).option("topic", topic).save()
          } else {
            df.toJSON.toDF("value").write.format("kafka").option("kafka.bootstrap.servers", bootstrap_server).option("topic", topic).save()
          }
        } else {
          var select_column = kafkaDetails("column_name")
          if (isSSL) {
            df.select(select_column).toDF("value").withColumn("value", col("value").cast("string")).write.format("kafka").options(kafkaConfSSLMap).option("kafka.bootstrap.servers", bootstrap_server).option("topic", topic).save()
          } else {
            df.select(select_column).toDF("value").withColumn("value", col("value").cast("string")).write.format("kafka").option("kafka.bootstrap.servers", bootstrap_server).option("topic", topic).save()
          }
        }
        println("===> Written to Kafka")
      } else if (op_sub_type.equalsIgnoreCase("hdfs")) {
        println("===> Writing to hdfs")
        var hdfsConnectionDetails: Map[String, String] = Map()
        var hdfsReadWriteDetails: Map[String, String] = Map()
        connection_array.filter(x => x.getAs("connection_string_id").toString.toLong == connection_string_id).foreach(row => hdfsConnectionDetails = hdfsConnectionDetails ++ Map(row.getAs[String]("key") -> row.getAs[String]("value")))
        read_write_array.filter(x => x.getAs("read_write_id").toString.toLong == read_write_id).foreach(row => hdfsReadWriteDetails = hdfsReadWriteDetails ++ Map(row.getAs[String]("key") -> row.getAs[String]("value")))
        var hdfsDetails = hdfsConnectionDetails ++ hdfsReadWriteDetails
        var url = hdfsDetails("url")
        var path = hdfsDetails("path")
        var format = hdfsDetails("format")
        var mode = if (!hdfsDetails.contains("mode")) "overwrite" else hdfsDetails("mode")

        var optionsMap = scala.collection.Map(
          "multiline" -> { if (hdfsDetails.contains("multiline")) "true" else "false" },
          "header" -> { if (hdfsDetails.contains("header")) "true" else "false" })

        if (format.equals("csv")) {
          optionsMap = optionsMap ++ scala.collection.Map("sep" -> { if (hdfsDetails.contains("seperator")) hdfsDetails("seperator") else "," })
        }

        if (hdfsDetails.contains("compression")) {
          optionsMap = optionsMap ++ scala.collection.Map("compression" -> hdfsDetails("compression"))
        }

        if (hdfsDetails.contains("partitionby")) {
          df.write.partitionBy(hdfsDetails("partitionby").split(","): _*).format(format).options(optionsMap).mode(mode).save(path)
        } else {
          df.write.format(format).options(optionsMap).mode(mode).save(url + path)
          //df.write.format(format).options(optionsMap).mode(mode).save(path)
        }
        println("===> Written to hdfs")
      } else if (op_sub_type.equalsIgnoreCase("hive")) {
        println("===> Writing to hive")
        var hiveConnectionDetails: Map[String, String] = Map()
        var hiveReadWriteDetails: Map[String, String] = Map()
        connection_array.filter(x => x.getAs("connection_string_id").toString.toLong == connection_string_id).foreach(row => hiveConnectionDetails = hiveConnectionDetails ++ Map(row.getAs[String]("key") -> row.getAs[String]("value")))
        read_write_array.filter(x => x.getAs("read_write_id").toString.toLong == read_write_id).foreach(row => hiveReadWriteDetails = hiveReadWriteDetails ++ Map(row.getAs[String]("key") -> row.getAs[String]("value")))
        var hiveDetails = hiveConnectionDetails ++ hiveReadWriteDetails
        var database = hiveDetails("database")
        var table = hiveDetails("table_name")
        if (hiveDetails.contains("partitionby"))
          df.write.format("hive").partitionBy(hiveDetails("partitionby").split(","): _*).saveAsTable(database + "." + table)
        else
          df.write.format("hive").saveAsTable(database + "." + table)
        println("===> Written to hive")
      } else if (op_sub_type.equalsIgnoreCase("elasticsearch")) {
        println("===> Writing to elastic")
        var elasticConnectionDetails: Map[String, String] = Map()
        var elasticReadWriteDetails: Map[String, String] = Map()
        connection_array.filter(x => x.getAs("connection_string_id").toString.toLong == connection_string_id).foreach(row => elasticConnectionDetails = elasticConnectionDetails ++ Map(row.getAs[String]("key") -> row.getAs[String]("value")))
        read_write_array.filter(x => x.getAs("read_write_id").toString.toLong == read_write_id).foreach(row => elasticReadWriteDetails = elasticReadWriteDetails ++ Map(row.getAs[String]("key") -> row.getAs[String]("value")))
        var optionsMap: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
        read_write_array.filter(x => x.getAs("read_write_id").toString.toLong == read_write_id && x.getAs[String]("key").contains("es.")).foreach(row => optionsMap = optionsMap ++ Map(row.getAs[String]("key") -> row.getAs[String]("value")))
        var elasticDetails = elasticConnectionDetails ++ elasticReadWriteDetails
        var node = elasticDetails("node")
        var port = elasticDetails("port")
        var index = elasticDetails("index")
        var username = elasticDetails("username")
        var password = decryptKey(elasticDetails("password"))
        var mode = elasticDetails("mode")
        //var index_timestamp = if(elasticReadWriteDetails.contains("index_timestamp")) elasticReadWriteDetails("index_timestamp") else ""
        var indexTimestamp = if (elasticDetails.contains("index_timestamp")) elasticDetails("index_timestamp") else ""
        var current_date = ReadInput.getDate("yyyy.MM.dd")
        if (indexTimestamp.equalsIgnoreCase("current_timestamp")) index = index + "-" + current_date
        //df.show(false)
        //ApiUDF.clearProxy
        df.write.format("org.elasticsearch.spark.sql")
          .option("es.nodes", node)
          .option("es.port", port)
          .option("es.net.http.auth.user", username)
          .option("es.net.http.auth.pass", password)
          .options(optionsMap)
          .option("es.write.operation", "index")
          .option("es.batch.write.refresh", false)
          .mode(mode)
          .save(index + "/_doc")
        println("===> Written to elastic")
      }
    }
  }*/

  def writeDf(outdf: DataFrame, workflow_id: Long) = {
    var writeDetails = workflow_array.filter(x => x.getAs("workflow_id").toString.toLong == workflow_id)(0)
    var op_type = writeDetails.getAs[String]("type")
    var op_sub_type = writeDetails.getAs[String]("subtype")
    var connection_string_id = writeDetails.getAs("connection_string_id").toString.toLong
    var read_write_id = writeDetails.getAs("read_write_id").toString.toLong
    var ConnectionDetails: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
    var WriteDetails: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
    connection_array.filter(x => x.getAs("connection_string_id").toString.toLong == connection_string_id && x.getAs("is_column").toString.equalsIgnoreCase("N")).foreach(row => ConnectionDetails = ConnectionDetails ++ Map(row.getAs[String]("key") -> row.getAs[String]("value")))
    read_write_array.filter(x => x.getAs("read_write_id").toString.toLong == read_write_id && x.getAs("is_column").toString.equalsIgnoreCase("N")).foreach(row => WriteDetails = WriteDetails ++ Map(row.getAs[String]("key") -> row.getAs[String]("value")))
    var attribute_map_N = ConnectionDetails ++ WriteDetails
    ConnectionDetails.clear()
    WriteDetails.clear()
    connection_array.filter(x => x.getAs("connection_string_id").toString.toLong == connection_string_id && x.getAs("is_column").toString.equalsIgnoreCase("Y")).foreach(row => ConnectionDetails = ConnectionDetails ++ Map(row.getAs[String]("key") -> row.getAs[String]("value")))
    read_write_array.filter(x => x.getAs("read_write_id").toString.toLong == read_write_id && x.getAs("is_column").toString.equalsIgnoreCase("Y")).foreach(row => WriteDetails = WriteDetails ++ Map(row.getAs[String]("key") -> row.getAs[String]("value")))
    var attribute_map_Y = ConnectionDetails ++ WriteDetails
    if (attribute_map_Y.isEmpty) {
      println("===> Writing to " + op_sub_type)
      write(op_sub_type, attribute_map_N, outdf)
      println("===> Written to " + op_sub_type)
    } else {
      println("===> Writing to " + op_sub_type)
      var futureOperations = ListBuffer[Future[Any]]()
      var columns_list = attribute_map_Y.map(x => x._2).toList
      var base_filter_cond = ""
      columns_list.foreach { column =>
        base_filter_cond += column + " is not null and "
      }
      base_filter_cond = base_filter_cond.substring(0, base_filter_cond.lastIndexOf(" and "))
      //println(base_filter_cond)
      //outdf.cache
      val temp_outdf = outdf.filter(base_filter_cond)
      temp_outdf.cache()
      //temp_outdf.show
      var distinct_attribute_rows = temp_outdf.select(columns_list.map(c => col(c)): _*).distinct.collect
      distinct_attribute_rows.foreach { row =>
        var filter_condition = ""
        attribute_map_Y.foreach { x =>
          attribute_map_N = attribute_map_N ++ Map(x._1 -> row.getAs[String](x._2))
          filter_condition += x._2 + "=" + "'" + row.getAs[String](x._2) + "' AND "
        }
        filter_condition = filter_condition.substring(0, filter_condition.lastIndexOf(" AND "))
        //println(attribute_map_N)
        //println(filter_condition)
        //temp_outdf.filter(filter_condition).drop(columns_list: _*).show
        futureOperations.append(write_future(op_sub_type, attribute_map_N, temp_outdf.filter(filter_condition).drop(columns_list: _*)))
      }
      val futureResults = Future.sequence(futureOperations)
      //Await.result(futureResults, Duration.Inf)
      temp_outdf.unpersist()
      println("===> Written to " + op_sub_type)
    }
  }

  def write_future(destination: String, map: scala.collection.mutable.Map[String, String], outdf: DataFrame): Future[String] = Future {
    write(destination, map, outdf)
    destination
  }

  def kafkaWrite(df: DataFrame, output_bootstrap_servers: String, topic_write: String): Future[String] = Future {
    df.toJSON.toDF("value").write.format("kafka")
      .option("kafka.bootstrap.servers", output_bootstrap_servers)
      .option("topic", topic_write).save()

    topic_write
  }

   def exception_handling(f: (Long, DataFrame) => DataFrame, inputDF: DataFrame, workflow_id: Long, next_step_id: Long, exception_id: Long): ArrayBuffer[(Any, Long, Long, Long, String)] = {
    try {
      fork(f(workflow_id, inputDF), next_step_id, workflow_id)
    }
    catch {
      case e: Exception => {
        println("====> Getting exception while going to execute workflow", s"\n$e.getMessage")
        fork(inputDF, exception_id, workflow_id)
      }
    }

  }




 def exception_handling(f: (Long, DataFrame, String) => DataFrame, inputDF: DataFrame, workflow_id: Long,next_step_id: Long, exception_id: Long ,op_subtype: String): ArrayBuffer[(Any, Long, Long, Long, String)] = {
    try {
      fork(f(workflow_id, inputDF, op_subtype), next_step_id, workflow_id)
    }
    catch {
      case e: Exception => {
        println("====> Getting exception while going to execute workflow", s"\n$e.getMessage")
        fork(inputDF,exception_id, workflow_id)
      }
    }

  }





  def fork(inputDF: DataFrame, workflow_id: Long, previous_workflow_id: Long = 0L, fork_taken_care: String = "N"): ArrayBuffer[Tuple5[Any, Long, Long, Long, String]] = {
    if (workflow_id == (-1)) return ArrayBuffer((1L, 1L, 1L, 1L, ""))
    //println("==> Entered fork "+ workflow_id)
    var workflow_row = workflow_array.filter(x => x.getAs("workflow_id").toString.toLong == workflow_id)(0) //finalworkflowDF.filter("workflow_id=" + workflow_id).head
    var dependency_id = workflow_row.getAs("dependency_id").toString.toLong
    var op_type = workflow_row.getAs[String]("type")
    var op_subtype = workflow_row.getAs[String]("subtype")
    var dependency_count = workflow_row.getAs[java.lang.Integer]("fork")
    var next_step_id = workflow_row.getAs("next_id").toString.toInt
    var exception_wf_id= workflow_row.getAs("exception_id").toString.toLong
    var mapVar = scala.collection.mutable.Map[Long, (DataFrame, Long, Long, String)]()
    if (dependency_count > 1 && fork_taken_care == "N") {
      //println("==> Entered dep cnt>1 "+ workflow_id)
      //println("before cache")
      inputDF.cache()
      //println("cached input df")
      //inputDF.checkpoint()
      var ordered_worflow_array = workflow_array.filter(x => x.getAs("dependency_id").toString.toLong == dependency_id)
      scala.util.Sorting.stableSort(ordered_worflow_array, (e1: Row, e2: Row) => e1.getAs("sequence").toString.toInt < e2.getAs("sequence").toString.toInt)

      //workflow_array.filter(x => x.getAs("dependency_id").toString.toLong == dependency_id).foreach { inp =>
      ordered_worflow_array.foreach { inp =>
        var arrbfr_ = fork(inputDF, inp.getAs("workflow_id").toString.toLong, previous_workflow_id, fork_taken_care = "Y")
        var index = 0

        while (index < arrbfr_.length) {
          var newdf = arrbfr_(index)._1
          var next_wf_id = arrbfr_(index)._2
          var previous_wf_id = arrbfr_(index)._3
          var current_wf_id = arrbfr_(index)._4
          var processed_flag = arrbfr_(index)._5
          //println("Map Status Inside while loop (start) " + mapVar)
          //println("ArrayBuffer status inside while loop (start) " + arrbfr_.mkString(","))
          if (newdf.isInstanceOf[DataFrame]) {
            if (mapVar.contains(current_wf_id)) {
              //println("(if)===> Next step id " + current_wf_id)
              //mapVar(current_wf_id) = (inputDF, previous_wf_id, next_wf_id, "Y")
              var operation = workflow_array.filter(x => x.getAs("workflow_id").toString.toLong == current_wf_id)(0).getAs[String]("type")
              if (operation.equalsIgnoreCase("join")) { //if (!join_array.filter(x => x.getAs("WORKFLOW_ID").toString.toInt == current_wf_id).isEmpty) {
                var joinresult = join((mapVar(current_wf_id)._1, mapVar(current_wf_id)._2), (newdf.asInstanceOf[DataFrame], previous_wf_id), current_wf_id)
                mapVar(current_wf_id) = (joinresult._1, joinresult._2, next_wf_id, "Y")
              } else if (operation.equalsIgnoreCase("union")) {
                var unionresult = union(mapVar(current_wf_id)._1, newdf.asInstanceOf[DataFrame])
                mapVar(current_wf_id) = (unionresult, previous_wf_id, next_wf_id, "Y")
              } else if (operation.equalsIgnoreCase("intersection")) {
                var intersectionresult = mapVar(current_wf_id)._1.intersect(newdf.asInstanceOf[DataFrame]) //union(mapVar(current_wf_id)._1, newdf.asInstanceOf[DataFrame])
                mapVar(current_wf_id) = (intersectionresult, previous_wf_id, next_wf_id, "Y")
              }
              var current_wf_id_ = next_wf_id
              var row_ = workflow_array.filter(x => x.getAs("workflow_id").toString.toLong == current_wf_id_)(0)
              var next_wf_id_ = row_.getAs("next_id").toString.toLong
              arrbfr_ ++= fork(mapVar(current_wf_id)._1, current_wf_id_)
              mapVar = mapVar.-(current_wf_id)
            } else {
              //println("(else)===> Next step id " + current_wf_id)
              mapVar(current_wf_id) = (newdf.asInstanceOf[DataFrame], previous_wf_id, next_wf_id, processed_flag)
            }
          }
          index += 1
          //println("Map Status Inside while loop (end) " + mapVar)
          //println("ArrayBuffer status inside while loop (end) " + arrbfr_.mkString(","))
        }
        //println("Map at the end of the loop "+mapVar)
      }
      inputDF.unpersist()
      //println("Map out of the loop "+mapVar)
      var arrbfr: ArrayBuffer[Tuple5[Any, Long, Long, Long, String]] = ArrayBuffer()
      mapVar.foreach { inputTuple =>
        if (inputTuple._2._4.equalsIgnoreCase("N")) {
          arrbfr :+= (inputTuple._2._1, inputTuple._2._3, inputTuple._2._2, inputTuple._1, "N")
        }
      }
      if (arrbfr.length > 0) {
        return arrbfr
      }
      ArrayBuffer((1L, 1L, 1L, 1L, ""))
    } else if (op_type.equalsIgnoreCase("conditional")) {
      println("==> Entered conditional " + workflow_id)
      //println("current workflow array: ", workflow_array.toList)
      workflow_array = updateCurrentWorkflowDeps(workflow_array, conditional_workflow(inputDF, workflow_id))
      //println("new workflow array: ", workflow_array.toList)
      workflow_row = workflow_array.filter(x => x.getAs("workflow_id").toString.toLong == workflow_id)(0)
      dependency_id = workflow_row.getAs("dependency_id").toString.toLong
      op_type = workflow_row.getAs[String]("type")
      op_subtype = workflow_row.getAs[String]("subtype")
      dependency_count = workflow_row.getAs[java.lang.Integer]("fork")
      next_step_id = workflow_row.getAs("next_id").toString.toInt
      fork(inputDF, next_step_id, workflow_id)
    } else if (op_type.equalsIgnoreCase("filter") /*&& !inputDF.schema.isEmpty*/ ) {
      println("==> Entered filter operation with workflow_id = " + workflow_id)
      //fork(filter(workflow_id, inputDF), next_step_id, workflow_id)
      exception_handling(filter, inputDF, workflow_id, next_step_id, exception_wf_id)
    } else if (op_type.equalsIgnoreCase("debug")) {
      debug(inputDF, op_subtype)
      fork(inputDF, next_step_id, workflow_id)
    } else if (op_type.equalsIgnoreCase("parsing") /*&& !inputDF.schema.isEmpty*/ ) {
      println("==> Entered parsing operation with workflow_id = " + workflow_id)
      //fork(genericParsing(workflow_id, inputDF, op_subtype), next_step_id, workflow_id)
      exception_handling(genericParsing, inputDF,workflow_id, next_step_id, exception_wf_id, op_subtype)
    } else if (op_type.equalsIgnoreCase("field evaluation") /*&& !inputDF.schema.isEmpty*/ ) {
      println("==> Entered field evalulation operation with workflow_id = " + workflow_id)
      //fork(field_evaluations(workflow_id, inputDF, op_subtype), next_step_id, workflow_id)
      exception_handling(field_evaluations, inputDF,workflow_id, next_step_id, exception_wf_id, op_subtype)
    } else if (op_type.equalsIgnoreCase("field transformation") /*&& !inputDF.schema.isEmpty*/ ) {
      println("==> Entered field transformation operation with workflow_id = " + workflow_id)
      //fork(field_transformations(workflow_id, inputDF, op_subtype), next_step_id, workflow_id)
      exception_handling(field_transformations, inputDF,workflow_id, next_step_id, exception_wf_id, op_subtype)
    } else if (op_type.equalsIgnoreCase("column rename") /*&& !inputDF.schema.isEmpty*/ ) {
      println("==> Entered column rename operation with workflow_id = " + workflow_id)
      //fork(columnRename(workflow_id, inputDF), next_step_id, workflow_id)
       exception_handling(columnRename, inputDF, workflow_id, next_step_id, exception_wf_id)
    } else if (op_type.equalsIgnoreCase("enrichment") /*&& !inputDF.schema.isEmpty*/ ) {
      println("==> Entered Enrichment operation with workflow_id = " + workflow_id)
      exception_handling(dynamicEnrichment, inputDF, workflow_id, next_step_id, exception_wf_id)
     // fork(dynamicEnrichment(workflow_id, inputDF), next_step_id, workflow_id)
    } else if (op_type.equalsIgnoreCase("aggregation") /*&& !inputDF.schema.isEmpty*/ ) {
      println("==> Entered Aggregation operation with workflow_id = " + workflow_id)
     // fork(genericAggregation(workflow_id, inputDF), next_step_id, workflow_id)
      exception_handling(genericAggregation, inputDF, workflow_id, next_step_id, exception_wf_id)
    } else if (op_type.equalsIgnoreCase("coalesce") /*&& !inputDF.schema.isEmpty*/ ) {
      println("==> Entered Coalesce operation with workflow_id = " + workflow_id)
      //fork(coalesceDF(workflow_id, inputDF), next_step_id, workflow_id)
      exception_handling(coalesceDF, inputDF, workflow_id, next_step_id, exception_wf_id)
    } else if (op_type.equalsIgnoreCase("explode") /*&& !inputDF.schema.isEmpty*/ ) {
      println("==> Entered Explode operation with workflow_id = " + workflow_id)
      //fork(explodeDF(workflow_id, inputDF), next_step_id, workflow_id)
       exception_handling(explodeDF2, inputDF, workflow_id, next_step_id, exception_wf_id)
    } else if (op_type.equalsIgnoreCase("pivot") /*&& !inputDF.schema.isEmpty*/ ) {
      println("==> Entered Pivot operation with workflow_id = " + workflow_id)
      try {
        inputDF.cache()
      val pivot_output_df = pivotDF(workflow_id, inputDF)
      var partition_count = 20
      val master = spark.sparkContext.getConf.get("spark.master")
      master.toLowerCase match {
        case x if x.contains("spark:") => partition_count = spark.sparkContext.getConf.get("spark.cores.max").toInt
        case x if x.contains("k8s:") => partition_count = spark.sparkContext.getConf.get("spark.executor.instances").toInt * spark.sparkContext.getConf.get("spark.executor.cores").toInt
        case x if x.contains("local:") => partition_count = 20
        case _ => println("unknown master")
          }
        val fork_reurn = fork(pivot_output_df.repartition(partition_count), next_step_id, workflow_id)
        inputDF.unpersist()
        fork_reurn
    }
      catch {
        case e: Exception => {
          println("getting exception while and going to execute exception workflow", e.getMessage)
          fork(inputDF, exception_wf_id, workflow_id)
        }}


    } else if (op_type.equalsIgnoreCase("sparksql") /*&& !inputDF.schema.isEmpty*/ ) {
      println("==> Entered Spark Sql Exec operation with workflow_id = " + workflow_id)
      //fork(sparkSqlExec(workflow_id, inputDF), next_step_id, workflow_id)
       exception_handling(sparkSqlExec, inputDF, workflow_id, next_step_id, exception_wf_id)
    } else if (op_type.equalsIgnoreCase("dml execution") /*&& !inputDF.schema.isEmpty*/ ) {
      println("==> Entered DML Execution operation with workflow_id = " + workflow_id)
      //fork(dmlExecution(workflow_id, inputDF), next_step_id, workflow_id)
      exception_handling(dmlExecution, inputDF, workflow_id, next_step_id, exception_wf_id)
    } else if (op_type.equalsIgnoreCase("dynamicsql") /*&& !inputDF.schema.isEmpty*/ ) {
      println("==> Entered Dynamic SQL operation with workflow_id = " + workflow_id)
      //fork(dynamic_sql_build(inputDF, workflow_id), next_step_id, workflow_id)
      exception_handling(dynamic_sql_build, inputDF, workflow_id, next_step_id, exception_wf_id)
    } else if (op_type.equalsIgnoreCase("distinct") /*&& !inputDF.schema.isEmpty*/ ) {
      println("==> Entered Distinct operation with workflow_id = " + workflow_id)
      try{
      fork(inputDF.distinct, next_step_id, workflow_id) }
      catch {
        case e: Exception => {
          println("getting exception while and going to execute exception workflow", e.getMessage)
          fork(inputDF, exception_wf_id, workflow_id)
        }}
      } else if (op_type.equalsIgnoreCase("cache") /*&& !inputDF.schema.isEmpty*/ ) {
      println("==> Entered Cache operation with workflow_id = " + workflow_id)
      try {
        fork(inputDF.cache, next_step_id, workflow_id)
      }
      catch {
        case e: Exception => {
          println("getting exception while and going to execute exception workflow", e.getMessage)
          fork(inputDF, exception_wf_id, workflow_id)
        }}
    } else if (op_type.equalsIgnoreCase("read")) {
      println("==> Entered Read operation with workflow_id = " + workflow_id)
      //fork(readBatch(workflow_id, inputDF), next_step_id, workflow_id)
      exception_handling(readBatch, inputDF, workflow_id, next_step_id, exception_wf_id)
    } else if (op_type.equalsIgnoreCase("repartition")) {
      println("==> Entered Repartition operation with workflow_id = " + workflow_id)
      //fork(dfRepartition(inputDF, workflow_id), next_step_id, workflow_id)
      exception_handling(dfRepartition, inputDF, workflow_id, next_step_id, exception_wf_id)
    } else if (op_type.equalsIgnoreCase("coalesce_partition")) {
      println("==> Entered Coalesce Partition operation with workflow_id = " + workflow_id)
      //fork(dfCoalesce(inputDF, workflow_id), next_step_id, workflow_id)
      exception_handling(dfCoalesce, inputDF, workflow_id, next_step_id, exception_wf_id)
    } else if (op_type.equalsIgnoreCase("write") && !inputDF.schema.isEmpty) {
      println("==> Entered Write operation with workflow_id = " + workflow_id)
      if (configuration.getBoolean("debug.write.operation")) {
        operation_debug("write", workflow_id, inputDF)
      }
      try {
      writeDf(inputDF, workflow_id)
        fork(inputDF, next_step_id, workflow_id)
      }
      catch {
        case e: Exception => {
          println("getting exception while and going to execute exception workflow", e.getMessage)
          fork(inputDF, exception_wf_id, workflow_id)
        }
      }
      //exception_handling_write(inputDF, workflow_id, next_step_id, exception_wf_id)
      //fork(inputDF, next_step_id, workflow_id)

      //ArrayBuffer((1, 1, 1, 1, ""))
    } else if (op_type.equalsIgnoreCase("union") /* && !inputDF.schema.isEmpty*/ ) {
      println("==> Entered Union operation with workflow_id = " + workflow_id)
      ArrayBuffer((inputDF, next_step_id, previous_workflow_id, workflow_id, "N"))
    } else if (op_type.equalsIgnoreCase("join") /* && !inputDF.schema.isEmpty*/ ) {
      println("==> Entered Join operation with workflow_id = " + workflow_id)
      ArrayBuffer((inputDF, next_step_id, previous_workflow_id, workflow_id, "N"))
    } else if (op_type.equalsIgnoreCase("intersection") /* && !inputDF.schema.isEmpty*/ ) {
      println("==> Entered Intersection operation with workflow_id = " + workflow_id)
      ArrayBuffer((inputDF, next_step_id, previous_workflow_id, workflow_id, "N"))
    } else if (op_type.equalsIgnoreCase("loop") /*&& !inputDF.schema.isEmpty*/ ) {
      println("==> Entered loop " + workflow_id)
      try{
      loop_workflow(workflow_id, inputDF)}
      catch {
        case e: Exception => {
          println("getting exception while and going to execute exception workflow", e.getMessage)
          fork(inputDF, exception_wf_id, workflow_id)
        }
      }
    } else if (op_type.equalsIgnoreCase("file listing") /*&& !inputDF.schema.isEmpty*/ ) {
      println("==> Entered File Listing " + workflow_id)
      fork(file_listing_workflow(workflow_id, inputDF), next_step_id, workflow_id)
      //exception_handling(file_listing_workflow, inputDF, workflow_id, next_step_id, exception_wf_ild)
    } else if (op_type.equalsIgnoreCase("schema validation") /*&& !inputDF.schema.isEmpty*/ ) {
      println("==> Entered Schema Validation " + workflow_id)
      //fork(schema_validation_workflow(workflow_id, inputDF), next_step_id, workflow_id)
       exception_handling(schema_validation_workflow, inputDF, workflow_id, next_step_id, exception_wf_id)
    } else if (op_type.equalsIgnoreCase("file migration") /*&& !inputDF.schema.isEmpty*/ ) {
      println("==> Entered File Migration " + workflow_id)
      //fork(file_migration_workflow(workflow_id, inputDF), next_step_id, workflow_id)
      exception_handling(file_migration_workflow, inputDF, workflow_id, next_step_id, exception_wf_id)
    } else if (op_type.equalsIgnoreCase("FileRename") /*&& !inputDF.schema.isEmpty*/ ) {
      println("==> Entered FileRename " + workflow_id)
      inputDF.cache.count
      //fork(FileRename.rename_files(workflow_id, inputDF), next_step_id, workflow_id)
      exception_handling(FileRename.rename_files, inputDF, workflow_id, next_step_id, exception_wf_id)
    } else {
      ArrayBuffer((1L, 1L, 1L, 1L, ""))
    }
  }

  def filter(workflow_id: Long, inputDF: DataFrame): DataFrame = {
    var df = spark.emptyDataFrame
    var isSuccess = !inputDF.schema.isEmpty
    if (isSuccess) {
      var row = workflow_filter_condition_array.filter(x => x.getAs("WORKFLOW_ID").toString.toLong == workflow_id)(0)
      var filter_condition = row.getAs[String]("FILTER_CONDITION")
      var check_empty_dataframe = row.getAs[String]("CHECK_EMPTY_DATAFRAME").equalsIgnoreCase("Y")
      var streaming_interval = row.getAs[String]("STREAMING_INTERVAL")
      var delta = row.getAs[String]("DELTA")
      var timestamp_col_name = row.getAs[String]("TIMESTAMP_COL_NAME")
      var timestamp_format = row.getAs[String]("TIMESTAMP_FORMAT")
      var insert_timestamp_col_name = if (row.getAs[String]("INSERT_TIMESTAMP_COL_NAME").equals("")) "insert_ts" else row.getAs[String]("INSERT_TIMESTAMP_COL_NAME")
      var insert_timestamp_format = if (row.getAs[String]("INSERT_TIMESTAMP_FORMAT").equals("")) "yyyy-MM-dd HH:mm:ss" else row.getAs[String]("INSERT_TIMESTAMP_FORMAT")
      var filter_cond = ""
      var isSuccess = true
      filter_condition=com.jio.pulse.plugins.Loop.loop_variables_replace(filter_condition)
      if (!streaming_interval.equals("") && !delta.equals("")) {
        if (!filter_condition.equals("")) {
          filter_cond = "(" + s"$filter_condition and (((unix_timestamp($timestamp_col_name,'$timestamp_format') < ${batchTime.batch_time} - $delta) and (unix_timestamp($timestamp_col_name,'$timestamp_format') >= ${batchTime.batch_time} - $delta - $streaming_interval) and ((${batchTime.batch_time} - $delta) % $streaming_interval) = 0) or (unix_timestamp($timestamp_col_name,'$timestamp_format') < cast(((${batchTime.batch_time} - $streaming_interval - $delta)/$streaming_interval) as int) * $streaming_interval and unix_timestamp($insert_timestamp_col_name,'$insert_timestamp_format') > unix_timestamp('${maxRecordedTime.max_rec_time}')))" + ")"
        } else {
          filter_cond = "(" + s"(((unix_timestamp($timestamp_col_name,'$timestamp_format') < ${batchTime.batch_time} - $delta) and (unix_timestamp($timestamp_col_name,'$timestamp_format') >= ${batchTime.batch_time} - $delta - $streaming_interval) and ((${batchTime.batch_time} - $delta) % $streaming_interval) = 0) or (unix_timestamp($timestamp_col_name,'$timestamp_format') < cast(((${batchTime.batch_time} - $streaming_interval - $delta)/$streaming_interval) as int) * $streaming_interval and unix_timestamp($insert_timestamp_col_name,'$insert_timestamp_format') > unix_timestamp('${maxRecordedTime.max_rec_time}')))" + ")"
        }
        /*println(filter_cond)
        isSuccess = Try { df = inputDF.filter(filter_cond) }.isSuccess
        if (!isSuccess) {
          println(s"===> Incorrect or Non Applicable Filter Condition: $filter_condition")
          df = spark.emptyDataFrame
        }*/
        try {
          df = inputDF.filter(filter_cond)
        } catch {
          case ex: Exception => {
            println(s"====> Error occured while applying filter condition: $filter_cond")
            println("====> Error Message: \n" + ex.getMessage)
          }
        }
      } else {
        /*isSuccess = Try { df = if (!filter_condition.equals("")) inputDF.filter(filter_condition) else inputDF }.isSuccess
        if (!isSuccess) {
          println(s"===> Incorrect or Non Applicable Filter Condition: $filter_condition")
          df = spark.emptyDataFrame
        }*/
        try {
          df = if (!filter_condition.equals("")) inputDF.filter(filter_condition) else inputDF
        } catch {
          case ex: Exception => {
            println(s"====> Error occured while applying filter condition: $filter_cond")
            println("====> Error Message: \n" + ex.getMessage)
          }
        }
      }
      //df.cache
      if (check_empty_dataframe) {
        if (df.head(1).isEmpty) df = spark.emptyDataFrame
      }
    }
    if (configuration.getBoolean("debug.filter.operation")) {
      if (configuration.getBoolean("debug.filter.show.input")) operation_debug("filter", workflow_id, df, inputDF)
      else operation_debug("filter", workflow_id, df)
    }
    df
  }

  def union(inpdf1: DataFrame, inpdf2: DataFrame): DataFrame = {
    var finalColumns: Set[String] = Set()
    var openMissingReqCols: Array[Column] = Array[Column]()
    var df1_columns = inpdf1.columns.map(_.toLowerCase())
    var df2_columns = inpdf2.columns.map(_.toLowerCase())
    var df1 = inpdf1.select(df1_columns.map(x => col("`" + x + "`").as(x.toLowerCase())): _*)
    var df2 = inpdf2.select(df2_columns.map(x => col("`" + x + "`").as(x.toLowerCase())): _*)
    var typemap1: Map[String, String] = Map()
    if (!df1.schema.isEmpty && !df2.schema.isEmpty) {
      df1_columns.foreach { column =>
        var typename = df1.schema(column).dataType.typeName
        if (!typename.equalsIgnoreCase("null") && !typename.equalsIgnoreCase("void")) {
          typemap1 = typemap1 ++ Map(column -> typename)
        }
      }
      var typemap2: Map[String, String] = Map()
      df2_columns.foreach { column =>
        var typename = df2.schema(column).dataType.typeName
        if (!typename.equalsIgnoreCase("null") && !typename.equalsIgnoreCase("void")) {
          typemap2 = typemap2 ++ Map(column -> typename)
        }
      }

      finalColumns = df1_columns.toSet ++ df2_columns.toSet
      var finaldf = finalColumns.toList.foldLeft(spark.emptyDataFrame)((a, b) => a.withColumn(b, lit("anyStringValue")))
      Array(df1, df2).foreach { df =>
        openMissingReqCols = finalColumns.toArray.filterNot(df.columns.toSet).map(f => lit(null).as(f.toString))
        finaldf = finaldf.unionByName(df.select(col("*") +: openMissingReqCols: _*))
      }

      finaldf.columns.foreach { column =>
        if (typemap1.contains(column)) {
          finaldf = finaldf.withColumn(column, col("`" + column + "`").cast(typemap1(column)))
        } else if (typemap2.contains(column)) {
          finaldf = finaldf.withColumn(column, col("`" + column + "`").cast(typemap2(column)))
        }
      }
      if (configuration.getBoolean("debug.union.operation")) {
        operation_debug("union", workflow_id, finaldf)
      }
      finaldf
    } else if (df1.schema.isEmpty) {
      if (configuration.getBoolean("debug.union.operation")) {
        operation_debug("union", workflow_id, df2)
      }
      df2
    } else {
      if (configuration.getBoolean("debug.union.operation")) {
        operation_debug("union", workflow_id, df1)
      }
      df1
    }
  }

  def join(df1: (DataFrame, Long), df2: (DataFrame, Long), workflow_id: Long): (DataFrame, Long) = {
    var df = spark.emptyDataFrame
    var ldf = spark.emptyDataFrame
    var rdf = spark.emptyDataFrame
    var wfid = 0L

    var join_details = join_array.filter(x => x.getAs("WORKFLOW_ID").toString.toLong == workflow_id)(0)
    var join_condition = join_details.getAs[String]("JOIN_EXPRESSION")
    var join_type = join_details.getAs[String]("JOIN_TYPE")
    var drop_columns = join_details.getAs[String]("DROP_COLUMNS").split(",").map(c => col(c))
    var additional_columns = join_details.getAs[String]("ADDITIONAL_COLUMN_EXPRESSION")
    var output_columns_datatype = join_details.getAs[String]("OUTPUT_COLUMN_DATATYPE")
    var left_table_join_columns = join_details.getAs[String]("LEFT_TABLE_JOIN_COLUMNS")
    var right_table_join_columns = join_details.getAs[String]("RIGHT_TABLE_JOIN_COLUMNS")
    var columns_rename = join_details.getAs[String]("COLUMN_RENAME")
    var left_wfid = join_details.getAs("LEFT_WORKFLOW_ID").toString().toInt

    if (df1._2 == left_wfid) {
      ldf = df1._1
      rdf = df2._1
      wfid = df1._2
    } else {
      ldf = df2._1
      rdf = df1._1
      wfid = df2._2
    }

    var isSuccess = left_table_join_columns.split(",").map(_.toLowerCase()).toSet.subsetOf(ldf.columns.map(_.toLowerCase()).toSet) && right_table_join_columns.split(",").map(_.toLowerCase()).toSet.subsetOf(rdf.columns.map(_.toLowerCase()).toSet)  || (left_table_join_columns.isEmpty && right_table_join_columns.isEmpty)

    if (isSuccess) {
      df = ldf.as("a").join(rdf.as("b"), expr(join_condition), join_type)
      drop_columns.foreach(column => df = df.drop(column))
      if (!additional_columns.equals("") && !df.columns.contains(additional_columns.split(";")(0))) {
        additional_columns.split("#").foreach { value => df = df.withColumn(value.split(";")(0), expr(value.split(";")(1))) }
      }
      if (!output_columns_datatype.equals("")) {
        output_columns_datatype.split(",").foreach { dtype => df = df.withColumn(dtype.split(":")(0), col("`" + dtype.split(":")(0) + "`").cast(dtype.split(":")(1))) }
      }
      if (!columns_rename.equals("")) {
        columns_rename.split(",").foreach { column => df = df.withColumnRenamed(column.split(":")(0), column.split(":")(1)) }
      }
    } else {
      println("Join Columns do not exist")
    }
    if (configuration.getBoolean("debug.join.operation")) {
      operation_debug("join", workflow_id, df)
    }
    (df, wfid)
  }
    def explodeDF2(workflow_id: Long, MasterDF: DataFrame): DataFrame = {
    var df = MasterDF
    var isSuccess = !df.schema.isEmpty
    if (isSuccess) {
      var rows = coalesce_explode_array.filter(x => x.getAs("WORKFLOW_ID").toString.toInt == workflow_id)
      if (rows.length > 0) {
        var select_columns = rows(0).getAs[String]("SELECTCOLUMNS").split(",")
        var key_column_name = rows(0).getAs[String]("EXPLODE_KEY_COLUMN_NAME")
        var value_column_name = rows(0).getAs[String]("EXPLODE_VALUE_COLUMN_NAME")
        var explode_columns = if (rows.length == 1) {
          df.columns.map(_.toLowerCase()).toSet.diff(select_columns.map(_.toLowerCase()).toSet).toArray
        } else {
          rows.map(_.getAs[String]("COLUMNNAME"))
        }
        var query = ""
        var stack_input = ""
    val fixedColumns=select_columns.toSeq
   df=dataframeToKeyValuePairsWithFixedColumns(df,fixedColumns,key_column_name,value_column_name)
      }
      if (configuration.getBoolean("debug.explode.operation")) {
        if (configuration.getBoolean("debug.explode.show.input")) operation_debug("explode", workflow_id, df, MasterDF)
        else operation_debug("explode", workflow_id, df)
      }
    }
    df
  }
    def dataframeToKeyValuePairsWithFixedColumns(df: DataFrame, fixedColumns: Seq[String],key_column_name: String,value_column_name: String): DataFrame = {
    import df.sparkSession.implicits._
    // Get columns to be transformed to key-value pairs, excluding the fixed columns
    val keyValueColumns = df.columns.filterNot(fixedColumns.contains)
    // Create an array of structs with key-value pairs
    //val kvArray = array(keyValueColumns.map(c => struct(lit(c).as("key"), col(c).as("value"))): _*)
    val kvArray = array(keyValueColumns.map(c => struct(lit(c).as(key_column_name), col("`"+c+"`").cast("string").as(value_column_name))): _*)
    // Add the array of key-value pairs to the DataFrame
    val dfWithKVArray = df.withColumn("kvArray", kvArray)
    // Explode the array to create a row for each key-value pair
    val explodedDF = dfWithKVArray.withColumn("kv", explode(col("kvArray")))
     .filter("kv."+value_column_name+" is not  null ")
      .select(fixedColumns.map(col) :+ col("kv."+key_column_name) :+ col("kv."+value_column_name): _*)
    explodedDF
  }

  def explodeDF(workflow_id: Long, MasterDF: DataFrame): DataFrame = {
    var df = MasterDF
    var isSuccess = !df.schema.isEmpty
    if (isSuccess) {
      df.createOrReplaceTempView("input_table")
      var rows = coalesce_explode_array.filter(x => x.getAs("WORKFLOW_ID").toString.toInt == workflow_id)
      if (rows.length > 0) {
        var select_columns = rows(0).getAs[String]("SELECTCOLUMNS").split(",")
        var key_column_name = rows(0).getAs[String]("EXPLODE_KEY_COLUMN_NAME")
        var value_column_name = rows(0).getAs[String]("EXPLODE_VALUE_COLUMN_NAME")
        var explode_columns = if (rows.length == 1) {
          df.columns.map(_.toLowerCase()).toSet.diff(select_columns.map(_.toLowerCase()).toSet).toArray
        } else {
          rows.map(_.getAs[String]("COLUMNNAME"))
        }
        /*.map(x => "`"+x+"`")*/
        var explode_columns_alias = if (rows.length == 1) {
          var alias = rows(0).getAs[String]("EXPLODE_KEY_POSTFIX")
          explode_columns.map(_ + alias).map(x => x.replace("\\", "\\\\").replace("'", "\\'"))
        } else {
          rows.map(x => x.getAs[String]("COLUMNNAME") + x.getAs[String]("EXPLODE_KEY_POSTFIX_")).map(x => x.replace("\\", "\\\\").replace("'", "\\'"))
        }
        var query = ""
        var stack_input = ""
        val number_of_columns = explode_columns.size
        if (number_of_columns > 0) {
          explode_columns.zip(explode_columns_alias).foreach { column =>
            stack_input += s"'${column._2}',cast(`${column._1}` as string),"
          }
          stack_input = number_of_columns + "," + stack_input.substring(0, stack_input.lastIndexOf(","))
          val select_columns_ = select_columns.map(x => "`" + x + "`").mkString(",")
          query = s"select $select_columns_,stack($stack_input) ($key_column_name,$value_column_name) from input_table"
          println(query)
          df = spark.sql(query).filter(s"$value_column_name is not null")
        }
      }
      if (configuration.getBoolean("debug.explode.operation")) {
        if (configuration.getBoolean("debug.explode.show.input")) operation_debug("explode", workflow_id, df, MasterDF)
        else operation_debug("explode", workflow_id, df)
      }
    }
    df
  }

  def coalesceDF(workflow_id: Long, MasterDF: DataFrame): DataFrame = {
    var df = MasterDF
    df.createOrReplaceTempView("input_table")
    var rows = coalesce_explode_array.filter(x => x.getAs("WORKFLOW_ID").toString.toLong == workflow_id)
    var select_columns = rows(0).getAs[String]("SELECTCOLUMNS")
    var partitionby_columns = rows(0).getAs[String]("PARTITIONCOLUMNS").split(",").map(x => "`" + x + "`").mkString(",")
    var coalesce_all = rows(0).getAs[String]("COALESCE_ALL").equalsIgnoreCase("Y")
    var coalesce_alias = rows(0).getAs[String]("COALESCE_ALIAS")
    var query = ""

    if (rows.length == 1 && rows(0).getAs[String]("COLUMNNAME").equals("")) {
      //println(df.columns.map(_.toLowerCase()).toSet.diff(select_columns.split(",").map(_.toLowerCase()).toSet))
      df.columns.map(_.toLowerCase()).toSet.diff(select_columns.split(",").map(_.toLowerCase()).toSet).toArray.foreach { column =>
        var column_ = column + coalesce_alias
        query += s"max(`$column`) over (partition by $partitionby_columns) `$column_`, "
      }
    } else {
      if (coalesce_all) {
        var columns_special = rows.map(_.getAs[String]("COLUMNNAME"))
        df.columns.map(_.toLowerCase()).toSet.diff(select_columns.split(",").map(_.toLowerCase()).toSet).diff(columns_special.map(_.toLowerCase()).toSet).toArray.foreach { column =>
          var column_ = column + coalesce_alias
          query += s"max(`$column`) over (partition by $partitionby_columns) `$column_`, "
        }
        rows.filter(_.getAs[String]("COALESCE_DISCARD").equalsIgnoreCase("N")).foreach { row =>
          var column = row.getAs[String]("COLUMNNAME")
          var alias = column + row.getAs[String]("COALESCE_ALIAS_")
          var function = row.getAs[String]("COALESCE_AGG_FUNCTION")
          var orderby = row.getAs[String]("COALESCE_ORDER_BY")
          var orderby_clause = if (!orderby.equals("")) {
            s"order by $orderby ${row.getAs[String]("COALESCE_ORDER_ASC_DESC")}"
          } else ""
          query += s"$function(`$column`) over (partition by $partitionby_columns $orderby_clause) `$alias`, "
        }
      } else {
        rows.filter(_.getAs[String]("COALESCE_DISCARD").equalsIgnoreCase("N")).foreach { row =>
          var column = row.getAs[String]("COLUMNNAME")
          var alias = column + row.getAs[String]("COALESCE_ALIAS_")
          var function = row.getAs[String]("COALESCE_AGG_FUNCTION")
          var orderby = row.getAs[String]("COALESCE_ORDER_BY")
          var orderby_clause = if (!orderby.equals("")) {
            s"order by $orderby ${row.getAs[String]("COALESCE_ORDER_ASC_DESC")}"
          } else ""
          query += s"$function(`$column`) over (partition by $partitionby_columns $orderby_clause) `$alias`, "
        }
      }
    }

    select_columns = select_columns.split(",").map(x => "`" + x + "`").mkString(",")
    query = query.substring(0, query.lastIndexOf(","))
    query = s"select $select_columns, $query from input_table"
    println(query)

    df = spark.sql(query).distinct

    if (configuration.getBoolean("debug.coalesce.operation")) {
      if (configuration.getBoolean("debug.coalesce.show.input")) operation_debug("coalesce", workflow_id, df, MasterDF)
      else operation_debug("coalesce", workflow_id, df)
    }
    df
  }

  def readBatch(workflow_id: Long, inputDF: DataFrame): DataFrame = {
    var df = spark.emptyDataFrame
    var readDetails = workflow_array.filter(x => x.getAs("workflow_id").toString.toLong == workflow_id)(0)
    var op_sub_type = readDetails.getAs[String]("subtype")
    if (op_sub_type.equalsIgnoreCase("api")) {
      var api_read = api_read_array.filter(x => x.getAs("WORKFLOW_ID").toString.toLong == workflow_id)(0)
      var api_id = api_read.getAs("API_ID").toString.toInt
      df = FetchApiData(api_id, inputDF)
    } else {
      var connection_string_id = readDetails.getAs("connection_string_id").toString.toLong
      var read_write_id = readDetails.getAs("read_write_id").toString.toLong
      var ConnectionDetails: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
      var ReadDetails: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
      connection_array.filter(x => x.getAs("connection_string_id").toString.toLong == connection_string_id && x.getAs("is_column").toString.equalsIgnoreCase("N")).foreach(row => ConnectionDetails = ConnectionDetails ++ Map(row.getAs[String]("key") -> row.getAs[String]("value")))
      read_write_array.filter(x => x.getAs("read_write_id").toString.toLong == read_write_id && x.getAs("is_column").toString.equalsIgnoreCase("N")).foreach(row => ReadDetails = ReadDetails ++ Map(row.getAs[String]("key") -> row.getAs[String]("value")))
      var attribute_map_N: scala.collection.mutable.Map[String, Any] = ConnectionDetails ++ ReadDetails

      /**
       * attibute_map_N is for is_column=N
       */
      ConnectionDetails.clear()
      ReadDetails.clear()
      connection_array.filter(x => x.getAs("connection_string_id").toString.toLong == connection_string_id && x.getAs("is_column").toString.equalsIgnoreCase("Y")).foreach(row => ConnectionDetails = ConnectionDetails ++ Map(row.getAs[String]("key") -> row.getAs[String]("value")))
      read_write_array.filter(x => x.getAs("read_write_id").toString.toLong == read_write_id && x.getAs("is_column").toString.equalsIgnoreCase("Y")).foreach(row => ReadDetails = ReadDetails ++ Map(row.getAs[String]("key") -> row.getAs[String]("value")))
      var attribute_map_Y = ConnectionDetails ++ ReadDetails
      /**
       * attibute_map_Y is for is_column=Y
       */
      if (attribute_map_Y.isEmpty) {
        df = read(op_sub_type, (attribute_map_N))._1
      } else {
        var columns_list = attribute_map_Y.map(x => x._2.toString).toList
        var base_filter_cond = ""
        columns_list.foreach { column =>
          base_filter_cond += column + " is not null and "
        }
        base_filter_cond = base_filter_cond.substring(0, base_filter_cond.lastIndexOf(" and "))
        val temp_inpdf = inputDF.filter(base_filter_cond)
        temp_inpdf.cache()
        var distinct_attribute_rows = temp_inpdf.select(columns_list.map(c => col(c)): _*).distinct.collect
        var dfArray: Array[DataFrame] = Array()
        distinct_attribute_rows.foreach { row =>
          attribute_map_Y.foreach { x =>
            attribute_map_N = attribute_map_N ++ Map(x._1 -> row.getAs[String](x._2))
          }
          dfArray = dfArray :+ read(op_sub_type, (attribute_map_N))._1
        }
        df = dfUnion(dfArray)
        temp_inpdf.unpersist()
      }
    }
    df = addEnrichFields(df, enrich_fields_values)
    if (configuration.getBoolean("debug.read.operation")) {
      operation_debug("Read", workflow_id, df)
    }
    df
  }

  /*
   * This dynamicEnrichment funcition is an overloaded method. used only for API Enrichments
   */
  def dynamicEnrichment(apiId: Long, outputDS: Dataset[Row], enrichment_df: Dataset[Row], base_id: Long): Dataset[Row] = {
    //println("Entered Recurrsion with API_ID = " + apiId)
    var isRecurssion: Boolean = false
    var df = outputDS
    var extract_columns_arr: Array[String] = Array()
    var enrichmentid = 0
    var status = ""
    var extract_columns = ""
    var master_join_columns = ""
    var enrich_join_columns = ""
    var join_expression = ""
    var join_type = ""
    var drop_columns = ""
    var additional_columns = ""
    var output_columns_datatype = ""
    var columns_rename = ""
    var isSuccess: Boolean = !df.schema.isEmpty
    var enrich_table = ""
    var enrichment_cols: Array[String] = Array()
    var rows = enrichment_df.filter("DEP_API_ID=" + apiId).orderBy("SEQUENCE").na.fill("").na.fill(0).collect
    rows.foreach { row =>
      //status = if (row.getAs("status") != null) row.getAs("status") else ""
      extract_columns = if (row.getAs("extract_columns") != null) row.getAs("extract_columns") else ""
      master_join_columns = if (row.getAs("master_table_join_columns") != null) row.getAs("master_table_join_columns") else ""
      enrich_join_columns = if (row.getAs("enrich_table_join_columns") != null) row.getAs("enrich_table_join_columns") else ""
      join_type = if (row.getAs("join_type") != null) row.getAs("join_type") else ""
      additional_columns = row.getAs("additional_column_expression")
      output_columns_datatype = if (row.getAs("output_column_datatype") != null) row.getAs("output_column_datatype") else ""
      drop_columns = if (row.getAs("drop_columns") != null) row.getAs("drop_columns") else ""
      join_expression = if (row.getAs("join_condition_expression") != null) row.getAs("join_condition_expression") else ""
      columns_rename = if (row.getAs("column_rename") != null) row.getAs("column_rename") else ""
      val independent_api_Id = row.getAs("indep_api_id").toString.toLong
      val dependent_api_id = row.getAs("dep_api_id").toString.toLong
      val independent_query_Id = row.getAs("indep_query_id").toString.toLong

      //val pulse_input_query = row.getAs[Array[Byte]]("query")
      val pulse_input_query = row.getAs[String]("query")
      val connection_string_id = row.getAs("connection_string_id").toString.toLong

      if (independent_api_Id != null && independent_api_Id > 0) {
        if (enrichment_df.filter("DEP_API_ID=" + independent_api_Id).count() > 0) {
          df = dynamicEnrichment(independent_api_Id, df, enrichment_df, base_id).withColumn("independent_api_Id", lit(independent_api_Id))
          isRecurssion = true
        }

        //println("====> df before join")
        //df.show
        var pulseapi: DataFrame = spark.emptyDataFrame
        if (!df.schema.isEmpty && isRecurssion) {
          pulseapi = apiDataset.filter("api_id=" + independent_api_Id).join(df, col("api_id") === col("independent_api_Id"), "left")
        } else {
          pulseapi = apiDataset.filter("api_id=" + independent_api_Id)
        }

        /*println("====> pulseapi")
        pulseapi.show*/

        pulseapi = pulseapi.select(pulseapi.columns.map(x => col(x).as(x.toLowerCase)): _*)

        pulseapi = pulseapi.withColumn("response", callUDF("ApiCallingFunction", struct("*")))
          .withColumn("response", explode_outer(col("response")))
          .withColumn("is_success", col("response._1"))
          .withColumn("events", col("response._2"))
          .drop("response")
          .cache

        var parseDF = genericJsonParsing(-1, spark.read.json(pulseapi.select("events").as(Encoders.STRING)), independent_api_Id.toInt, -1)

        //spark.read.json(pulseapi.select("events").as[String])
        //println("====> Independent API ID - " + independent_api_Id.toInt)

        //val schema = spark.read.json(pulseapi.select("events").as[String]).schema

        /*var parseDF = genericJsonParsing(-1,
          pulseapi.withColumn("events", from_json(col("events"), schema)).
          select(col("events.*")), independent_api_Id.toInt, -1)*/

        /*println("=====> parseDF after parsing")
        parseDF.show*/

        //parseDF = parseDF.withColumn("base_url_id", lit(base_id))

        if (!df.schema.isEmpty && !parseDF.schema.isEmpty && !isRecurssion) {
          extract_columns_arr = extract_columns.split(",").map(x => x.split("\\s+")(0))
          enrichment_cols = parseDF.columns

          /*println("Inside api")
          println(master_join_columns.split(",").map(_.toLowerCase).toSet)
          println(df.columns.map(_.toLowerCase).toSet)
          println(enrich_join_columns.split(",").map(_.toLowerCase).toSet)
          println(enrichment_cols.map(_.toLowerCase).toSet)*/

          if ((master_join_columns.split(",").map(_.toLowerCase).toSet subsetOf df.columns.map(_.toLowerCase).toSet)
            && (enrich_join_columns.split(",").map(_.toLowerCase).toSet subsetOf enrichment_cols.map(_.toLowerCase).toSet)) {

            df.createOrReplaceTempView("input_table")
            parseDF.createOrReplaceTempView("enrich_table")
            //println("entered conditional join")

            df = spark.sql(s"select /*+ Broadcast(enrich) */* from input_table master $join_type join enrich_table enrich on $join_expression")
          } else {
            //println("entered cross join")
            df = df.join(parseDF.withColumn("base_url_id", lit(base_id)), Seq("base_url_id"), "inner")
          }

        } else {
          //println("entered neither joins")
          df = parseDF.withColumn("base_url_id", lit(base_id))
        }
      } else if (pulse_input_query != null) {
        var ConnectionDetails: Map[String, String] = Map()
        //println(connection_string_id)
        connection_array.filter(x => x.getAs("connection_string_id").toString.toLong == connection_string_id).foreach(row => ConnectionDetails = ConnectionDetails ++ Map(row.getAs[String]("key") -> row.getAs[String]("value")))
        //println(ConnectionDetails)
        var url = ConnectionDetails("url")
        var username = ConnectionDetails("userid")
        var password = com.jio.encrypt.DES3.decryptKey(ConnectionDetails("password"))
        var driver = ConnectionDetails("driver")
        var ipaddress = ConnectionDetails("ipaddress")
        var port = ConnectionDetails("port")
        var database = ConnectionDetails("database")
        //driver = "oracle.jdbc.driver.OracleDriver"
        //url = s"jdbc:oracle:thin:@$ipaddress:$port/$database"
        //ApiUDF.callProxy()
        var parseDF = spark.read.format("jdbc").option("url", url).option("user", username).option("password", password).option("driver", driver).option("dbtable", s"($pulse_input_query) input").load
        //ApiUDF.clearProxy()
        //parseDF = parseDF.withColumn("base_url_id", lit(base_id))

        //parseDF.show

        //df.show
        //println("==> Before CMS")
        //parseDF.show
        parseDF = getCMSCredentials(parseDF).drop("cms_att_name")
        //println("==> After CMS")
        //parseDF.show

        if (!df.schema.isEmpty && !parseDF.schema.isEmpty) {
          extract_columns_arr = extract_columns.split(",").map(x => x.split("\\s+")(0))
          enrichment_cols = parseDF.columns

          /*println("Inside query")
          println(master_join_columns.split(",").map(_.toLowerCase).toSet)
          println(df.columns.map(_.toLowerCase).toSet)
          println(enrich_join_columns.split(",").map(_.toLowerCase).toSet)
          println(enrichment_cols.map(_.toLowerCase).toSet)*/

          if ((master_join_columns.split(",").map(_.toLowerCase).toSet subsetOf df.columns.map(_.toLowerCase).toSet)
            && (enrich_join_columns.split(",").map(_.toLowerCase).toSet subsetOf enrichment_cols.map(_.toLowerCase).toSet)) {

            df.createOrReplaceTempView("input_table")
            parseDF.createOrReplaceTempView("enrich_table")

            df = spark.sql(s"select /*+ Broadcast(enrich) */* from input_table master $join_type join enrich_table enrich on $join_expression")

            /*println("df after enrichment")
            df.show*/
          } else {
            df = df.join(parseDF.withColumn("base_url_id", lit(base_id)), Seq("base_url_id"), "inner")
          }

        } else {
          df = parseDF.withColumn("base_url_id", lit(base_id))
        }
      }
    }
    //println("Exiting Recurssion with API_ID = " + apiId)
    df.select(df.columns.map(x => col(x).as(x.toLowerCase)): _*)
  }

    def FetchApiData(api_id: Long, inputDF: DataFrame): DataFrame = {
    var endf = inputDF
    if (!inputDF.schema.isEmpty) {
      endf = endf.withColumn("base_url_id", lit(api_id))
    }
    var api_Dataset = dynamicEnrichment(api_id, endf, dep_df, api_id)

    //println("======> api_Dataset after all the enrichments")
    //api_Dataset.show

    if (!api_Dataset.schema.isEmpty) {
      api_Dataset = api_Dataset.withColumn("base_url_id", lit(api_id)).join(apiDataset, col("api_id") === col("base_url_id"), "left")
    } else {
      api_Dataset = apiDataset.filter("api_id=" + api_id)
    }
    //api_Dataset.show
    //getCMSCredentials(parseDF)
    api_Dataset = api_Dataset.select(api_Dataset.columns.map(x => col(x).as(x.toLowerCase)): _*)
    api_Dataset = getCMSCredentials(api_Dataset).drop("cms_att_name")
    var input_df: DataFrame = spark.emptyDataFrame

    val api_Dataset1 = api_Dataset.repartition(20)
    api_Dataset1.cache
    val dfcount = api_Dataset1.count
    println("Total API Calls: " + dfcount)
    //api_Dataset1.show

    if (dfcount > 0) {
      val api_config_row = api_Dataset1.head

      var isDownload: Boolean = false
      if (api_config_row.schema.fieldNames.contains("download")) {
        isDownload = if (api_config_row.getAs[String]("download") != null) api_config_row.getAs[String]("download").equalsIgnoreCase("true") else false
      }

      if (isDownload) {
        println("Entered download")
         var dflist = new ListBuffer[DataFrame]()
         api_Dataset1.collect().foreach(api_config_row => {
          val result = com.jio.pulse.ApiUDF.triggerApiDownload(spark, api_config_row)
          dflist += ApiUtils.updateAdditionalColumn(result._2, api_config_row)
           println("end updateAdditionalColumn")
        })

        /**
         * union all data frames after download each of the data
         */
        input_df = dfUnion(dflist.toArray)
       
      } else {
        val api_Dataset2 = api_Dataset1
          .withColumn("response", callUDF("ApiCallingFunction", struct("*")))
          .withColumn("response", explode_outer(col("response")))
          .withColumn("is_success", col("response._1"))
          .withColumn("events", col("response._2"))
          .drop("response")
          .cache

        //api_Dataset2.show()
        val appid = spark.sparkContext.getConf.get("spark.app.id")
        val appname = spark.sparkContext.getConf.get("spark.app.name")
        val api_error_df = api_Dataset2.filter("is_success='false'").select("events")
          .withColumn("Technology", lit("Application"))
          .withColumn("SubTech", lit("Application"))
          .withColumn("appId", lit(appid))
          .withColumn("appName", lit(appname))
          .withColumn("insert_timestamp", expr("from_unixtime(unix_timestamp(current_timestamp,'yyyy-MM-dd HH:mm:ss'),'dd-MMM-yyyy hh:mm.ss a')"))
          .withColumnRenamed("events", "error")
          .withColumn("errorData", col("error"))
          .withColumn("type", lit("error"))
          .withColumn("job_instance_id", lit(job_instance_id))

        val api_error_kafka_map = healthmonitor_kafka_ssl_map ++ Map("kafka.bootstrap.servers" -> healthmonitor_kafka_bootstrapserver, "topic" -> healthmonitor_kafka_topic)
        api_error_df.toJSON.toDF("value").write.format("kafka").options(api_error_kafka_map).save()

        val api_success_df = api_Dataset2.filter("is_success='true'")

        val final_data = api_success_df.select("events")
        //final_data.show()

        ApiUtils.updateDataInOracle(api_success_df)

        val schema = spark.read.json(final_data.as(Encoders.STRING)).schema
        
        input_df = final_data.withColumn("events", from_json(col("events"), schema)).select(col("events.*"))
         .cache()

        /**
         * head function need for persist data before sending to next work flow 
         */
        input_df.head()
        api_Dataset2.unpersist()
      }
    }
    input_df
  }

  def getCMSCred(cms_url: String, cms_user: String, cms_pass: String, cms_aes_iv: String, cms_aes_key: String) = {
    val cms_inst = CMS(cms_url, cms_user, cms_pass, cms_aes_iv, cms_aes_key)
    // 2 arguements here also - (uuid, encryp)
    udf((uuid: String, encryption_type: String) => {
      // here also 2 - (u, e)
      cms_inst.getCredential(uuid, encryption_type)
    })
  }

  def getCMSCredentials(inpdf: DataFrame): DataFrame = {
    var df = inpdf
    df = df.select(df.columns.map(x => col(x).as(x.toLowerCase)): _*)
    val fieldnames = df.schema.fieldNames.map(_.toLowerCase)
    if (fieldnames.contains("cms_att_name")) {
      val encryption_type = if (fieldnames.contains("cms_encr_type")) df.head.getAs[String]("cms_encr_type") else "base64"
      val cms_columns = df.head.getAs[String]("cms_att_name")
      if (cms_columns != null) {
        cms_columns.split(",").foreach { column =>
          // 2 columns will be passed - uuid, encry_type
          df = df.withColumn(column, getCMSCred(cms_url, cms_user, cms_pass, cms_aes_iv, cms_aes_key)(col(column), lit(encryption_type)))
        }
      }
    }
    df
  }

  def pivotDF(workflow_id: Long, MasterDF: DataFrame): DataFrame = {
    var df = MasterDF
    val isSuccess = !df.schema.isEmpty
    if (isSuccess) {
      var rows = pivot_array.filter(x => x.getAs("WORKFLOW_ID").toString.toLong == workflow_id)
      scala.util.Sorting.stableSort(rows, (e1: Row, e2: Row) => e1.getAs("SEQUENCE").toString.toInt < e2.getAs("SEQUENCE").toString.toInt)
      val distinct_seq = rows.map(x => x.getAs("SEQUENCE").toString.toInt).distinct
      //df.cache
      var df_ = df //.repartition(10)
      distinct_seq.foreach { seq =>
        df_.createOrReplaceTempView("input_table")
        val pivot_conf_arr = rows.filter(x => x.getAs("SEQUENCE").toString.toInt == seq)
        val pivot_conf = pivot_conf_arr.head
        val agg_cols = pivot_conf_arr.map(x => x.getAs[String]("AGG_COL"))
        val pivot_col = pivot_conf.getAs[String]("PIVOT_COL")
        val grp_cols = pivot_conf.getAs[String]("GROUP_BY_COLS")
        val req_cols = agg_cols.map(_.toLowerCase()) :+ pivot_col.toLowerCase()
        val req_cols_exits: Boolean = req_cols.toSet subsetOf df_.columns.map(_.toLowerCase).toSet
        if (req_cols_exits) {
          var grp_cls = ""
          if (grp_cols.equals("*")) {
            grp_cls = (df_.columns.toSet diff Set(pivot_col) diff agg_cols.toSet).map(x => "`" + x + "`").mkString(",")
          } else {
            grp_cls = grp_cols.split(",").map(x => "`" + x + "`").mkString(",")
          }
          val distinct_keys = df_.select(pivot_col).distinct.collect.filter(x => x(0) != null).map(x => x(0).toString)
          if (distinct_keys.size > 0) {
            var select_query = ""
            var agg_query = ""
            pivot_conf_arr.foreach { data =>
              val agg_col = data.getAs[String]("AGG_COL")
              val agg_alias = data.getAs[String]("COL_ALIAS")
              val agg_func = data.getAs[String]("AGG_FUNCTION")

              distinct_keys.foreach { key =>
                select_query += s"case when `$pivot_col`='$key' then `$agg_col` else null end as `$key$agg_alias`, "
                agg_query += s"$agg_func(`$key$agg_alias`) over (partition by $grp_cls) as `$key$agg_alias`, "
              }
            }

            select_query = s"select $grp_cls, " + select_query.substring(0, select_query.lastIndexOf(",")) + " from input_table"
            agg_query = s"select $grp_cls, " + agg_query.substring(0, agg_query.lastIndexOf(",")) + " from input_table"

            //println(select_query)
            //println(agg_query)
            spark.sql(select_query).createOrReplaceTempView("input_table")
            df_ = spark.sql(agg_query).distinct
          } else {
            println(s"===> Error: Pivot operation: All the values in the column $pivot_col are null values. Hence, not performing pivot corresponding to this configuration.")
          }
        } else {
          println(s"===> Warning: Pivot operation: All the required columns may not exist. Required Columns - ${req_cols.mkString(",")}")
        }
      }
      //df.unpersist
      df = df_
    }
    if (configuration.getBoolean("debug.pivot.operation")) {
      if (configuration.getBoolean("debug.pivot.show.input")) operation_debug("pivot", workflow_id, df, MasterDF)
      else operation_debug("pivot", workflow_id, df)
    }
    df
  }

  def columnRename(workflow_id: Long, MasterDF: DataFrame): DataFrame = {
    var df = MasterDF
    val isSuccess = !df.schema.isEmpty
    if (isSuccess) {
      var rows = column_rename_array.filter(x => x.getAs("WORKFLOW_ID").toString.toLong == workflow_id)
      scala.util.Sorting.stableSort(rows, (e1: Row, e2: Row) => e1.getAs("SEQUENCE").toString.toInt < e2.getAs("SEQUENCE").toString.toInt)
      //val columns = df.columns.toList
      //var replace_map: Map[String, String] = Map()

      rows.foreach { x =>
        val columns = df.columns.toList
        var replace_map: Map[String, String] = Map()
        val column_name = x.getAs[String]("COLUMN_NAME")
        val rename_col_name = x.getAs[String]("RENAME_COL_NAME")
        val regex_rep = x.getAs[String]("REGEX_REP")
        val regex_rep_with = x.getAs[String]("REGEX_REP_WITH")
        val check_dup: Boolean = x.getAs[String]("CHECK_DUPLICATES").equalsIgnoreCase("Y")

        val filtered_cols = getColumnsByRegex(columns, List(column_name))
        //println(filtered_cols)

        if (!rename_col_name.equals("")) {
          filtered_cols.foreach { column =>
            replace_map = replace_map ++ Map(column -> rename_col_name)
          }
        } else {
          filtered_cols.foreach { column =>
            replace_map = replace_map ++ Map(column -> column.replaceAll(regex_rep, regex_rep_with))
          }
        }

        if (check_dup) {
          replace_map = replace_map.groupBy(_._2).mapValues(_.keys.toList).filter(x => x._2.length == 1).map(x => x._2(0) -> x._1)
          val case_duplicates = replace_map.groupBy(_._2.toLowerCase).mapValues(_.keys.toList).filter(x => x._2.length != 1)
          if (case_duplicates.size > 0) {
            spark.conf.set("spark.sql.caseSensitive", "true")
            case_duplicates.foreach { dup_col =>
              df = df.withColumn(dup_col._1.toLowerCase(), coalesce(dup_col._2.map(x => col(s"`$x`")): _*)).drop(dup_col._2/*.map(x => s"`$x`")*/: _*)
              dup_col._2.foreach(x => replace_map = replace_map.-(x))
            }
            spark.conf.set("spark.sql.caseSensitive", "false")
          }
        }
        //println(replace_map)
        val df_non_rename_cols = df.columns.toSet -- replace_map.keys.toSet
        val df_final_cols = df_non_rename_cols.toList.map(x => col(s"`$x`"))
        val select_expressions = replace_map.keys.map(x => col(s"`$x`").as(replace_map(x))).toList
        df = df.select(df_final_cols ++ select_expressions: _*)
        /*replace_map.keys.foreach { key =>
          df = df.withColumnRenamed(key, replace_map(key).toString)
        }*/
      }
    }
    if (configuration.getBoolean("debug.column_rename.operation")) {
      if (configuration.getBoolean("debug.column_rename.show.input")) operation_debug("column_rename", workflow_id, df, MasterDF)
      else operation_debug("column_rename", workflow_id, df)
    }
    df
  }

  def sparkSqlExec(workflow_id: Long, MasterDF: DataFrame): DataFrame = {
    var df = MasterDF
    val isSuccess = !df.schema.isEmpty
    if (isSuccess) {
      val attributes: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
      var rows = spark_sql_exec_array.filter(x => x.getAs("WORKFLOW_ID").toString.toLong == workflow_id)
      scala.util.Sorting.stableSort(rows, (e1: Row, e2: Row) => e1.getAs("SEQUENCE").toString.toInt < e2.getAs("SEQUENCE").toString.toInt)

      rows.foreach { x =>
        var spark_sql_stmt = x.getAs[String]("SPRK_SQL_STMT")
        spark_sql_stmt=com.jio.pulse.plugins.Loop.loop_variables_replace(spark_sql_stmt)
        attributes("spark_sql_stmt") = spark_sql_stmt
        val replaced_attributes = plugins.Variables.variable_replace(attributes)
        spark_sql_stmt = replaced_attributes("spark_sql_stmt")
        val env_vars = x.getAs[String]("ENV_VARS")
        if (!env_vars.equals("")) {
          val env_vars_array = env_vars.split(",").map(x => (x.trim.replace(".", "_"), x.trim))
          env_vars_array.foreach { env_var =>
            df = df.withColumn(env_var._1, lit(spark.sparkContext.getConf.get(env_var._2)))
          }
        }
        df.createOrReplaceTempView("input_table")
        try {
          df = spark.sql(spark_sql_stmt)
        } catch {
          case ex: Exception => {
            println(ex)
            df = spark.emptyDataFrame
          }
        }
      }
    }
    plugins.Variables.variable_define(workflow_id, MasterDF)
    if (configuration.getBoolean("debug.sparksql.operation")) {
      if (configuration.getBoolean("debug.sparksql.show.input")) operation_debug("sparksql", workflow_id, df, MasterDF)
      else operation_debug("sparksql", workflow_id, df)
    }
    df
  }

  def dmlExecution(workflow_id: Long, MasterDF: DataFrame): DataFrame = {
    val df = MasterDF.cache
    //val isSuccess = !df.schema.isEmpty
    val dml_id_list = ReadInput.readInput(spark, "(" + dml_exec + job_instance_id + ") x", url, username, password, driver).select("dml_id").collect().map(x => x(0).toString).mkString(",")
    val dml_dtl_empty = ReadInput.readInput(spark, s"($dml_exec_details ($dml_id_list)) x", url, username, password, driver).head(1).isEmpty
    val data_empty = df.head(1).isEmpty
    if (!data_empty && !dml_dtl_empty) {
      var final_dml_df = df.join(dml_exec_df.filter(s"WORKFLOW_ID = $workflow_id"), expr("1=1"), "cross")
      final_dml_df = final_dml_df.select(final_dml_df.columns.map(x => col(x).as(x.toLowerCase())): _*)
      ApiUtils.updateDataInOracle(final_dml_df)
    } else if (dml_dtl_empty) {
      var final_dml_df = dml_exec_df.filter(s"WORKFLOW_ID = $workflow_id")
      final_dml_df = final_dml_df.select(final_dml_df.columns.map(x => col(x).as(x.toLowerCase())): _*)
      ApiUtils.updateDataInOracle(final_dml_df)
    }
    df
  }

  def addEnrichFields(input_df: DataFrame, enrich_fields_values: List[(String, String)]): DataFrame = {
    var df = input_df
    enrich_fields_values.foreach { field =>
      df = df.withColumn(field._1, lit(field._2))
    }
    df
  }

  def updateCurrentWorkflowDeps(workflow_array: Array[Row], wf_id_list: List[Long]): Array[Row] = {
    var cur_wf_id_list: List[Long] = wf_id_list
    var temp_workflow_array = workflow_array.toList.filter(x => !cur_wf_id_list.contains(x.getAs("workflow_id").toString.toLong))

    while (!cur_wf_id_list.isEmpty) {
      val cur_wf_id = cur_wf_id_list.head
      var index_list: List[Int] = List()
      temp_workflow_array.foreach { row =>
        val dep_id = row.getAs("dependency_id").toString.toLong
        val wf_id = row.getAs("workflow_id").toString.toLong
        val wf_cnt = temp_workflow_array.filter(x => x.getAs("workflow_id").toString.toLong == dep_id).size
        if (dep_id == cur_wf_id && wf_cnt <= 1) {
          index_list :+= temp_workflow_array.indexOf(row)
          cur_wf_id_list :+= row.getAs("workflow_id").toString.toLong
        }
      }
      var temp_wf_arr: List[Row] = List()
      for (i <- 0 to temp_workflow_array.length - 1) {
        if (!index_list.contains(i)) {
          temp_wf_arr :+= temp_workflow_array(i)
        }
      }
      temp_workflow_array = temp_wf_arr
      cur_wf_id_list = cur_wf_id_list.tail
    }
    val tempdf = spark.createDataFrame(sc.parallelize(temp_workflow_array.toSeq), temp_workflow_array(0).schema).drop("next_id", "fork")
    getWorkflowArray(tempdf)
  }

  def getWorkflowArray(df: DataFrame): Array[Row] = {
    var finalworkflowDF = df.select(df.columns.map(c => col(c).as(c.toLowerCase)): _*)
    finalworkflowDF = finalworkflowDF.select(finalworkflowDF.columns.map(c => col(c).as(c.toLowerCase)): _*)
    finalworkflowDF = finalworkflowDF.withColumn("dependency_id", when(col("dependency_id").isNull, lit(0)).otherwise(col("dependency_id")))
    var partcl = org.apache.spark.sql.expressions.Window.partitionBy(col("dependency_id"))
    finalworkflowDF = finalworkflowDF.join(
      finalworkflowDF.select(col("dependency_id").as("next_dep_id"), first(col("workflow_id")).over(partcl.orderBy(lit(1))).as("next_id")).distinct.as("nextdf"),
      finalworkflowDF.col("workflow_id") === col("next_dep_id"),
      "left")
      .withColumn("next_id", when(col("next_id").isNull, lit(-1)).otherwise(col("next_id")))
      .drop("next_dep_id")
    finalworkflowDF = finalworkflowDF.withColumn("fork", (count(col("*")) over partcl).cast(types.IntegerType)).orderBy("workflow_id", "dependency_id")

    finalworkflowDF.collect
  }

  val toolbox = currentMirror.mkToolBox()

  def executeCodeAndReturnFunction(code: String): Any => Boolean = {
    val tree = toolbox.parse(code)
    val compiledCode = toolbox.compile(tree)
    compiledCode().asInstanceOf[Any => Boolean]
  }

  def conditional_workflow(input_df: DataFrame, workflow_id: Long): List[Long] = {
    input_df.createOrReplaceTempView("input_df")
    val cond_arr = cond_array.filter(x => x.getAs("WORKFLOW_ID").toString.toLong == workflow_id)
    val sql_stmt = cond_arr(0).getAs[String]("SQL_STMT")
    var data_type = cond_arr(0).getAs[String]("DATA_TYPE")
    val cond_id = cond_arr(0).getAs("CONDITION_ID").toString.toLong
    val df_output = spark.sql(sql_stmt).collect()
    var output: Any = ""

    data_type = data_type.toLowerCase match {
      case "int" => "Int"
      case "long" => "Long"
      case "double" => "Double"
      case "float" => "Float"
      case "boolean" => "Boolean"
      case _ => "String"
    }

    output = data_type.toLowerCase match {
      case "int" => df_output(0)(0).toString.toInt
      case "long" => df_output(0)(0).toString.toLong
      case "double" => df_output(0)(0).toString.toDouble
      case _ => df_output(0)(0).toString
    }

    var excluded_wf_id: List[Long] = List()
    cond_dtls_array.filter(x => x.getAs("CONDITION_ID").toString.toLong == cond_id).foreach { row =>
      val filter_condition = row.getAs[String]("EXPRESSION").replaceAll("output", s"output.asInstanceOf[$data_type]")
      println("===> Conditional Filter expression: " + filter_condition)
      val filter_function = executeCodeAndReturnFunction(s"""(output: Any) => { $filter_condition }""")
      val result = filter_function(output)
      println("===> Result: " + result)
      if (!result) {
        excluded_wf_id :+= row.getAs("NEXT_WF_ID").toString.toLong
      }
    }
    excluded_wf_id
  }

  def dfRepartition(workflow_id: Long,input_df: DataFrame): DataFrame = {
    val rows = repartition_array.filter(x => x.getAs("WORKFLOW_ID").toString.toLong == workflow_id)
    val temp_partition_cols = rows.map(x => x.getAs[String]("COLUMN_NAME")).filter(x => !x.equals(""))
    val partition_columns = temp_partition_cols.map(x => col(x))
    val num_partitions = rows(0).getAs("NUM_PARTITIONS").toString.toInt
    val df = if (num_partitions != 0 && partition_columns.size != 0)
      input_df.repartition(num_partitions, partition_columns: _*)
    else if (num_partitions != 0)
      input_df.repartition(num_partitions)
    else if (partition_columns.size != 0)
      input_df.repartition(partition_columns: _*)
    else
      input_df.repartition()
    df
  }

  def dfCoalesce(workflow_id: Long,input_df: DataFrame): DataFrame = {
    val row = coalesce_array.filter(x => x.getAs("WORKFLOW_ID").toString.toLong == workflow_id)(0)
    val num_partitions = row.getAs("NUM_PARTITIONS").toString.toInt
    input_df.coalesce(num_partitions)
  }

  def dynamic_sql_master_read(dyn_sql_id: Long, connection_string_id: Long, source: String): (DataFrame, Boolean) = {
    var master_table: DataFrame = spark.emptyDataFrame
    var isSuccess: Boolean = false
    if (source.equalsIgnoreCase("api")) {
      var ConnectionDetails: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map()
      var api_read = api_read_array.filter(x => x.getAs("READ_ENRICHMENT_ID").toString.toLong == dyn_sql_id)(0)
      var api_id = api_read.getAs("API_ID").toString.toInt
      ConnectionDetails = ConnectionDetails ++ Map("method" -> api_read.getAs[String]("METHOD")) ++ Map("payload" -> api_read.getAs[String]("PAYLOAD")) ++ Map("url" -> api_read.getAs[String]("URL")) ++ Map("username" -> api_read.getAs[String]("AUTH_USERNAME")) ++ Map("password" -> api_read.getAs[String]("AUTH_PASSWORD")) ++ Map("api_id" -> api_id.toString)
      var header_map: Map[String, String] = Map()
      api_header_array.filter(x => x.getAs("API_ID").toString.toInt == api_id).foreach { row =>
        var key = row.getAs[String]("HEADER_KEY")
        var value = row.getAs[String]("HEADER_FIXED_VALUE")
        header_map = header_map ++ Map(key -> value)
      }
      ConnectionDetails = ConnectionDetails ++ Map("header" -> header_map)
      var read_output = com.jio.pulse.ReadWriteUtility.read(source, ConnectionDetails, configuration.getBoolean("debug.show.dynamicsql.tables"))
      master_table = read_output._1
      isSuccess = read_output._2
    } else {
      var ConnectionDetails: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
      var ReadDetails: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
      connection_array.filter(x => x.getAs("connection_string_id").toString.toLong == connection_string_id).foreach(row => ConnectionDetails = ConnectionDetails ++ Map(row.getAs[String]("key") -> row.getAs[String]("value")))
      dyn_sql_add_attr_array.filter(x => x.getAs("DYN_SQL_ID").toString.toLong == dyn_sql_id).foreach(row => ReadDetails = ReadDetails ++ Map(row.getAs[String]("KEY") -> row.getAs[String]("VALUE")))
      var read_output = com.jio.pulse.ReadWriteUtility.read(source, (ConnectionDetails ++ ReadDetails), configuration.getBoolean("debug.show.dynamicsql.tables"))
      master_table = read_output._1
      isSuccess = read_output._2
    }

    if (!dyn_sql_json_parsing_array.filter(x => x.getAs("DYN_SQL_ID").toString.toLong == dyn_sql_id).isEmpty) {
      try {
        master_table = genericJsonParsing(-1, master_table, -1, -1, dyn_sql_id)
        isSuccess = true
      } catch {
        case ex: Exception => {
          isSuccess = false
          println("====> Error while parsing master table with dyn_sql_id: " + dyn_sql_id)
          println(ex.getMessage)
          println("====> Stack trace")
          println(ex.getStackTrace.mkString("\n"))
        }
      }
    }

    (master_table, isSuccess)
  }

  var master_refresh_map: scala.collection.mutable.Map[Long, Long] = scala.collection.mutable.Map()
  var master_df_map: scala.collection.mutable.Map[Long, DataFrame] = scala.collection.mutable.Map()
  var master_arr_map: scala.collection.mutable.Map[Long, Array[Row]] = scala.collection.mutable.Map()
  var master_case_statement_map: scala.collection.mutable.Map[Long, String] = scala.collection.mutable.Map()

  def dynamic_sql_build(workflow_id: Long,input_df: DataFrame): DataFrame = {
    var df = input_df
    df = df.select(df.columns.map(x => col(x).as(x.toLowerCase())): _*)
    val isSuccess: Boolean = !df.schema.isEmpty
    if (isSuccess) {
      val df_columns = df.columns.map(x => x.toLowerCase())
      val rows = dyn_sql_array.filter(x => x.getAs("WORKFLOW_ID").toString.toLong == workflow_id)
      scala.util.Sorting.stableSort(rows, (e1: Row, e2: Row) => e1.getAs("SEQ").toString.toInt < e2.getAs("SEQ").toString.toInt)
      rows.foreach { row =>
        val dyn_sql_id = row.getAs("DYN_SQL_ID").toString.toLong
        val connection_string_id = row.getAs("CONNECTION_STRING_ID").toString.toLong
        val source = row.getAs("SOURCE").toString
        val output_column_name = row.getAs("DERIVED_COLUMN_NAME").toString
        val refresh_freq = row.getAs("REFRESH_FREQ").toString.toLong
        val validate_master = row.getAs("VALIDATE_MASTER").toString.equalsIgnoreCase("Y")

        if (!master_refresh_map.contains(dyn_sql_id)) master_refresh_map(dyn_sql_id) = 0L

        val isRefresh: Boolean = System.currentTimeMillis() - master_refresh_map(dyn_sql_id) >= refresh_freq

        if (isRefresh) {
          master_df_map(dyn_sql_id) = dynamic_sql_master_read(dyn_sql_id, connection_string_id, source)._1
          master_df_map(dyn_sql_id) = master_df_map(dyn_sql_id).select(master_df_map(dyn_sql_id).columns.map(c => col(c).as(c.toLowerCase)): _*)
          master_refresh_map(dyn_sql_id) = System.currentTimeMillis()
        }

        val dyn_sql_op_info_rows = dyn_sql_op_info_array.filter(x => x.getAs("DYN_SQL_ID").toString.toLong == dyn_sql_id)
        val operation = dyn_sql_op_info_rows.head.getAs[String]("OPERATION_NAME")
        val att_map: Map[String, String] = dyn_sql_op_info_rows.map(x => (x.getAs[String]("ATT_NAME"), x.getAs[String]("VALUE"))).toMap
        var case_statement = ""

        if (isRefresh || validate_master) {
          operation.toLowerCase match {
            case "concat" => {
              val select_columns = dyn_sql_dist_col_array.filter(x => x.getAs("DYN_SQL_ID").toString.toLong == dyn_sql_id).map(x => x.getAs[String]("MST_COL_NAME")).mkString(",")
              val grp_col = att_map.get("col_name_in_enr").getOrElse("").toLowerCase()
              val seq_col = att_map.get("seq_col_name_in_enr").getOrElse("").toLowerCase()
              val separator = att_map.get("separator").getOrElse("").toLowerCase()
              val postfix = att_map.get("static_column_postfix").getOrElse("").toLowerCase()
              val prefix = att_map.get("static_column_prefix").getOrElse("").toLowerCase()

              master_df_map(dyn_sql_id).createOrReplaceTempView("input_table")
              var master_df = spark.sql(s"select $select_columns, concat_ws(',', $grp_col) $grp_col, concat_ws(',', $seq_col) $seq_col from (select $select_columns, collect_list('`' || $grp_col || '`') $grp_col, collect_list($seq_col) $seq_col from input_table group by $select_columns) a")
              master_arr_map(dyn_sql_id) = master_df.collect()
              var end_cnt: Int = master_arr_map(dyn_sql_id).length - 1

              master_arr_map(dyn_sql_id).foreach { row =>
                var add_to_case: Boolean = true
                var when_part = ""
                dyn_sql_dist_col_array.filter(x => x.getAs("DYN_SQL_ID").toString.toLong == dyn_sql_id).foreach { x =>
                  if (!df_columns.contains(x.getAs[String]("COL_NAME_IN_INPUT").toLowerCase())) df = df.withColumn(x.getAs[String]("COL_NAME_IN_INPUT"), lit(null))
                  val single_quotes = if (x.getAs[String]("DATA_TYPE").equalsIgnoreCase("string")) "'" else ""
                  when_part += x.getAs[String]("COL_NAME_IN_INPUT") + s" = $single_quotes" + row.getAs(x.getAs[String]("MST_COL_NAME").toLowerCase).toString + s"$single_quotes and "
                }
                when_part = when_part.substring(0, when_part.lastIndexOf(" and "))

                val grp_col_ = row.getAs[String](s"$grp_col")
                val seq_col_ = row.getAs[String](s"$seq_col")
                var arr = grp_col_.split(",").zip(seq_col_.split(",").map(_.toInt))
                scala.util.Sorting.stableSort(arr, (e1: (String, Int), e2: (String, Int)) => e1._2 < e2._2)
                var then_part = ""
                arr.foreach { grp =>
                  if (!df_columns.contains(grp._1.toLowerCase) && add_to_case) {
                    add_to_case = false
                    end_cnt -= 1
                    println(s"column '${grp._1}' does not exist in master")
                    if (validate_master) {
                      throw new Exception(s"Not Found Column: ${grp._1}")
                    }
                  } else if (add_to_case) {
                    then_part += grp._1 + ", " + s"'$separator', "
                  }
                }
                if (add_to_case) {
                  then_part = then_part.substring(0, then_part.lastIndexOf(s", '$separator', "))
                  if (!prefix.equals("")) then_part = prefix + ", '" + separator + "', " + then_part
                  if (!postfix.equals("")) then_part = then_part + ", '" + separator + "', " + postfix
                  case_statement += "case when " + when_part + " then " + operation + "(" + then_part + ")" + " else "
                }
              }
              case_statement = case_statement.substring(0, case_statement.lastIndexOf(" else "))
              (0 to end_cnt).foreach(_ => case_statement += " end")

              println(case_statement)
              master_case_statement_map(dyn_sql_id) = case_statement
            }
            case "split" => {
              val delimiter_col = att_map.get("delimiter_column").getOrElse("").toLowerCase()
              val index_col = att_map.get("index_column").getOrElse("").toLowerCase()
              val split_col = att_map.get("split_column").getOrElse("").toLowerCase()
              master_arr_map(dyn_sql_id) = master_df_map(dyn_sql_id).collect()
              var end_cnt: Int = master_arr_map(dyn_sql_id).length - 1

              master_arr_map(dyn_sql_id).foreach { row =>
                var when_part = ""
                dyn_sql_dist_col_array.filter(x => x.getAs("DYN_SQL_ID").toString.toLong == dyn_sql_id).foreach { x =>
                  if (!df_columns.contains(x.getAs[String]("COL_NAME_IN_INPUT").toLowerCase())) df = df.withColumn(x.getAs[String]("COL_NAME_IN_INPUT"), lit(null))
                  val single_quotes = if (x.getAs[String]("DATA_TYPE").equalsIgnoreCase("string")) "'" else ""
                  when_part += x.getAs[String]("COL_NAME_IN_INPUT") + s" = $single_quotes" + row.getAs(x.getAs[String]("MST_COL_NAME").toLowerCase).toString + s"$single_quotes and "
                }
                when_part = when_part.substring(0, when_part.lastIndexOf(" and "))

                val index = if (row.getAs(s"$index_col") != null) row.getAs(s"$index_col").toString.toInt else 0
                val delimiter = if (row.getAs(s"$delimiter_col") != null) row.getAs[String](s"$delimiter_col") else ""
                val split_column = if (row.getAs(s"$split_col") != null) row.getAs[String](s"$split_col") else ""
                if (df_columns.contains(split_column.toLowerCase)) {
                  var then_part = s"$operation($split_column, '$delimiter')[$index]"
                  case_statement += "case when " + when_part + " then " + then_part + " else "
                } else {
                  end_cnt -= 1
                  println(s"column '$split_column' does not exist in master")
                  if (validate_master) {
                    throw new Exception(s"Not Found Column: '$split_column'")
                  }
                }
              }
              case_statement = case_statement.substring(0, case_statement.lastIndexOf(" else "))
              (0 to end_cnt).foreach(_ => case_statement += " end")
              println(case_statement)

              master_case_statement_map(dyn_sql_id) = case_statement
            }
             case "percentile_approx" => {
              val percentile_column_att = att_map.get("percentile_column").getOrElse("").toLowerCase()
              val approx_value_att = att_map.get("approx_value").getOrElse("").toLowerCase()
              val partition_columns_att = att_map.get("partition_columns").getOrElse("").toLowerCase()
              master_arr_map(dyn_sql_id) = master_df_map(dyn_sql_id).collect()
              var end_cnt: Int = master_arr_map(dyn_sql_id).length - 1
              val case_statement_map = collection.mutable.Map[Double, (String, String)]()
              master_arr_map(dyn_sql_id).foreach { row =>
                var when_part = ""
                dyn_sql_dist_col_array.filter(x => x.getAs("DYN_SQL_ID").toString.toLong == dyn_sql_id).foreach { x =>
                  if (!df_columns.contains(x.getAs[String]("COL_NAME_IN_INPUT").toLowerCase())) df = df.withColumn(x.getAs[String]("COL_NAME_IN_INPUT"), lit(null))
                  val single_quotes = if (x.getAs[String]("DATA_TYPE").equalsIgnoreCase("string")) "'" else ""
                  when_part += x.getAs[String]("COL_NAME_IN_INPUT") + s" = $single_quotes" + row.getAs(x.getAs[String]("MST_COL_NAME").toLowerCase).toString + s"$single_quotes and "
                }
                when_part = when_part.substring(0, when_part.lastIndexOf(" and "))

                val approx_value = if (row.getAs(s"$approx_value_att") != null) row.getAs(s"$approx_value_att").toString else ""
                val partition_columns = if (row.getAs(s"$partition_columns_att") != null) row.getAs[String](s"$partition_columns_att") else ""
                val percentile_column = if (row.getAs(s"$percentile_column_att") != null) row.getAs[String](s"$percentile_column_att") else ""
                if (df_columns.contains(percentile_column.toLowerCase)) {
                  approx_value.split(",").foreach(percent => {
                    val percentile = if (percent.toDouble > 1) percent.toDouble / 100 else percent.toDouble
                    if (case_statement_map.contains(percentile)) {
                      val map_data = case_statement_map.get(percentile).get
                      var end = map_data._1 + " end "
                      var then_part = s"$operation($percentile_column, $percentile) over (partition by $partition_columns )"
                      var case_stm = map_data._2 + "case when " + when_part + " then " + then_part + " else "
                      case_statement_map.put(percentile, (end, case_stm))
                    } else {
                      var end = " end "
                      var then_part = s"$operation($percentile_column, $percentile) over (partition by $partition_columns )"
                      var case_stm = "case when " + when_part + " then " + then_part + " else "
                      case_statement_map.put(percentile, (end, case_stm))
                    }
                  }

                  )
                } else {
                  end_cnt -= 1
                  println(s"column '$percentile_column' does not exist in master")
                  if (validate_master) {
                    throw new Exception(s"Not Found Column: '$percentile_column'")
                  }
                }
              }
              for (elem <- case_statement_map) {
                df = df.withColumn(output_column_name.trim + "_" + (elem._1 * 100).toInt, expr(elem._2._2.substring(0, elem._2._2.lastIndexOf(" else ")) + elem._2._1))
              }
              case_statement_map.clear()
             
            }
            case _ => println("===> The operation is not defined in the master")
          }
        }

        try {
          df= if(operation.toLowerCase.equalsIgnoreCase("percentile_approx")) df else df.withColumn(output_column_name, expr(master_case_statement_map(dyn_sql_id)))
        } catch {
          case ex: Exception => {
            if (validate_master) {
              df = spark.emptyDataFrame
              throw new Exception(ex.getMessage)
            } else {
              df = df.withColumn(output_column_name, lit(null))
            }
            println(ex)
          }
        }
      }
    }
    if (configuration.getBoolean("debug.dynamicsql.operation")) {
      if (configuration.getBoolean("debug.dynamicsql.show.input")) operation_debug("dynamicsql", workflow_id, df, input_df)
      else operation_debug("dynamicsql", workflow_id, df)
    }
    df
  }

  //finalworkflowDF.show(false)
  var readdetailsdf = workflow_array.filter(x => x.getAs[String]("type").equalsIgnoreCase("read") && x.getAs("dependency_id").toString().toLong == 0)(0)
  var op_sub_type = readdetailsdf.getAs[String]("subtype")
  var is_streaming = readdetailsdf.getAs[String]("is_streaming").equalsIgnoreCase("Y")
  var data_format = readdetailsdf.getAs[String]("data_format")
  var connection_string_id = readdetailsdf.getAs("connection_string_id").toString.toLong
  var read_write_id = readdetailsdf.getAs("read_write_id").toString.toLong
  var workflow_id = readdetailsdf.getAs("workflow_id").toString.toLong
  var next_id = readdetailsdf.getAs("next_id").toString.toLong
  if (op_sub_type.equalsIgnoreCase("kafka") && is_streaming) {
    setKafkaParameters
//    new ReadStream(connection_string_id, read_write_id, next_id, data_format)
    new StructuredStreaming(connection_string_id, read_write_id, next_id, data_format)
    /*var rdd = sc.textFile("C:\\Users\\praneet.pulivendula\\Documents\\My Files\\Spark_Jobs_Data\\gplinux_data.txt").map(x => (x, "topic"))
    fork(spark.createDataFrame(rdd).withColumnRenamed("_1", "__data__").drop("_2"), next_id)*/
    //fork(spark.read.json("C:\\Users\\praneet.pulivendula\\Documents\\My Files\\Spark_Jobs_Data\\DT_data.txt"), next_id) //deep_tracing_data,disk-fstab_comp.json,per_process.json,nmas_data_event,ns_events_data_1,nmas_data_autoclosure,solarwind_data }
  } else if (op_sub_type.equalsIgnoreCase("Eventhub") && is_streaming) { //added this line to stream from event hub
    setKafkaParameters
    new ReadStream_Eventhub(connection_string_id, read_write_id, next_id, data_format)
    //fork(spark.read.json("C:\\Users\\praneet.pulivendula\\Documents\\My Files\\Spark_Jobs_Data\\activity_logs.txt"), next_id)
  } else if (op_sub_type.equalsIgnoreCase("PubSub") && is_streaming) {
    setKafkaParameters
    new ReadStream_Pubsub(connection_string_id, read_write_id, next_id, data_format)
    //fork(spark.read.json("C:\\Users\\praneet.pulivendula\\Documents\\My Files\\Spark_Jobs_Data\\activity_logs.txt"), next_id)
  } else if (is_streaming) {
    try {
      val running_interval = readdetailsdf.getAs("runninginterval").toString.toLong
      while (true) {
        val start_time_milli: Long = System.currentTimeMillis()
        new ReadBatch(workflow_id)
        val end_time_milli: Long = System.currentTimeMillis()
        val sleep_time: Long = (running_interval * 1000) - (end_time_milli - start_time_milli)
        if (sleep_time > 0L) {
          println(s"===> Job Running interval defined = ${running_interval * 1000} milliseconds")
          println(s"===> Total time taken to run the job = ${end_time_milli - start_time_milli} milliseconds")
          println(s"===> Sleeping Time = ${Math.max(sleep_time, 0L)} milliseconds")
          Thread.sleep(sleep_time)
        }
      }
    } catch {
      case ex: Exception => println(ex)
    }
  } else {
    new ReadBatch(workflow_id)
  }
}