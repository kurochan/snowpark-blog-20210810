package org.kurochan.snowpark_test

import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._

object Main {
  def createSession(): Session = {
    val configs = Map (
      "URL" -> "https://ab12345.ap-northeast-1.aws.snowflakecomputing.com:443",
      "USER" -> "SNOWPARK_TEST",
      "PASSWORD" -> "password here",
      "ROLE" -> "SYSADMIN",
      "WAREHOUSE" -> "COMPUTE_WH",
      "DB" -> "SANDBOX",
      "SCHEMA" -> "PUBLIC"
    )
    Session.builder.configs(configs).create
  }

  val randomId: String = scala.util.Random.alphanumeric.take(10).mkString
  def getRandomId(s: String): String = {
    randomId
  }
  val randomIdUdf = udf(getRandomId _)

  var counter = 0
  def incrementAndGet(s: String): Long = {
    counter += 1
    counter
  }
  val incrementAndGetUdf = udf(incrementAndGet _)

  def getProcessors(s: String): Long = {
    Runtime.getRuntime.availableProcessors()
  }
  val getProcessorsUdf = udf(getProcessors _)

  def getMemory(s: String): Long = {
    Runtime.getRuntime.maxMemory()
  }
  val getMemoryUdf = udf(getMemory _)

  def getCurrentMemory(s: String): Long = {
    Runtime.getRuntime.totalMemory()
  }
  val getCurrentMemoryUdf = udf(getCurrentMemory _)

  def main(args: Array[String]): Unit = {
    val session = createSession()

    val table = session.table("test_log").limit(1000000)

    val data = table.select(
      randomIdUdf(col("name")).as("static_id"),
      incrementAndGetUdf(col("name")).as("counter_value"),
      getProcessorsUdf(col("name")).as("processors"),
      getMemoryUdf(col("name")).as("memory"),
      getCurrentMemoryUdf(col("name")).as("current_memory"),
    )

    val localResult = data.groupBy(col("static_id"), col("counter_value"))
      .agg(
        min(col("processors")).as("min_processors"),
        max(col("processors")).as("max_processors"),
        (min(col("memory"))).as("min_memory"),
        (max(col("memory"))).as("max_memory"),
        (max(col("current_memory"))).as("current_memory"),
        count(lit(1)).as("count"),
      )

    val clusterResult = localResult.groupBy(col("static_id"))
      .agg(
        min(col("min_processors")).as("min_processors"),
        max(col("max_processors")).as("max_processors"),
        (min(col("min_memory")) / 1024 / 1024 / 1024).as("min_memory"),
        (max(col("max_memory")) / 1024 / 1024 / 1024).as("max_memory"),
        (max(col("current_memory")) / 1024 / 1024 / 1024).as("current_memory"),
        max(col("count")).as("conflict_count"),
        sum(col("count")).as("exec_count")
      )
      .sort(col("exec_count").desc)
     clusterResult.show(100)
  }
}

