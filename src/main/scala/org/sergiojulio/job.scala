package org.sergiojulio

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object job {

  def main(args: Array[String]): Unit = {

    //Logger.getLogger("org").setLevel(Level.ERROR)
    print("\n\n>>>>> START OF PROGRAM <<<<<\n\n")
    // args(0) path sqlite db
    // args(1) path parquets

    if (args.length > 0) {
      print("\n\n>>>>>" + args(0) + "<<<<<\n\n")
    }

    val spark = SparkSession.builder().master("local[8]").appName("sqlite-to-parquet").getOrCreate()
    /*
    val df = spark.read.option("header",true)
      .csv("/home/sergio/Downloads/new_user_credentials.csv")
    df.printSchema()
    df.show()
    */
    // for tables
    // can you read my mind?
    val df2 = spark.read.format("jdbc").options(
      Map("url" -> "jdbc:sqlite:/home/sergio/dev/spark/spark-sqlite-to-parquet/tmp/chinook.db",
        "query" -> "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")).load()

    df2.show()

    val myTableNames = df2.select("name").collect.map(f => f.getString(0)).toList

    for (t <- myTableNames) {

      println(t.toString)

      val str1 = "SELECT * FROM "
      val str2 = t.toString

      val tableData = spark.read.format("jdbc").options(
        Map("url" -> "jdbc:sqlite:/home/sergio/dev/spark/spark-sqlite-to-parquet/tmp/chinook.db",
          "query" -> str1.concat(str2))).load()

      tableData.coalesce(1).write.format("parquet").mode("append").save("/home/sergio/dev/spark/spark-sqlite-to-parquet/tmp/" + t + ".parquet")

      //tableData.show()
    }

    // end for tables
    print("\n\n>>>>> END OF PROGRAM <<<<<\n\n")
    spark.close()
  }
}
