package org.sergiojulio
import org.apache.spark.sql.SparkSession

object job {

  def main(args: Array[String]): Unit = {

    print("\n\n>>>>> START OF PROGRAM <<<<<\n\n")

    val spark = SparkSession.builder().master("local[4]").appName("sqlite-to-parquet").getOrCreate()
    /*
    val df = spark.read.option("header",true)
      .csv("/home/sergio/Downloads/new_user_credentials.csv")
    df.printSchema()
    df.show()
    */


    // for tables

    val df2 = spark.read.format("jdbc").options(
        Map("url" -> "jdbc:sqlite:/home/sergio/Downloads/Datasets/spotify.sqlite",
            "query" -> "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")).load()

    df2.show()



    val myTableNames = df2.select("name").collect.map(f=>f.getString(0)).toList

    for (t <- myTableNames) {

      println(t.toString)

      val str1 = "SELECT * FROM "
      val str2 = t.toString

      val tableData = spark.read.format("jdbc").options(
        Map("url" -> "jdbc:sqlite:/home/sergio/Downloads/Datasets/spotify.sqlite",
            "query" -> str1.concat(str2))).load()

      tableData.coalesce(1).write.format("parquet").mode("append").save("/home/sergio/Downloads/Datasets/parquets/" + t + ".parquet")

      //tableData.show()
    }



    // end for tables

    print("\n\n>>>>> END OF PROGRAM <<<<<\n\n")

    spark.close()

  }
}
