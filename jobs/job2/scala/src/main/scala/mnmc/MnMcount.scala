// scalastyle:off println

package main.scala.mnmc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Usage: MnMcount <mnm_file_dataset>
  */
object MnMcount {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("MnMCount")
      .getOrCreate()

    if (args.length < 1) {
      print("Usage: MnMcount <mnm_file_dataset>")
      sys.exit(1)
    }
    // get the M&M data set file name
    val mnmFile = args(0)
    // read the file into a Spark DataFrame
    val mnmDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(mnmFile)
    // display DataFrame
    mnmDF.show(5, false)

    // aggregate count of all colors and groupBy state and color
    // orderBy descending order
    val countMnMDF = mnmDF.select("State", "Color", "Count")
        .groupBy("State", "Color")
        .sum("Count")
        .orderBy(desc("sum(Count)"))

    // show all the resulting aggregation for all the dates and colors
    countMnMDF.show(60)
    println(s"Total Rows = ${countMnMDF.count()}")
    println()

    // find the aggregate count for California by filtering
    val caCountMnNDF = mnmDF.select("*")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .sum("Count")
      .orderBy(desc("sum(Count)"))

    // show the resulting aggregation for California
    caCountMnNDF.show(10)

    caCountMnNDF.write.option("header", true).mode("overwrite").csv("./output");
    caCountMnNDF.write.option("header", true).mode("overwrite").csv("hdfs://localhost:9000/user/test/");
    spark.read.option("header", true).option("inferSchema", true).csv("hdfs://localhost:9000/user/test/").show(20, false);
  }
}
// scalastyle:on println
