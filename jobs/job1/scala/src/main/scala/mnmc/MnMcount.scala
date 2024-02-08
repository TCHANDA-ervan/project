package main.scala.mnmc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.time.LocalDate
import org.apache.spark.sql.types._
import java.time._
import org.apache.spark.sql.functions.{unix_timestamp, from_unixtime}
/**
  * Usage: MnMcount <mnm_file_dataset>
  */
object MnMcount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("MnMCount")
      .getOrCreate()

    // Chemin du fichier
    val path = "hdfs://localhost:9000/user/ubuntu/off_raw/en.openfoodfacts.org.products.csv"

    // Lecture du fichier CSV
    val df = spark.read.option("header", "true").option("delimiter", "\\t").csv(path)

    // Afficher le schéma du DataFrame
    df.printSchema()

    // Sélection des colonnes utiles
    val selectedData = df.select(
      "code", "url", "created_t", "last_modified_t", "product_name", "brands", "quantity", "categories",
      "categories_tags", "labels", "labels_tags", "countries", "countries_tags", "ingredients_text", "allergens",
      "product_quantity", "traces", "traces_tags", "serving_size", "energy_100g", "proteins_100g", "fat_100g",
      "carbohydrates_100g", "sugars_100g", "fiber_100g", "sodium_100g", "additives_n", "additives_tags",
      "nutrition-score-fr_100g", "main_category", "image_url", "nutriscore_score", "nutriscore_grade",
      "ecoscore_score", "ecoscore_grade"
    )

    //schema DDL

    val customSchemaDDL = """
  `code` STRING,
  `url` STRING,
  `product_name` STRING,
  `brands` STRING,
  `quantity` STRING,
  `categories` STRING,
  `categories_tags` ARRAY<STRING>,
  `labels` STRING,
  `labels_tags` ARRAY<STRING>,
  `countries` STRING,
  `countries_tags` ARRAY<STRING>,
  `ingredients_text` STRING,
  `traces` STRING,
  `traces_tags` ARRAY<STRING>,
  `serving_size` STRING,
  `energy_100g` DOUBLE,
  `proteins_100g` DOUBLE,
  `fat_100g` DOUBLE,
  `carbohydrates_100g` DOUBLE,
  `sugars_100g` DOUBLE,
  `fiber_100g` DOUBLE,
  `sodium_100g` DOUBLE,
  `additives_n` LONG,
  `additives_tags` ARRAY<STRING>,
  `nutrition_grade_fr` STRING,
  `main_category` STRING,
  `image_url` STRING
"""
selectedData.select("product_name","categories")show(30)
    // Suppression des doublons basés sur le code du produit
    val uniqueDataByCode = selectedData.dropDuplicates("code")

    // Suppression des doublons basés sur le nom du produit
    val uniqueDataByName = selectedData.dropDuplicates("product_name")

    // Suppression des lignes avec des valeurs null dans les colonnes spécifiées
    val productNotNull = uniqueDataByName.na.drop(Seq("quantity", "product_name", "code"))

    

    //sparksql
    val sqlframe=selectedData.createOrReplaceTempView("produit")


spark.sql("select product_name,brands,quantity,energy_100g,'nutrition-score-fr_100g' from produit where quantity is not null limit 2").show(false)


spark.sql("select * from produit where quantity is not null limit 10").show



    // Convertir les colonnes de dates en un format unique
    val dateFormat = "yyyy-MM-dd HH:mm:ss"
    val dataWithCustomTimestamps = productNotNull.withColumn("created_t", from_unixtime(col("created_t"), dateFormat).cast("timestamp"))
      .withColumn("last_modified_t", from_unixtime(col("last_modified_t"), dateFormat).cast("timestamp"))

    // Affichage des 5 premières lignes du DataFrame avec les colonnes sélectionnées
    dataWithCustomTimestamps.select("code", "product_name", "created_t", "last_modified_t").show(5)

    // Écriture du DataFrame final sur Hadoop
    val currentDate = LocalDate.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    dataWithCustomTimestamps.write.mode("overwrite").parquet(s"hdfs://localhost:9000/user/ubuntu/off_formatted/$currentDate")

    // Arrêter la session Spark
    spark.stop()
  }
}
