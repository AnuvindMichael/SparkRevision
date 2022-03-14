package SparkRevision


import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

import scala.util.control.Exception.noCatch.desc
object SparkObj {
  case class schema(txnno:String,txndate:String,custno:String,amount:String,Category:String,product:String,city:String,state:String,spendby:String)

  def main(args:Array[String]): Unit ={
    val spark = SparkSession.builder()
                .appName("SparkRevision")
                .master("local[*]")
                .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val SchemaStruct = new StructType()
      .add("txnno",StringType,true)
      .add("txndate",StringType,true)
      .add("custno",StringType,true)
      .add("amount",StringType,true)
      .add("Category",StringType,true)
      .add("product",StringType,true)
      .add("city",StringType,true)
      .add("state",StringType,true)
      .add("spendby",StringType,true)

    val columnList=
      List("txnno","txndate","custno","amount","category","product","city","state","spendby")

    println()
    println("=========Process Start==========")
    println()


    val listrdd = List(1,4,6,7)
    val frdd = listrdd.map(x => x+2)
    frdd.foreach(println)

    val slist = List("zeyo","zeyobron","analytics","sparkzeyo")
    val flist = slist.filter(x => x.contains("zeyo"))
    flist.foreach(println)
    println()
    println("=========RDD Process ==========")
    println()

    val rdd1 = spark.sparkContext.textFile("file:///D://Practice//revdata//revdata/file1.txt")
    rdd1.take(10).foreach(println)
    val rdd2 = rdd1.filter(x => x.contains("Gymnastics"))
    rdd2.take(5).foreach(println)
    println()
    println("=========Schema Rdd ==========")
    println()

    val rdd3 = rdd1.map(x =>x.split(","))
    val rdd4 = rdd3.map(x =>schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
    val rdd5 = rdd4.filter(x =>x.product.contains("Gymnastics"))
    rdd5.take(10).foreach(println)

    println()
    println("=========Row Rdd ==========")
    println()

    val rdd6 = sc.textFile("file:///D://Practice//revdata//revdata/file2.txt")
    val rdd7 = rdd6.map(x =>x.split(","))
    val rdd8 = rdd7.map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
    rdd8.take(10).foreach(println)

    println()
    println("=========DataFrame Creation Using Schema Rdd && Row RDD ==========")
    println()

    //To Create DataFrame From Schema Rdd we use toDF(Method)
   val df = rdd5.toDF()
   df.show(10)

    val df1 = spark.createDataFrame(rdd8,SchemaStruct)
    df1.show(10)
    println()
    println("=========READ CSV FILE==========")
    println()

    val df2 = spark.read.format("csv")
              .options(Map("header"->"true","delimiter"->",","inferSchema"->"true"))
              .load("file:///D://Practice//revdata//revdata/file3.txt")
    df2.show(5)
    println()
    println("=========READ json FILE==========")
    println()
    val df3 = spark.read.format("json").option("multiLine","true")
              .load("file:///D://Practice//revdata//revdata/file4.json")
      .select(columnList.map(col):_*) //ADDING UNIFIED COLUMNS NOTATION
    df3.show(5)

    println()
    println("=========READ Parquet FILE==========")
    println()

    val df4 = spark.read.format("parquet")
      .load("file:///D://Practice//revdata//revdata/file5.parquet")
      .select(columnList.map(col):_*) //ADDING UNIFIED COLUMNS NOTATION
    df4.show(5)
    println()
    println("=========READ XML FILE==========")
    println()

    val df5 = spark.read.format("com.databricks.spark.xml").option("rowTag","txndata")
      .load("file:///D://Practice//revdata//revdata/file6")
      .select(columnList.map(col):_*) //ADDING UNIFIED COLUMNS NOTATION
    df5.show()
    df5.printSchema()

    println()
    println("=========Union All DataFrames==========")
    println()

    val uniondf = df2.union(df3).union(df4).union(df5)
    uniondf.show(10)

    println()
    println("=========Process Union Data==========")
    println()

    val df6 = uniondf.filter(col("txnno").cast("int")>50000)
              .withColumn("txndate",expr("split(txndate,'-')[2]"))
      .withColumnRenamed("txndate","Year")
      .withColumn("status",expr("case when spendby='cash' then 1 else 0 end"))
    df6.show(10)
    println()
    println("=========Cummulative sum for each category==========")
    println()
    val df7 = df6.groupBy("category")
    .agg(sum("amount").cast("float").as("Total_Amount"))
      .orderBy(functions.desc("Total_Amount"))
    df7.show(10)
    println()
    println("=========Data Write started==========")
    println()

    df7.write.format("com.databricks.spark.avro")
      .mode("append").save("file:///D:/Practice/RevisionData")

    println()
    println("=========Successfully Data Written into Local System==========")
    println()

  }

}
