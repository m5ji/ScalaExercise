package com.jjworkshop.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._


object PeopleSparkSQL {
  case class Person(ID:Int, name:String, age:Int, numFriends:Int)
  
  def personMapper (line: String): Person = {
    val fields = line.split(',')
    val ID = fields(0).toInt
    val name = fields(1).toString
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    
    val person:Person = Person(ID, name, age, numFriends)
    return person
  }
  
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("PeopleSparkSQL")
      .master("local[*]")
      .getOrCreate()
    
    val lines = spark.sparkContext.textFile("./files/fakefriends.csv")
    val people = lines.map(personMapper)
    
    // Infer the schema, and register the DataSet as a table.
    import spark.implicits._
    val peopleDataSet = people.toDS
    
    peopleDataSet.printSchema()
    
    peopleDataSet.createOrReplaceTempView("peopleTable")
    
    // SQL can be run over DataFrames that have been registered as a table
    val avgNumFriendsByAge = spark.sql("SELECT age, AVG(numFriends) FROM peopleTable GROUP BY age ORDER BY age")
    
    val results = avgNumFriendsByAge.collect()
    
    results.foreach(println)
    
    spark.stop()
  }
}