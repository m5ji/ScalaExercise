package com.jjworkshop.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object AmountSpentByCustomer {
  def parseLine (line: String) = {
    //(id, item id, amount spent)
    val fields = line.split(",")
    val customerID = fields(0).toInt
    val amountSpent = fields(2).toDouble
    
    (customerID, amountSpent)
  }
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "AmountSpentByCustomer")   
    
    // Read each line of my book into an RDD
    val input = sc.textFile("./files/customer-orders.csv")
    
    //Adding amount of each customer spent
    val totalAmount = input.map(parseLine).reduceByKey((x,y) => (x+y))
    
    //Set amounts to two decimals
    val twoDecimalsAmount = totalAmount.mapValues(x => BigDecimal(x).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
    
    //Sort the results by amount spent
    val sorted = twoDecimalsAmount.sortBy(x => (x._2, x._1))
    
    val results = sorted.collect()
    // Print the results.
    results.foreach(println)
  }
}