package com.jjworkshop.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

object MostPopularMovies {
  
  def loadMovieNames(): Map[Int, String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    
    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()
    val lines = Source.fromFile("./files/ml-100k/u.item").getLines()
    
    for(line <- lines) {
      val fields = line.split('|')
      val movieID = fields(0).toInt
      val movieName = fields(1).toString
      movieNames += (movieID -> movieName)
    }
    return movieNames
  }
  
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MostPopularMovie")   
    
    // Read each line into an RDD
    //u.data (User ID  Movie ID  Rating  Timestamp)
    val input = sc.textFile("./files/ml-100k/u.data")
    
    //Map each line to have (movieID, 1)
    val parsed = input.map(x => (x.split("\t")(1).toInt, 1))
    
    //Reduce and sum by the same key
    val mostPopularMovies = parsed.reduceByKey((x,y) => x+y)
    
    //Sort by count
    val sorted = mostPopularMovies.sortBy(x => (x._2, x._1)).cache()
    
    //u.item (Movie ID|Movie Name|Release Date|...)
    val movieNames = sc.broadcast(loadMovieNames)
    
    //Map to have (movieName, count) by switching movieID with movieName
    val results = sorted.map(x => (movieNames.value(x._1), x._2))
    
    results.foreach(println)
    
  }
  
}