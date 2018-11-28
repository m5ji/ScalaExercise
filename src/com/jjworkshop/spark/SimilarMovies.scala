package com.jjworkshop.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import scala.math.sqrt

object SimilarMovies {
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
  
  def parseLine(line: String) = {
    val fields = line.split("\t")
    val userID = fields(0).toInt
    val movieID = fields(1).toInt
    val rating = fields(2).toFloat
    
    (userID, (movieID, rating))
  }
  
  //pair: (userID, ((MovieID, rating), (MovieID, rating)))
  def parseJoined(pair: (Int, ((Int, Float),(Int,Float)))) = {
    val movie1 = pair._2._1
    val movie2 = pair._2._2
    
    val movie1ID = movie1._1
    val movie2ID = movie2._1
    
    val rating1 = movie1._2
    val rating2 = movie2._2
    if(movie1ID < movie2ID) {
      Some(((movie1ID, movie2ID), (rating1, rating2)))
    } else {
      None
    }
  }
  
  type RatingPair = (Float, Float)
  type RatingPairs = Iterable[RatingPair]
  def computeCosineSimilarity(ratingPairs:RatingPairs): (Double, Int) = {
    var numPairs:Int = 0
    var sum_xx:Double = 0.0
    var sum_yy:Double = 0.0
    var sum_xy:Double = 0.0
    
    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2
      
      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }
    
    val numerator:Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)
    
    var score:Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }
    
    return (score, numPairs)
  }
  
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "SimilarMovies")   
    
    //Broadcast movie names
    val movieNames = sc.broadcast(loadMovieNames)
    
    // Read each line into an RDD
    //u.data (User ID  Movie ID  Rating  Timestamp)
    val input = sc.textFile("./files/ml-100k/u.data")
    
    //Map each line to have userID => (movieID, rating))
    val parsed = input.map(parseLine)
    
    //userID => ((MovieID, rating), (MovieID, rating))
    val joined = parsed.join(parsed)
    
    //(movie1ID, movie2ID) => (rating1, rating2)
    val movieIDKeys = joined.flatMap(parseJoined)
    
    //(movie1ID, movie2ID) => [(rating1, rating2), (rating1, rating2),...]
    val group = movieIDKeys.groupByKey()
    
    //(movie1ID, movie2ID) => (score, numPairs)
    val similarityCalculation = group.mapValues(computeCosineSimilarity)
    
    val similarityCalculationWithMovieNames = similarityCalculation.map(
        x => ((movieNames.value(x._1._1), movieNames.value(x._1._2)), x._2))
    
    // Sort by first movie name of each key pair
    val results = similarityCalculationWithMovieNames.sortByKey()
    
    results.foreach(println)
    
  }
}