package com.jjworkshop.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.sql.Timestamp

object MostPopularMoviesSparkSQL {
  case class UserMovie(userID:Int, movieID:Int, rating:Int)
  case class MovieName(movieID:Int, name:String)
  
  def userMovieMapper (line: String): UserMovie = {
    val fields = line.split("\t")
    val userID = fields(0).toInt
    val movieID = fields(1).toInt
    val rating = fields(2).toInt
    
    val userMovie:UserMovie = UserMovie(userID, movieID, rating)
    return userMovie
  }
  
  def movieNameMapper (line: String): MovieName = {
    val fields = line.split('|')
    val movieID = fields(0).toInt
    val name = fields(1).toString
    
    val movieName:MovieName = MovieName(movieID, name)
    return movieName
  }
  
  def main(args: Array[String]) {
     // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("MostPopularMoviesSparkSQL")
      .master("local[*]")
      .getOrCreate()
    
    val lines = spark.sparkContext.textFile("./files/ml-100k/u.data")
    val userMoive = lines.map(userMovieMapper)
    
    // Infer the schema, and register the DataSet as a table.
    import spark.implicits._
    val userMoiveDataSet = userMoive.toDS.cache()
    
    userMoiveDataSet.printSchema()
    
    val movieNameLines = spark.sparkContext.textFile("./files/ml-100k/u.item")
    val movieNames = movieNameLines.map(movieNameMapper)
    
    val movieNamesDataSet = movieNames.toDS
    
    movieNamesDataSet.printSchema()
    
    val mostPopularMovies 
    = userMoiveDataSet
    .groupBy("movieID").count()
    .join(movieNamesDataSet, Seq("movieID"))
    .orderBy(desc("count"))
    .select("movieID", "name", "count").show()
    
    spark.stop()
  }
}