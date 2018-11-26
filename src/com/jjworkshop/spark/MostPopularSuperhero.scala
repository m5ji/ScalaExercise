package com.jjworkshop.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object MostPopularSuperhero {
  
  def parseNames(line: String) = {
    val pattern = "\".*\"".r
    val fields = line.split("\\s+")
    val heroID = fields(0).toInt
    //Get hero name in double quotations
    val heroName = pattern.findFirstIn(line).get.replaceAll("\"", "")
    
    (heroID, heroName)
  }
  
  def parseConnection(line: String) = {
    val fields = line.split("\\s+")
    val heroID = fields(0).toInt
    val connections = fields.length - 1
    
    (heroID, connections)
  }
  
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "MostPopularSuperhero")
    
    //marvel-names (heroID "heroName")
    val names = sc.textFile("./files/marvel-names.txt").map(parseNames).cache()
    
    //marvel-graph (heroID heroID heroID ...)
    val lines = sc.textFile("./files/marvel-graph.txt")
    
    //Map lines into (heroID, connections)
    val connections = lines.map(parseConnection)
    
    //Reduce and sum connection counts by heroID
    val connectionsByHero = connections.reduceByKey((x,y) => x+y)
    
    //Sort by connections number
    val sorted = connectionsByHero.sortBy(x => (x._2, x._1), false)
    
    //Get the first line in the sorted results
    val mostPopularHero = sorted.first()
    
    //Get the name of most popular hero
    val mostPopularHeroName = names.lookup(mostPopularHero._1)
    
    println(s"${mostPopularHeroName(0)} is the most popular superhero with ${mostPopularHero._2} co-appearances.")
  }
}