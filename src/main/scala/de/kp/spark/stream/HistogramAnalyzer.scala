package de.kp.spark.stream
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Elastic-Streaming project
* (https://github.com/skrusche63/elastic-streaming).
* 
* Elastic-Streaming is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Elastic-Streaming is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Elastic-Streaming. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream

import com.twitter.algebird._

import org.json4s.DefaultFormats
import org.json4s.native.Serialization.write

import scala.util.parsing.json._

class HistogramAnalyzer(config:Map[String,String]) extends StreamAnalyzer(config) {

  override def analyze(stream:DStream[String]):DStream[String] = {
    
    implicit val formats = DefaultFormats    
    
    /* Mapify stream */
    val mapified = stream.map(json => {
      
      JSON.parseFull(json) match {
      
        case Some(map) => map.asInstanceOf[Map[String,String]]
        case None => Map.empty[String,String]
      
      }
      
    })

    val histo_field = config("histo_field") 
    
    /* Extract field values and compute support for each field value */
    val values = mapified.map(m => m(histo_field))
    val support = values.map(v => (v, 1)).reduceByKey((a, b) => a + b)

    /* The data type of the field value is a String */
    var global = Map[String,Int]()
    val monoid = new MapMonoid[String, Int]()    
    
    /* Determine Top K */
    support.foreachRDD(rdd => {
      
      if (rdd.count() != 0) {
        val partial = rdd.collect().toMap
        global = monoid.plus(global.toMap, partial)
      }
    
    })
    
    mapified.transform(rdd => {
      
      rdd.map(m => {
        
        val v = m(histo_field)
        val s = global(v)
        
        write(m ++ Map("histo_field" -> histo_field, "histo_valu" -> v, "histo_supp" -> s.toString))
        
      })
      
    })
    
  }  
}