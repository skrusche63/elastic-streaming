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

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.serializer.KryoSerializer

import org.apache.spark.streaming.{Seconds,StreamingContext}

import scala.collection.JavaConversions._

trait SparkBase {
  
  protected def createStxLocal(name:String,config:Map[String,String]):StreamingContext = {

    val sc = createCtxLocal(name,config)
    
    /*
     * Batch duration is the time duration spark streaming uses to 
     * collect spark RDDs; with a duration of 5 seconds, for example
     * spark streaming collects RDDs every 5 seconds, which then are
     * gathered int RDDs    
     */
    val batch  = config("spark.batch.duration").toInt    
    new StreamingContext(sc, Seconds(batch))

  }
  
  protected def createCtxLocal(name:String,config:Map[String,String]):SparkContext = {

    /* Extract Spark related properties from the Hadoop configuration */
    for (prop <- config) {
      System.setProperty(prop._1,prop._2)      
    }

    val runtime = Runtime.getRuntime()
	runtime.gc()
		
	val cores = runtime.availableProcessors()
		
	val conf = new SparkConf()
	conf.setMaster("local["+cores+"]")
		
	conf.setAppName(name);
    conf.set("spark.serializer", classOf[KryoSerializer].getName)		
    
    /* Set the Jetty port to 0 to find a random port */
    conf.set("spark.ui.port", "0")        
        
	new SparkContext(conf)
		
  }

  protected def createStxRemote(name:String,config:Map[String,String]):SparkContext = {
    /* Not implemented yet */
    null
  }

  protected def createCtxRemote(name:String,config:Map[String,String]):SparkContext = {
    /* Not implemented yet */
    null
  }

}