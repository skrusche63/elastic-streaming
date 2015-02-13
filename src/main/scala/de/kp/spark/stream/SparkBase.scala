package de.kp.spark.stream

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