package de.kp.spark.stream

import scala.util.parsing.json._

import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext._

import org.apache.spark.storage.StorageLevel

import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.io.{MapWritable,NullWritable,Text}

import org.elasticsearch.hadoop.mr.EsOutputFormat

import com.datasift.client.stream.Interaction
import de.kp.spark.datasift.DatasiftUtils

class EsDatasift(@transient ctx:RequestContext) extends Serializable {

  private val elasticSettings = ctx.config.elastic
  private val datasiftSettings = ctx.config.datasift

  def run(index:String,mapping:String) {
    /*
     * Elasticsearch configuration
     */
    val elasticConfig = new HConf()                          

    elasticConfig.set("es.nodes", elasticSettings("es.nodes"))
    elasticConfig.set("es.port", elasticSettings("es.port"))
    
    elasticConfig.set("es.resource", String.format("""%s/%s""",index,mapping)) 

    /*
     * Datasift configuration
     */
    val datasift_key = datasiftSettings("datasift.key")
    val datasift_user = datasiftSettings("datasift.user")
    
    val datasift_query = datasiftSettings("datasift.query")
    val stream = DatasiftUtils.createStream(
      ctx.streamingContext,
      datasift_key,
      datasift_user,
      datasift_query,
      StorageLevel.MEMORY_AND_DISK
      )
    
    stream.foreachRDD(rdd => {
      val messages = rdd.map(prepare)
      messages.saveAsNewAPIHadoopFile("-",classOf[NullWritable],classOf[MapWritable],classOf[EsOutputFormat],elasticConfig)          
    })
    
    /* Start the streaming context and await termination */
    ctx.streamingContext.start()
    ctx.streamingContext.awaitTermination()

  }
  
  private def prepare(interaction:Interaction):(Object,Object) = {
      
    val message = interaction.toString()
    
    val m = JSON.parseFull(message) match {
      case Some(map) => map.asInstanceOf[Map[String,String]]
      case None => Map.empty[String,String]
    }

    val kw = NullWritable.get
    
    val vw = new MapWritable
    for ((k, v) <- m) vw.put(new Text(k), new Text(v))
    
    (kw, vw)
    
  }
  
}