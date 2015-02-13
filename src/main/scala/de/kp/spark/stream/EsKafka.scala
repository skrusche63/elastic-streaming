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

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka._

import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.{Configuration => HConf}

/**
 * EsStream provides base functionality for indexing transformed live streams 
 * from Apache Kafka with Elasticsearch; to appy a customized transformation,
 * the method 'transform' must be overwritten
 */
class EsKafka(@transient ctx:RequestContext,analyzer:StreamAnalyzer) extends EsConnector {
  
  private val elasticSettings = ctx.config.elastic
  private val kafkaSettings = ctx.config.kafka

  def run(index:String,mapping:String) {
 
    /*
     * Elasticsearch configuration
     */
    val elasticConfig = new HConf()                          

    elasticConfig.set("es.nodes", elasticSettings("es.nodes"))
    elasticConfig.set("es.port", elasticSettings("es.port"))
    
    elasticConfig.set("es.resource", String.format("""%s/%s""",index,mapping)) 
 
  /* Kafka configuration */
   val kafkaConfig = Map(
      "group.id" -> kafkaSettings("kafka.group"),
      
      "zookeeper.connect" -> kafkaSettings("kafka.zklist"),
      "zookeeper.connection.timeout.ms" -> kafkaSettings("kafka.timeout")
    
    )

    val kafkaTopics = kafkaSettings("kafka.topics").split(",").map((_,kafkaSettings("kafka.threads").toInt)).toMap   
 
    /*
     * The KafkaInputDStream returns a Tuple where only the second component
     * holds the respective message; we therefore reduce to a DStream[String]
     */
    val stream = KafkaUtils.createStream[String,String,StringDecoder,StringDecoder](
        ctx.streamingContext,
        kafkaConfig,
        kafkaTopics,StorageLevel.MEMORY_AND_DISK
        ).map(_._2)
       
    val transformed = if (analyzer == null) stream else analyzer.analyze(stream)
    
    /* Save to Elasticsearch */
    saveToES(transformed,elasticConfig)
     
    /* Start the streaming context and await termination */
    ctx.streamingContext.start()
    ctx.streamingContext.awaitTermination()

  }

}