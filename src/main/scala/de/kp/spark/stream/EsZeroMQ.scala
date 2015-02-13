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

import akka.util.ByteString
import akka.zeromq.Subscribe

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.zeromq._

import org.apache.hadoop.conf.{Configuration => HConf}

/**
 * The EsZeroMQ class can be used in combination with
 * logstash and the respective ZMQ output; the benefit
 * from this class is, that additional analysis can be
 * applied to the stream before indexing the results 
 * in an Elasticsearch cluster
 */
class EsZeroMQ(@transient ctx:RequestContext,analyzer:StreamAnalyzer) extends EsConnector {
  
  private val elasticSettings = ctx.config.elastic
  private val zeromqSettings = ctx.config.zeromq

  def run(index:String,mapping:String) {
 
    /*
     * Elasticsearch configuration
     */
    val elasticConfig = new HConf()                          

    elasticConfig.set("es.nodes", elasticSettings("es.nodes"))
    elasticConfig.set("es.port", elasticSettings("es.port"))
    
    elasticConfig.set("es.resource", String.format("""%s/%s""",index,mapping)) 

    /*
     * ZeroMQ configuration
     */
    val endpointZMQ = zeromqSettings("zeromq.endpoint")
    val topicZMQ = zeromqSettings("zeromq.topic")
    
    def bytesToStringIterator(x: Seq[ByteString]) = (x.map(_.utf8String)).iterator

    val stream = ZeroMQUtils.createStream(
        ctx.streamingContext, 
        endpointZMQ, 
        Subscribe(topicZMQ), 
        bytesToStringIterator _)
       
    val transformed = if (analyzer == null) stream else analyzer.analyze(stream)
    
    /* Save to Elasticsearch */
    saveToES(transformed,elasticConfig)
    
    /* Start the streaming context and await termination */
    ctx.streamingContext.start()
    ctx.streamingContext.awaitTermination()
    
  }
  
}