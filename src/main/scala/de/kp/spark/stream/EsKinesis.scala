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

import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel

import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.kinesis._

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain

import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream

import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.io.{MapWritable,NullWritable,Text}

import org.elasticsearch.hadoop.mr.EsOutputFormat

import scala.util.parsing.json._

class EsKinesis(@transient ctx:RequestContext) extends Serializable {

  private val elasticSettings = ctx.config.elastic
  private val kinesisSettings = ctx.config.kinesis
  /*
   * The Kinesis stream that this object receives from.
   * The application name used in the streaming context 
   * becomes the Kinesis application name. 
   * 
   * It must be unique for a given account and region.
   * 
   * The Kinesis backend automatically associates the 
   * application name to the Kinesis stream using a 
   * DynamoDB table (always in the us-east-1 region) 
   * created during Kinesis Client Library initialization.
   * 
   * Changing the application name or stream name can lead 
   * to Kinesis errors in some cases. 
   * 
   * If you see errors, you may need to manually delete the 
   * DynamoDB table.
   */
  private val streamName = kinesisSettings("kinesis.stream.name")
  /*
   * Valid Kinesis endpoints URL
   * (e.g. https://kinesis.us-east-1.amazonaws.com)
   */
  private val endpoint = kinesisSettings("kinesis.endpoint")

  /*
   * The DefaultAWSCredentialsProviderChain searches for credentials
   * in the following order of precedence:
   * 
   * Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_KEY
   * 
   * Java System Properties - aws.accessKeyId and aws.secretKey
   * 
   * Credential profiles file - default location (~/.aws/credentials) shared by all AWS SDKs
   * Instance profile credentials - delivered through the Amazon EC2 metadata service
   * 
   */
  private val aws_key = kinesisSettings("kinesis.aws.key")
  System.setProperty("aws.accessKeyId",aws_key)      

  private val aws_secret = kinesisSettings("kinesis.aws.secret")
  System.setProperty("aws.secretKey",aws_secret)      
  
  def run(index:String,mapping:String) {
  
    val kinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain())
    /* 
     * Determine the number of shards from the stream 
     */
    kinesisClient.setEndpoint(endpoint)
    val numShards = kinesisClient.describeStream(streamName).getStreamDescription().getShards().size()
    
    /* 
     * We're going to create 1 Kinesis Worker/Receiver/DStream for each shard. 
     */
    val numStreams = numShards

    /*
     * Elasticsearch configuration
     */
    val elasticConfig = new HConf()                          

    elasticConfig.set("es.nodes", elasticSettings("es.nodes"))
    elasticConfig.set("es.port", elasticSettings("es.port"))
    
    elasticConfig.set("es.resource", String.format("""%s/%s""",index,mapping)) 
    
    /*
     * The interval (e.g., Duration(2000) = 2 seconds) at which 
     * the Kinesis Client Library saves its position in the stream. 
     * 
     * For starters, set it to the same as the batch interval of the 
     * streaming application.
     */
    val interval = kinesisSettings("kinesis.checkpoint.interval").toInt
    /*
     * Can be either InitialPositionInStream.TRIM_HORIZON or InitialPositionInStream.LATEST 
     * (see Kinesis Checkpointing section and Amazon Kinesis API documentation for more details).
     */
    val initialPosition = InitialPositionInStream.LATEST  
    val storageLevel = StorageLevel.MEMORY_AND_DISK_2
  
    /* 
     * Create the same number of Kinesis DStreams/Receivers as Kinesis stream's shards 
     */  
    val kinesisStreams = (0 until numStreams).map {i =>
    
      KinesisUtils.createStream(
        ctx.streamingContext, 
        streamName, 
        endpoint, 
        Duration(interval), 
        initialPosition, 
        storageLevel)
  
    }
  
    /* Unify streams */
    val unifiedStream = ctx.streamingContext.union(kinesisStreams)
    
    /*
     * Convert each line of Array[Byte] into a String
     */
    unifiedStream.map(bytes => new String(bytes, "UTF-8")).foreachRDD(rdd => {
      
      val messages = rdd.map(prepare)
      messages.saveAsNewAPIHadoopFile("-",classOf[NullWritable],classOf[MapWritable],classOf[EsOutputFormat],elasticConfig)          
      
    })
    
    /* Start the streaming context and await termination */
    ctx.streamingContext.start()
    ctx.streamingContext.awaitTermination()
  
  }
  
  private def prepare(message:String):(Object,Object) = {
      
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