package de.kp.spark.stream

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext

class RequestContext(
  @transient val sparkContext:SparkContext,
  @transient val streamingContext:StreamingContext
  ) extends Serializable {

  val config = Configuration
  
}