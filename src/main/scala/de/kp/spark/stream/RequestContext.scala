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

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext

class RequestContext(
  @transient val sparkContext:SparkContext,
  @transient val streamingContext:StreamingContext
  ) extends Serializable {

  val config = Configuration
  
}