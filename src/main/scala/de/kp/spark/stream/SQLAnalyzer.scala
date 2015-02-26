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
import org.apache.spark.sql._

import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream

class SQLAnalyzer(config:Map[String,String]) extends StreamAnalyzer(config) {

  override def analyze(stream:DStream[String]):DStream[String] = {
    
    val sc = stream.context.sparkContext
    val sqlc = new SQLContext(sc)
    
    stream.transform(rdd => {
      
      val table = sqlc.jsonRDD(rdd)
      table.registerTempTable("logstash")
 
      val sqlreport = sqlc.sql("SELECT host, COUNT(host) AS host_c, AVG(lineno) AS line_a FROM logstash WHERE path = '/var/log/system.log' AND lineno > 70 GROUP BY host ORDER BY host_c DESC LIMIT 100")
      sqlreport.map(r => 
        String.format("""{"host":"%s","count":%s,"sum":%s}""",r(0).toString,r(1).toString,r(2).toString)
      )
      
    })
    
  }

}