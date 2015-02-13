package de.kp.spark.datasift
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
 * 
 * This file is part of the Spark-DataSift project
 * (https://github.com/skrusche63/spark-datasift).
 * 
 * Spark-DataSift is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 * 
 * Spark-DataSift is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with
 * Spark-DataSift. 
 * 
 * If not, see <http://www.gnu.org/licenses/>.
 */

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.storage.StorageLevel

import org.apache.spark.Logging
import org.apache.spark.streaming.receiver.Receiver

import com.datasift.client.{DataSiftClient,DataSiftConfig}
import com.datasift.client.core.Stream
import com.datasift.client.stream._

class DatasiftInputDStream(
    @transient ssc_ : StreamingContext,
    appkey:String,
    user:String,
    query:String,
    storageLevel: StorageLevel
    
  ) extends ReceiverInputDStream[Interaction](ssc_)  {

  override def getReceiver(): Receiver[Interaction] = {
    new DatasiftReceiver(appkey,user,query,storageLevel)
  }

}

class DatasiftReceiver(     
    appkey:String,
    user:String,
    query:String,
    storageLevel: StorageLevel
  ) extends Receiver[Interaction](storageLevel) with Logging {
    
    val config = new DataSiftConfig(user,appkey)
    private var datasift:DataSiftClient = _
    
  def onStart() {
    
    try {
      
      val newDataSift = new DataSiftClient(config)
      val stream = datasift.compile(query).sync()  
    
      /*
       * Error handling
       */
      newDataSift.liveStream().onError(new ErrorListener() {
        def exceptionCaught(t:Throwable) {
          logError("DataSift stream error detected.",t)
          restart("Error receiving tweets", t)
        }        
      })
      
      /*
       * Delete handling
       */
      newDataSift.liveStream().onStreamEvent(new StreamEventListener() {
        def onDelete(di:DeletedInteraction) {
    	  /* no nothing */
        }        
      })
      
      /*
       * Stream handling
       */
      newDataSift.liveStream().subscribe(new StreamSubscription(stream){
        def onDataSiftLogMessage(di:DataSiftMessage) {
          
          val msg = if (di.isError()) {
            "Error"  + ":\n" + di
          } else if (di.isInfo()) {
            "Info" + ":\n" + di
          } else {
            "Warning" + ":\n" + di
          }
 
          logInfo(msg)
          
        }
 
        def onMessage(interaction:Interaction) {
          store(interaction)
        }
        
      })

      setDataSift(newDataSift)
      logInfo("DataSift receiver started")
      
    } catch {
      case e: Exception => restart("Error starting DataSift stream", e)        
    }
    
  }

  def onStop() {
    setDataSift(null)
    logInfo("DataSift receiver stopped")    
  }
  
  private def setDataSift(newDataSift:DataSiftClient) = synchronized {
    if (datasift != null) {
      datasift.shutdown()
    }
    datasift = newDataSift
  }

}
