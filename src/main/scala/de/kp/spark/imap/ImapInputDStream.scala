package de.kp.spark.imap
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

import javax.mail._

import org.apache.spark.storage.StorageLevel
 
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream

import org.apache.spark.Logging
import org.apache.spark.streaming.receiver.Receiver

class ImapInputDStream(
    @transient ssc_ : StreamingContext,
    user:String,
    password:String,
    folder:String,
    storageLevel:StorageLevel = StorageLevel.MEMORY_ONLY) extends ReceiverInputDStream[Email](ssc_) {
  
  override def getReceiver(): Receiver[Email] = new ImapReceiver(user,password,folder,storageLevel)

}

class ImapReceiver(
  user:String,
  password:String,
  folder:String,
  storageLevel:StorageLevel) extends Receiver[Email](storageLevel) with Logging {

  var readEmails:Int = 0
  var t:Thread = null
  
  val me = this
  val gmail = new GMailInbox(user,password)

  override def onStart(): Unit = {
    
    var t = new Thread(new Runnable {
      
      override def run():Unit = {
       
        while (true) {
         
          val messages = gmail.getMessages(folder,10,readEmails)
          readEmails += messages.length
          
          messages.foreach(store)
          Thread.sleep(2000)
        
        }
      
      }
    
    })
    
    t.run()
  
  }

  override def onStop(): Unit = {
    t.interrupt()
  }
}