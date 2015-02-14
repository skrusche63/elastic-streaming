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

import java.util.Properties

import javax.mail._
import javax.mail.event.{ConnectionEvent,ConnectionListener,StoreEvent,StoreListener}

import scala.collection.immutable.Seq

class GMailInbox(user:String,password:String) extends Serializable {

  val props = new Properties()

  props.setProperty("mail.imap.host","smtp.gmail.com")
  props.setProperty("mail.imap.socketFactory.port","993")
  
  props.setProperty("mmail.imap.socketFactory.class","javax.net.ssl.SSLSocketFactory")
  
  props.setProperty("mmail.imap.auth","true")
  props.setProperty("mmail.imap.port","993")


  private def getConnectedStore():Store =  {
    
    val session = Session.getDefaultInstance(props, null)
    val store = session.getStore("imaps");
    
    store.connect("smtp.gmail.com", user, password);
    store
  
  }

  def listDefaultFolder : Seq[String] = {

    val store = getConnectedStore()
    val ret = store.getDefaultFolder.list().toList.map(_.getName)
    
    store.close();
    ret
  
  }

  def listFolder(folderName:String):Seq[String] = {
    
    val store= getConnectedStore()
    val ret =  store.getFolder(folderName).list().toList.map(_.getName)
    
    store.close()
    ret
  
  }

  def getMessages(folder:String="inbox",take:Int = Int.MaxValue, drop:Int = 0) : Seq[Email] = {

    val store = getConnectedStore()
    val inbox = store.getFolder(folder)
    
    inbox.open(Folder.READ_ONLY)
    
    val ret = inbox.getMessages.toStream.map{ (x:Message) =>
      
      val from: String = x.getFrom match {
      
        case null => "Null"
        
        case Array() => "Empty"
        
        case all @ Array(_*) => all.mkString(",")
        
        case _ => "Unknown"
      
      }
      
      if (x.getContent.isInstanceOf[Multipart]) {
        
        val multipart = x.getContent.asInstanceOf[Multipart]
        val p:Seq[EmailContent] = (0 until multipart.getCount) flatMap { bodyIndex =>
          
          val bodyPart = multipart.getBodyPart(bodyIndex)
          if(bodyPart.getContentType.contains(`TEXT/HTML`.toString))
            Some(EmailContent(bodyPart.getContent.asInstanceOf[String],`TEXT/HTML`))
          
          else if(bodyPart.getContentType.contains(`TEXT/PLAIN`.toString))
            Some(EmailContent(bodyPart.getContent.asInstanceOf[String],`TEXT/PLAIN`))
          
          else None
        
        }
        
        Email(from,x.getSubject,p)
      
      } else {
        
        val mimeType = if(x.getContentType.contains(`TEXT/HTML`.toString)) `TEXT/HTML` else `TEXT/PLAIN`
        Email(from,x.getSubject,Seq(EmailContent(x.getContent.asInstanceOf[String],mimeType)))
      
      }
    }
    
    .drop(drop)
    .take(take)
    .toList
    
    store.close()
    ret
  
  }
}