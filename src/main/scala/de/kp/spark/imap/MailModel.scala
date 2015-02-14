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

sealed trait MimeType

case object `TEXT/PLAIN` extends MimeType
case object `TEXT/HTML` extends MimeType

case class Email(
  from:String,
  subject:String,
  content:Seq[EmailContent]
)

case class EmailContent(
  content:String,
  msgType:MimeType
) {

  override def toString():String = {
    s"EmailContent(${content} (...),$msgType})"
  }

}