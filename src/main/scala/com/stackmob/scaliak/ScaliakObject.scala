package com.stackmob.scaliak

import scalaz._
import Scalaz._
import com.basho.riak.client.cap.VClock
import com.basho.riak.client.IRiakObject
import com.basho.riak.client.builders.RiakObjectBuilder
import com.basho.riak.client.http.util.{Constants => RiakConstants}

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/10/11
 * Time: 10:45 AM
 */

case class ScaliakObject(key: String,
                         bucket: String,
                         contentType: String,
                         vClock: VClock,
                         vTag: Option[String],
                         bytes: Array[Byte]) {

  val vClockString = vClock.asString

  // TODO: probably should move, leaving for now since its used in a bunch of places
  def getBytes = bytes

  def stringValue = new String(bytes)

}

object ScaliakObject {
  implicit def IRiakObjectToScaliakObject(obj: IRiakObject): ScaliakObject = {
    ScaliakObject(
      key = obj.getKey,
      bytes = obj.getValue,
      bucket = obj.getBucket,
      vClock = obj.getVClock,
      vTag = Option(obj.getVtag),
      contentType = obj.getContentType
    )
  }
  
  implicit def ScaliakObjectToIRiakObject(obj: ScaliakObject): IRiakObject = {
    val base = (RiakObjectBuilder.newBuilder(obj.bucket, obj.key) 
      withContentType obj.contentType 
      withVClock obj.vClock
      withValue obj.getBytes)
    obj.vTag foreach { base withVtag  _ }
    base.build()
  }
}

sealed trait PartialScaliakObject {
  def _key: String
  def _vTag: Option[String]
  def _bytes: Array[Byte]
  def _contentType: Option[String]

  def asRiak(bucket: String, vClock: VClock): IRiakObject = {
    (RiakObjectBuilder.newBuilder(bucket, _key)
      withVClock vClock
      withContentType (_contentType | RiakConstants.CTYPE_TEXT_UTF8)
      withValue _bytes
      withVtag (_vTag | null)).build()

  }
}

object PartialScaliakObject {
  
  def apply(key: String, value: Array[Byte], contentType: Option[String] = None, vTag: Option[String] = None) = new PartialScaliakObject {
    def _key = key
    def _bytes = value
    def _contentType = contentType
    def _vTag = vTag
  }
  
}