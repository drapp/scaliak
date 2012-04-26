package com.stackmob.scaliak
package domainwrapper

import scalaz._
import Scalaz._
import effects._ // not necessary unless you want to take advantage of IO monad
import com.basho.riak.client.convert._

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.{ DeserializationConfig, ObjectMapper }

import com.basho.riak.client.http.util.{ Constants ⇒ RiakConstants }
import com.basho.riak.client.{ RiakLink, IRiakObject }
import com.basho.riak.client.query.indexes.{ RiakIndexes, IntIndex, BinIndex }

import java.text.SimpleDateFormat
import java.util.Date
import org.joda.time._
import mapreduce._

import scala.collection.JavaConverters._

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/16/11
 * Time: 2:00 PM
 */

abstract class DomainObject {
  def key: String
}

//case class DomainObject(val key: String, val value: String)
abstract class DomainObjectWrapper[ObjectType <: DomainObject](val clazz: Class[ObjectType], bucketName: Option[String] = None) {

  val objectMapper = new ObjectMapper
  objectMapper.registerModule(DefaultScalaModule)
  objectMapper.registerModule(new com.basho.riak.client.convert.RiakJacksonModule());

  objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  val defaultDateTimeFormat = new java.text.SimpleDateFormat("yyyy-MM-ddTHH:mm:ssZ")
  val localTimeZone = DateTimeZone.getDefault
  objectMapper.getSerializationConfig().setDateFormat(defaultDateTimeFormat)
  objectMapper.getDeserializationConfig().setDateFormat(defaultDateTimeFormat)

  val usermetaConverter: UsermetaConverter[ObjectType] = new UsermetaConverter[ObjectType]
  val riakIndexConverter: RiakIndexConverter[ObjectType] = new RiakIndexConverter[ObjectType]
  val riakLinksConverter: RiakLinksConverter[ObjectType] = new RiakLinksConverter[ObjectType]

  val client = Scaliak.pbClient("http://localhost:8091/riak")
  client.generateAndSetClientId()

  implicit val domainConverter: ScaliakConverter[ObjectType] = ScaliakConverter.newConverter[ObjectType](
    (o: ReadObject) ⇒ {
      val domObject = objectMapper.readValue(o.stringValue, clazz)
      usermetaConverter.populateUsermeta(o.metadata.asJava, domObject)
      riakIndexConverter.populateIndexes(buildIndexes(o.binIndexes, o.intIndexes), domObject)
      riakLinksConverter.populateLinks(((o.links map { _.list map { l ⇒ new RiakLink(l.bucket, l.key, l.tag) } }) | Nil).asJavaCollection, domObject)
      KeyUtil.setKey(domObject, o.key)
      domObject.successNel
    },
    (o: ObjectType) ⇒ {
      val value = objectMapper.writeValueAsBytes(o)
      val key = KeyUtil.getKey(o, o.key)
      val indexes = riakIndexConverter.getIndexes(o)
      val links = (riakLinksConverter.getLinks(o).asScala map { l ⇒ l: ScaliakLink }).toList.toNel
      val metadata = usermetaConverter.getUsermetaData(o).asScala.toMap
      val binIndexes = indexes.getBinIndexes().asScala.mapValues(_.asScala.toSet).toMap
      val intIndexes = indexes.getIntIndexes().asScala.mapValues(_.asScala.map(_.intValue()).toSet).toMap

      WriteObject(key, value, contentType = "application/json", metadata = metadata, binIndexes = binIndexes, intIndexes = intIndexes, links = links)
    }
  )

  private def buildIndexes(binIndexes: Map[BinIndex, Set[String]], intIndexes: Map[IntIndex, Set[Int]]): RiakIndexes = {
    val tempBinIndexes: java.util.Map[BinIndex, java.util.Set[String]] = new java.util.HashMap[BinIndex, java.util.Set[String]]()
    val tempIntIndexes: java.util.Map[IntIndex, java.util.Set[java.lang.Integer]] = new java.util.HashMap[IntIndex, java.util.Set[java.lang.Integer]]()
    for { (k, v) ← binIndexes } {
      val set: java.util.Set[String] = new java.util.HashSet[String]()
      v.foreach(set.add(_))
      tempBinIndexes.put(k, set)
    }
    for { (k, v) ← intIndexes } {
      val set: java.util.Set[java.lang.Integer] = new java.util.HashSet[java.lang.Integer]()
      v.foreach(set.add(_))
      tempIntIndexes.put(k, set)
    }

    new RiakIndexes(tempBinIndexes, tempIntIndexes)
  }

  // Default to lower case string representation of the class name => UserProfile: userprofile
  val bucket = client.bucket(bucketName.getOrElse(clazz.getSimpleName.toString.toLowerCase)).unsafePerformIO match {
    case Success(b) ⇒ b
    case Failure(e) ⇒ throw e
  }

  def fetch(key: String) = {
    bucket.fetch(key).unsafePerformIO match {
      case Success(mbFetched) ⇒ {
        mbFetched
      }
      case Failure(es) ⇒ throw es.head
    }
  }
  
  def fetch(keys: String*) = bucket.mapReduce(MapReduceJob(MapValuesToJson()), Some(Map(bucket.name -> keys.toSet)))    

  def deleteWithKeys(keys: String*) = {
    keys.foreach(key ⇒ delete(key))
  }

  def store(domainObject: ObjectType) = {
    bucket.store(domainObject).unsafePerformIO match {
      case Success(mbFetched) ⇒ {
        Unit
      }
      case Failure(es) ⇒ throw es.head
    }
  }

  def delete(domainObject: ObjectType) = {
    bucket.delete(domainObject).unsafePerformIO match {
      case Failure(es) ⇒ throw es
    }
  }

  def delete(key: String) = {
    bucket.deleteByKey(key).unsafePerformIO match {
      case Failure(es) ⇒ throw es
    }
  }

  def deleteWithCheck(domainObject: ObjectType, checkFunction: ⇒ ObjectType ⇒ Boolean) = {
    if (checkFunction(domainObject))
      delete(domainObject)
    else
      throw new Exception("Could not delete object, check did not succeed")
  }

  def fetchKeysForIndexWithValue(idx: String, idv: String) = {
    bucket.fetchIndexByValue(idx, idv).unsafePerformIO match {
      case Success(mbFetched) ⇒ mbFetched
      case Failure(es)        ⇒ throw es
    }
  }

  def fetchKeysForIndexWithValue(idx: String, idv: Int) = {
    bucket.fetchIndexByValue(idx, idv).unsafePerformIO match {
      case Success(mbFetched) ⇒ mbFetched
      case Failure(es)        ⇒ throw es
    }
  }
}