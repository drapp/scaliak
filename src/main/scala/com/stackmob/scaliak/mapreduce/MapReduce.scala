package com.stackmob.scaliak.mapreduce

import scalaz._
import Scalaz._
import com.basho.riak.client.raw._
import com.basho.riak.client.query.{MapReduce, BucketKeyMapReduce, BucketMapReduce}
import com.basho.riak.pbc.mapreduce.{MapReduceBuilder, JavascriptFunction}
import com.basho.riak.pbc.{RequestMeta, MapReduceResponseSource}

import com.stackmob.scaliak.{ScaliakConverter, ScaliakResolver, ReadObject, ScaliakBucket}

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/23/11
 * Time: 11:16 PM
 */

sealed trait MapReduceJob extends MapReduceJobOperators {
  def phases = existingPhases
  val existingPhases = List[Either[MapPhase, ReducePhase]]()
}

trait MapReduceJobOperators {
  def existingPhases: MapReducePhases
  
  def >>(next: MapReducePhase): MapReducePhases = add(next)
  def >>(nexts: MapReducePhases): MapReducePhases  = add(nexts)
  def add(next: MapReducePhase): MapReducePhases  = add(next.wrapNel)
  def add(nexts: MapReducePhases): MapReducePhases  = existingPhases |+| nexts
}

sealed trait MapReducePhase

sealed trait MapPhase extends MapReducePhase  {
  def fn: JavascriptFunction
  def keep: Boolean
  def arguments: Option[java.lang.Object]
}

object MapPhase {

  def apply(f: JavascriptFunction, k: Boolean, a: Option[java.lang.Object]): MapPhase = new MapPhase{
    val fn = f
    val keep = k
    val arguments = a 
  }
}

sealed trait ReducePhase extends MapReducePhase{
  def fn: JavascriptFunction
  def keep: Boolean
  def arguments: Option[java.lang.Object]
}

object ReducePhase {
  def apply(f: JavascriptFunction, k: Boolean, a: Option[java.lang.Object]): ReducePhase = new ReducePhase{
    val fn = f
    val keep = k
    val arguments = a 
  }
}

//class LinkWalkStepTuple3(value: (String, String, Boolean)) {
//  def toLinkWalkStep = LinkWalkStep(value._1, value._2, value._3)
//}
//
//class LinkWalkStepTuple2(value: (String, String)) {
//  def toLinkWalkStep = LinkWalkStep(value._1, value._2)
//}
//
//class LinkWalkStepsW(values: LinkWalkSteps) extends LinkWalkStepOperators {
//  val existingSteps = values
//}
//
//class LinkWalkStartTuple(values: (ScaliakBucket, ReadObject)) {
//  private val bucket = values._1
//  private val obj = values._2
//
//  def linkWalk[T](steps: LinkWalkSteps)(implicit converter: ScaliakConverter[T]) = {
//    bucket.linkWalk(obj, steps)
//  }
//}