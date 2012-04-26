package com.stackmob.scaliak.mapreduce

import scalaz._
import Scalaz._
import com.basho.riak.client.raw._
import com.basho.riak.client.query.{ MapReduce, BucketKeyMapReduce, BucketMapReduce }
import com.basho.riak.pbc.mapreduce.{ MapReduceBuilder, JavascriptFunction }
import com.basho.riak.pbc.{ RequestMeta, MapReduceResponseSource }

import com.basho.riak.client.query.indexes.BinIndex
import com.basho.riak.client.query.indexes.IntIndex
import com.basho.riak.client.raw.query.indexes.IndexQuery
import com.basho.riak.client.raw.query.indexes.BinValueQuery

import com.stackmob.scaliak.{ ScaliakConverter, ScaliakResolver, ReadObject, ScaliakBucket }

/**
 * Created by IntelliJ IDEA.
 * User: jordanrw
 * Date: 12/23/11
 * Time: 11:16 PM
 */

sealed trait MapReduceJob {
  val phases: MapReducePhases
}

object MapReduceJob {

  def apply(p: MapReducePhases): MapReduceJob = new MapReduceJob {
    val phases = p
  }

  def apply(p: MapPhase): MapReduceJob = new MapReduceJob {
    val phases = List[Either[MapPhase, ReducePhase]](Left(p))
  }

  def apply(p: ReducePhase): MapReduceJob = new MapReduceJob {
    val phases = List[Either[MapPhase, ReducePhase]](Right(p))
  }
}

sealed trait MapReducePhase {
  def existingPhases: MapReducePhases

  def >>(next: MapPhase): List[Either[MapPhase, ReducePhase]] = add(Left(next))
  def >>(next: ReducePhase): List[Either[MapPhase, ReducePhase]] = add(Right(next))
  def >>(nexts: List[Either[MapPhase, ReducePhase]]): List[Either[MapPhase, ReducePhase]] = add(nexts)
  
  def add(next: Either[MapPhase, ReducePhase]): List[Either[MapPhase, ReducePhase]] = add(List[Either[MapPhase, ReducePhase]](next))
  def add(nexts: List[Either[MapPhase, ReducePhase]]): List[Either[MapPhase, ReducePhase]] = existingPhases |+| nexts
}

sealed trait MapPhase extends MapReducePhase {
  def fn: JavascriptFunction
  def keep: Boolean
  def arguments: Option[java.lang.Object]
}

object MapPhase {

  def apply(f: JavascriptFunction, k: Boolean = false, a: Option[java.lang.Object] = None): MapPhase = new MapPhase {
    val fn = f
    val keep = k
    val arguments = a
  }
}

sealed trait ReducePhase extends MapReducePhase {
  def fn: JavascriptFunction
  def keep: Boolean
  def arguments: Option[java.lang.Object]
}

object ReducePhase {
  def apply(f: JavascriptFunction, k: Boolean = false, a: Option[java.lang.Object] = None): ReducePhase = new ReducePhase {
    val fn = f
    val keep = k
    val arguments = a
  }
}

object MapValuesToJson {
  def apply() = MapPhase(JavascriptFunction.named("Riak.mapValuesJson"))
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