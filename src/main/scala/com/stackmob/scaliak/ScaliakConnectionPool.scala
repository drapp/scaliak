package com.stackmob.scaliak

import org.apache.commons.pool._
import org.apache.commons.pool.impl._

import com.basho.riak.client.raw.http.HTTPClientAdapter
import com.basho.riak.client.raw.pbc.PBClientAdapter
import com.basho.riak.client.raw.RawClient

private class ScaliakPbClientFactory(host: String, port: Int, httpPort: Int) extends PoolableObjectFactory {
  def makeObject = Scaliak.pbClient(host, port, httpPort) 

  def destroyObject(sc: Object): Unit = { 
	  sc.asInstanceOf[RawClient].shutdown
	  // Methods for client destroying
  }

  def passivateObject(sc: Object): Unit = {} 
  def validateObject(sc: Object) = {
    try {
      sc.asInstanceOf[RawClient].ping()
      true
    }
    catch {
      case e => false 
    }
  }

  def activateObject(sc: Object): Unit = {}
}

class ScaliakPbClientPool(host: String, port: Int, httpPort: Int) {
  val pool = new StackObjectPool(new ScaliakPbClientFactory(host, port, httpPort))
  override def toString = host + ":" + String.valueOf(port)

  def withClient[T](body: RawClient => T) = {
    val client = pool.borrowObject.asInstanceOf[RawClient]
    try {
      body(client)
    } finally {
      pool.returnObject(client)
    }
  }

  // close pool & free resources
  def close = pool.close
}