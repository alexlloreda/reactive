package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  override def preStart = {
    arbiter ! Join
  }

  override def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica(0))
  }

  /* Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(k, v, id) => {
      kv = kv.updated(k,v)
      sender ! OperationAck(id)
    }
    case Remove(k, id) => {
      kv = kv - k
      sender ! OperationAck(id)
    }
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)
  }

  private def updateKV(k: String, vOption: Option[String]) = vOption match {
    case None => kv = kv - k
    case Some(v) => kv = kv.updated(k, v)
  }

  /* Behavior for the replica role. */
  def replica(expected: Long): Receive = {
    // KV Protocol
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)

    // Replication Protocol
    case Replicate(key, valueOption, id) => {
      updateKV(key, valueOption)
      sender ! Replicated(key, id)
    }
    case Snapshot(key, valueOption, seq) =>
      if (seq < expected) sender ! SnapshotAck(key, seq)
      else if (seq == expected) {
        updateKV(key, valueOption)
        context.become(replica(expected + 1))
        sender ! SnapshotAck(key, seq)
      }
      // Ignore any request with seq > expected
  }

}
