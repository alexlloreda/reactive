package kvstore

import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props}
import kvstore.Arbiter._

import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart

import scala.annotation.tailrec
import akka.pattern.{ask, pipe}
import akka.actor.Terminated

import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.{ReceiveTimeout, SupervisorStrategy}
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
  // map from id to Persist message and Actor to respond to
  var persistMessages = Map.empty[Long, (Persist, ActorRef, Int)]
  var pendingReplicaAcks = Map.empty[Long, Set[ActorRef]]
  val persistence = context.actorOf(persistenceProps, "Persistence")

  override def preStart = arbiter ! Join

  override def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica(0))
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)
  }


  /* Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(k, v, id) => updateKV(k,Some(v),id)
    case Remove(k, id) => updateKV(k,None,id)
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)
    // Persistence Protocol
    case Persisted(key, id) =>
      for ((_,actor,_) <- persistMessages.get(id)) actor ! OperationAck(id)
      persistMessages = persistMessages - id
    case Replicated(key, id) =>
      for (
        s <- pendingReplicaAcks.get(id)
        if (s.contains(sender))
      ) {
        val ss = s - sender
        pendingReplicaAcks = pendingReplicaAcks.updated(id, ss)
        if (ss.isEmpty && persistMessages.get(id).equals(None)) {

        }
      }


    case ReceiveTimeout =>
      // TODO Need to send failure after 1 second if persist continues to fail
      for ((id,(msg,actorRef,attempts)) <- persistMessages) {
        if (attempts < 10) {
          persistence ! msg
          persistMessages = persistMessages updated(id,(msg,actorRef, attempts+1))
        }
        else {
          actorRef ! OperationFailed(id)
          persistMessages = persistMessages - id
        }
      }
  }

  private def updateKV(k: String, vOption: Option[String], id: Long) = {
    vOption match {
      case None => kv = kv - k
      case Some(v) => kv = kv.updated(k, v)
    }
    val msg = Persist(k, vOption, id)
    persistMessages = persistMessages updated(id,(msg, sender, 0))
    persistence ! msg
    context setReceiveTimeout(100.milliseconds)

    replicators foreach {_ ! Replicate(k, vOption, id)}
    pendingReplicaAcks = pendingReplicaAcks updated(id, replicators)
  }

  /* Behavior for the replica role. */
  def replica(expected: Long): Receive = {
    // KV Protocol
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)
    // Replication Protocol
    case Snapshot(key, valueOption, seq) =>
      if (seq < expected) sender ! SnapshotAck(key, seq)
      else if (seq == expected) {
        updateKV(key, valueOption, seq)
        replicators = replicators + sender()
      }
      // Ignore any request with seq > expected

    // Persistence Protocol
    case Persisted(key, seq) =>
      for ((msg, actor,_) <- persistMessages.get(seq)) actor ! SnapshotAck(key, seq)
      context.become(replica(expected + 1))
      persistMessages = persistMessages - seq
      context.setReceiveTimeout(Duration.Undefined)
    case ReceiveTimeout => for ((_,(msg,_,_)) <- persistMessages) persistence ! msg
  }

}
