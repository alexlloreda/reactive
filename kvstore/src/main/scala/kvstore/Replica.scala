package kvstore

import language.postfixOps

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.concurrent.Future

import akka.actor.Status.{Failure, Success}
import akka.actor.{Actor, ActorRef, OneForOneStrategy, PoisonPill, Props, ReceiveTimeout, SupervisorStrategy, Terminated}
import akka.pattern.ask
import akka.util.Timeout

import kvstore.Arbiter._
import kvstore.Persistence.Persisted

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

  case class InternalReply(caller: ActorRef, msg: Persisted)

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
  val persistor = context.actorOf(Persistor.props(persistenceProps), "Persistor")

  override def preStart = arbiter ! Join

  override def receive = get.orElse {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica(0))
  }

  // Partial function for chaining with other behaviours
  val get: Receive = { case Get(key, id) => sender ! GetResult(key, kv.get(key), id) }

  /* Behavior for  the leader role. */
  val leader: Receive = get.orElse {
    case Insert(k, v, id) =>
      updateKV(k,Some(v),id)
      updateActors(k,Some(v),id)

    case Remove(k, id) =>
      updateKV(k, None, id)
      updateActors(k, None, id)

    case Replicas(replicas) =>
      secondaries filterKeys(k => !(replicas contains k)) foreach(drp => drp._2 ! PoisonPill)
      secondaries = secondaries filterKeys(k => replicas contains k)

      val toAdd: Set[ActorRef] = (replicas - self) filter(k => !(secondaries contains k))

      toAdd.foreach(r => {
        val replicator = context.actorOf(Replicator.props(r))
        kv.foreach(p => { replicator ! Replicate(p._1, Some(p._2), p._1.hashCode) })
        secondaries = secondaries updated(r, replicator)
        replicators += replicator
      })
  }

  private def updateKV(k: String, vOption: Option[String], id: Long) = {
    vOption match {
      case None => kv -= k
      case Some(v) => kv = kv.updated(k, v)
    }
  }

  private def updateActors(k: String, vOption: Option[String], id: Long) = {
    val s = sender()
    implicit val timeout = Timeout(1 second)
    val f2: Set[Future[Any]] = replicators map {r => r ? Replicate(k, vOption, id)}
    //val f = Future.traverse(replicators)(r => r ? Replicate(k, vOption, id))
    val fp = persistor ? Persist(k, vOption, id)
    val ff = Future.sequence(f2 + fp)
    ff onSuccess { case _ => s ! OperationAck(id)}
    ff onFailure { case _ => s ! OperationFailed(id)}
  }

  /* Behavior for the replica role. */
  def replica(expected: Long): Receive = get.orElse {
    // Replication Protocol
    case Snapshot(key, valueOption, seq) =>
      if (seq < expected) sender ! SnapshotAck(key, seq)
      else if (seq == expected) {
        updateKV(key, valueOption, seq)
        persistor ! Persist(key, valueOption, seq)
        context.become(awaitingPersistence(sender, expected))
      }
      // Ignore any request with seq > expected
  }

  def awaitingPersistence(origSender: ActorRef, expected: Long): Receive = get.orElse {
    case Snapshot(k,v, seq) => if (seq < expected) sender ! SnapshotAck(k, seq)
    case Persisted(k, seq) =>
      origSender ! SnapshotAck(k, seq)
      context.become(replica(expected + 1))
  }
}
