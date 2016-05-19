package kvstore

import akka.actor.{Actor, ActorRef, Props, ReceiveTimeout}

import scala.concurrent.duration._
import scala.language.postfixOps

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }


  def receive: Receive = {
    case Replicate(key, valueOption, id) =>
      val seq = nextSeq
      replica ! Snapshot(key, valueOption, seq)
      acks = acks updated(seq, (sender, Replicate(key, valueOption, id)))
      context setReceiveTimeout(100 milliseconds)
      context become(pendingConfirmation(seq))
  }

  def pendingConfirmation(expected: Long): Receive = {
    case msg: Replicate =>
      val seq = nextSeq
      acks = acks updated(seq, (sender, msg))
      /* TODO Initial idea to implement batching
      for (
        (_,m) <- acks get expected
        if (m key equals(msg key))
      ) {
        pending = pending :+ Snapshot(msg.key, msg.valueOption, seq)
      }
      */
    case ReceiveTimeout =>
      for ((_,Replicate(k,v,id)) <- acks get expected) replica ! Snapshot(k,v,expected)
    case SnapshotAck(_, s) =>
      for ((sender, Replicate(k,v,id)) <- acks get s) {
        sender ! Replicated(k, id)
        acks = acks - s

        acks.get(expected + 1) match {
          case None =>
            context become receive
            context setReceiveTimeout Duration.Undefined
          case Some((_, Replicate (k, v, id))) =>
            replica ! Snapshot(k, v, expected + 1)
            context become pendingConfirmation(expected + 1)
        }
      }
  }
}
