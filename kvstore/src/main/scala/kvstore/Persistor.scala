package kvstore

import akka.actor.Status.{Failure, Success}
import akka.actor.{Actor, ActorRef, Props, ReceiveTimeout}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._
import kvstore.Persistence.{Persist, Persisted}

object Persistor {
  def props(persistenceProps: Props): Props = Props(new Persistor(persistenceProps))
}
/**
  * Created by alex on 23/05/2016.
  */
class Persistor(val persistenceProps: Props) extends Actor {
  val persistence = context.actorOf(persistenceProps, "Persistence")

  override def receive: Receive = {
    case msg: Persist =>
      persistence ! msg
      context.setReceiveTimeout(100 millis)
      context.become(retrying(sender(), msg))
  }

  def retrying(origSender: ActorRef, msg: Any): Receive  = {
    case ReceiveTimeout => persistence ! msg
    case p: Persisted =>
      origSender ! p
      context.setReceiveTimeout(Duration.Undefined)
      context.become(receive)
  }
}

