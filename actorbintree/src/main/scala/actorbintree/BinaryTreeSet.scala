/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  var pendingQueue = Queue.empty[Operation]

  def receive = normal

  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case (op: Operation) => root ! op
    case GC => {
      val tempRoot = createRoot
      context.become(garbageCollecting(tempRoot))
      root ! CopyTo(tempRoot)
    }
  }

  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case (op: Operation) => pendingQueue = pendingQueue.enqueue(op)
    case CopyFinished => {
      root ! PoisonPill
      root = newRoot
      pendingQueue.foreach( op => root ! op)
      context.become(normal)
      pendingQueue = Queue.empty[Operation]
    }
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  def receive = normal

  private def opOnNode(op: Operation): Unit = op match {
    case Contains(ref, id, el) => ref ! ContainsResult(id, !removed)
    case Insert(ref, id, el) => removed = false; ref ! OperationFinished(id)
    case Remove(ref, id, el) => removed = true; ref ! OperationFinished(id)
  }

  private def opOnChild(op: Operation, pos: Position): Unit = subtrees get pos match {
    case Some(child) => child ! op
    case None => op match {
      case Contains(ref, id, el) => ref ! ContainsResult(id, false)
      case Insert(ref, id, el) => {
        subtrees = subtrees.updated(pos, context.actorOf(props(el, false)))
        ref ! OperationFinished(id)
      }
      case Remove(ref, id, el) => ref ! OperationFinished(id)
    }
  }

  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case op: Operation => {
      if (op.elem == elem) opOnNode(op)
      else if (op.elem < elem) opOnChild(op, Left)
      else opOnChild(op, Right)
    }
    case CopyTo(ref) => {
      if (!removed) ref ! Insert(self, 1, elem)
      val children = subtrees.values.toSet
      children.foreach(c => c ! CopyTo(ref))

      if (removed && children.isEmpty) context.parent ! CopyFinished
      else context.become(copying(children, removed))
    }
  }

  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(id) => {
      if (expected.isEmpty) context.parent ! CopyFinished
      else context.become(copying(expected, true))
    }
    case CopyFinished => {
      val newSet = expected - context.sender()
      if (newSet.isEmpty && insertConfirmed) context.parent ! CopyFinished
      else context.become(copying(newSet, insertConfirmed))
      context.sender() ! PoisonPill
    }
    case PoisonPill => context.stop(self)
  }

}
