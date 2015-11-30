package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop.{forAll, BooleanOperators}

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  lazy val genHeap: Gen[H] = Gen.frequency((4,
    for {
      i <- arbitrary[Int]
      h <- Gen.frequency((1,const(empty)), (3,genHeap))
    } yield insert(i, h)),
    (1, const(empty)))

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

  property("gen1") = forAll { (h: H) =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    findMin(insert(m, h)) == m
  }

  property("min2") = forAll {(a: Int, b:Int) =>
    val heap = insert(b, insert(a, empty))
    findMin(heap) == math.min(a,b)
  }

  property("deleting a single element heap should give empty") = forAll { a: Int =>
    isEmpty(deleteMin(insert(a, empty)))
  }

  property("meld heap gets the minimum") = forAll { (h1: H, h2: H) =>
    (!isEmpty(h1) && !isEmpty(h2)) ==> {
      val min1 = findMin(h1)
      val min2 = findMin(h2)
      findMin(meld(h1, h2)) == math.min(min1, min2)
    }
  }

  def isSorted(h: H, a: Int): Boolean = {
    if (isEmpty(h)) true
    else {
      val m = findMin(h)
      m >= a && isSorted(deleteMin(h), m)
    }
  }

  property("heaps are sorted") = forAll { h: H =>
    (!isEmpty(h)) ==> {
      isSorted(h, findMin(h))
    }
  }

  property("merged heaps are sorted") = forAll { (h1: H, h2: H) =>
    (!isEmpty(h1) && !isEmpty(h2)) ==> {
      isSorted(meld(h1, h2), math.min(findMin(h1), findMin(h2)))
    }
  }

  def size(h: H): Int = {
    def innerSize(h: H, a: Int): Int = {
      if (isEmpty(h)) a
      else innerSize(deleteMin(h), a + 1)
    }
    innerSize(h,0)
  }

  property("merge heaps preserve size") = forAll {(h1: H, h2: H) =>
    size(h1) + size(h2) == size(meld(h1,h2))
  }

  def buildFromList(xs: List[Int]): H = xs match {
    case Nil   => empty
    case i::is => insert(i, buildFromList(is))
  }
  def isSameOrder(h: H, xs: List[Int]): Boolean = xs match {
    case Nil   => isEmpty(h)
    case i::is => findMin(h) == i && isSameOrder(deleteMin(h), is)
  }
  property("inserting lists of ints") = forAll { xs: List[Int] =>
    val h = buildFromList(xs)
    xs.size == size(h)
    (!isEmpty(h)) ==> isSorted(h, xs.min)
  }

  property("heaps have the same order as a sorted list") = forAll { xs: List[Int] =>
    val h = buildFromList(xs)
    isSameOrder(h, xs.sorted)
  }
}
