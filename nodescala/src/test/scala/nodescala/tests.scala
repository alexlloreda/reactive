package nodescala

import scala.language.postfixOps
import scala.util.{Try, Success, Failure}
import scala.collection._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.async.Async.{async, await}
import org.scalatest._
import NodeScala._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NodeScalaSuite extends FunSuite {

  test("A Future should always be completed") {
    val always = Future.always(517)

    assert(Await.result(always, 0 nanos) == 517)
  }
  test("A Future should never be completed") {
    val never = Future.never[Int]

    try {
      Await.result(never, 1 second)
      assert(false)
    } catch {
      case t: TimeoutException => // ok!
    }
  }

  test("The first future to complete is returned") {
    val f1 = Future.always(123)
    val f2 = Future.never
    val any = Future.any(List(f1,f2))
    assert(Await.result(any, 10 millis) == 123)
  }

  test("All futures complete and are returned") {
    val f1 = Future.always(123)
    val f2 = Future.always(234)
    val f3 = Future.always(345)
    val all = Future.all(List(f1,f2,f3))
    assert(Await.result(all, 10 millis) === List(123,234,345))

  }

  test("A Future should be completed after 300ms delay") {
    val f1 = Future.delay(300 millis)
    val start = System.currentTimeMillis()

    f1 onComplete { case _ =>
      val duration = System.currentTimeMillis() - start
      assert (duration >= 300L && duration < 400L)
    }
  }

  test("A Future with 100 ms delay should not complete after just 10 ms") {
    val f = Future.delay(100 millis)
    try {
      Await.result(f, 10 millis)
      fail("Should not have finished in 10 millis")
    } catch {
      case e: TimeoutException => // Success
    }
  }

  test("future now gets the value of completed future") {
    val f1 = Future.always(123)
    assert(f1.now === 123)
  }

  test("future.now throws exception for incomplete futures") {
    val f1 = Future.delay(2 seconds)
    try {
      f1.now
      fail("Should have thrown an exception")
    } catch {
      case e: NoSuchElementException => // That's what we wanted
    }
  }

  test("ContinueWith create a future from an exisitng future by passing a function that returns a result based on the first future") {
    val f1 = Future.delay(1 second)
    val f2 = f1.continueWith(_ => 13)
    assert(Await.result(f2, 2 seconds) === 13)
  }

  test("ContinueWith starts working on a future only after the initial future is complete") {
    val f1 = Future.delay(200 millis)
    val f2 = f1.continueWith(_ => 13)
    try {
      Await.result(f2, 100 millis)
    } catch {
      case e: TimeoutException => // As expected
    }
    Await.result(f1, 500 millis)
    // after f1 finishes f2 must be finished
    assert(f2.now === 13)
  }

  test("Chaining futures with ContinueWith should still work") {
    val f1 = Future.delay(100 millis).continueWith(_ => 13)
    val f2 = f1.continueWith(n => n.now * 2)
    assert(Await.result(f2, 200 millis) === 26)
  }

  test("Continue is weird") {
    val df = Future.delay(100 millis).continueWith(_ => 13)
    val cf = df.continue(t => t match {
      case Success(v) => v*2
      case Failure(e) => 42
    })
    assert(Await.result(cf, 150 millis) === 26)
  }

  class DummyExchange(val request: Request) extends Exchange {
    @volatile var response = ""
    val loaded = Promise[String]()
    def write(s: String) {
      response += s
    }
    def close() {
      loaded.success(response)
    }
  }

  class DummyListener(val port: Int, val relativePath: String) extends NodeScala.Listener {
    self =>

    @volatile private var started = false
    var handler: Exchange => Unit = null

    def createContext(h: Exchange => Unit) = this.synchronized {
      assert(started, "is server started?")
      handler = h
    }

    def removeContext() = this.synchronized {
      assert(started, "is server started?")
      handler = null
    }

    def start() = self.synchronized {
      started = true
      new Subscription {
        def unsubscribe() = self.synchronized {
          started = false
        }
      }
    }

    def emit(req: Request) = {
      val exchange = new DummyExchange(req)
      if (handler != null) handler(exchange)
      exchange
    }
  }

  class DummyServer(val port: Int) extends NodeScala {
    self =>
    val listeners = mutable.Map[String, DummyListener]()

    def createListener(relativePath: String) = {
      val l = new DummyListener(port, relativePath)
      listeners(relativePath) = l
      l
    }

    def emit(relativePath: String, req: Request) = this.synchronized {
      val l = listeners(relativePath)
      l.emit(req)
    }
  }
  test("Server should serve requests") {
    val dummy = new DummyServer(8191)
    val dummySubscription = dummy.start("/testDir") {
      request => for (kv <- request.iterator) yield (kv + "\n").toString
    }

    // wait until server is really installed
    Thread.sleep(500)

    def test(req: Request) {
      val webpage = dummy.emit("/testDir", req)
      val content = Await.result(webpage.loaded.future, 1 second)
      val expected = (for (kv <- req.iterator) yield (kv + "\n").toString).mkString
      assert(content == expected, s"'$content' vs. '$expected'")
    }

    test(immutable.Map("StrangeRequest" -> List("Does it work?")))
    test(immutable.Map("StrangeRequest" -> List("It works!")))
    test(immutable.Map("WorksForThree" -> List("Always works. Trust me.")))

    dummySubscription.unsubscribe()
  }

}




