package suggestions



import language.postfixOps
import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Try, Success, Failure}
import rx.lang.scala._
import org.scalatest._
import gui._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class WikipediaApiTest extends FunSuite {

  object mockApi extends WikipediaApi {
    def wikipediaSuggestion(term: String) = Future {
      if (term.head.isLetter) {
        for (suffix <- List(" (Computer Scientist)", " (Footballer)")) yield term + suffix
      } else {
        List(term)
      }
    }
    def wikipediaPage(term: String) = Future {
      "Title: " + term
    }
  }

  import mockApi._

  test("WikipediaApi should make the stream valid using sanitized") {
    val notvalid = Observable.just("erik", "erik meijer", "martin")
    val valid = notvalid.sanitized

    var count = 0
    var completed = false

    val sub = valid.subscribe(
      term => {
        assert(term.forall(_ != ' '))
        count += 1
      },
      t => assert(false, s"stream error $t"),
      () => completed = true
    )
    assert(completed && count == 3, "completed: " + completed + ", event count: " + count)
  }

  /** Given an observable that can possibly be completed with an error, returns a new observable
    * with the same values wrapped into `Success` and the potential error wrapped into `Failure`.
    *
    * E.g. `1, 2, 3, !Exception!` should become `Success(1), Success(2), Success(3), Failure(Exception), !TerminateStream!`
    */
  test("WikipediaApi should properly recover") {
    val thrw = new Throwable
    val failedObservable = Observable.just(1, 2, 3) ++ Observable.error(thrw)

    val recoveredObserable = failedObservable.recovered
    val observed = mutable.Buffer[Any]()

    val sub = recoveredObserable subscribe {
      observed += _
    }

    assert(observed == Seq(Success(1), Success(2), Success(3), Failure(thrw)), observed)
  }

  test("Observable should complete before timeout") {
    val start = System.currentTimeMillis
    val timedOutStream = Observable.from(1 to 3).zip(Observable.interval(100 millis)).timedOut(3L)
    val contents = timedOutStream.toBlocking.toList
    val totalTime = System.currentTimeMillis - start
    assert(contents == List((1,0),(2,1),(3,2)))
    assert(totalTime <= 1000)
  }

  test("return the first two values, and complete without errors") {
    val timedOutStream = Observable.from(1 to 3).zip(Observable.interval(400 millis)).timedOut(1L)
    val contents = timedOutStream.toBlocking.toList
    assert(contents == List((1,0),(2,1)))
  }

  test("return the first value, and complete without errors") {
    val timedOutStream = Observable.from(1 to 3).zip(Observable.interval(700 millis)).timedOut(1L)
    val contents = timedOutStream.toBlocking.toList
    assert(contents == List((1,0)))
  }

  test("WikipediaApi should correctly use concatRecovered") {
    val requests = Observable.just(1, 2, 3)
    val remoteComputation = (n: Int) => Observable.just(0 to n : _*)
    val responses = requests concatRecovered remoteComputation
    val sum = responses.foldLeft(0) { (acc, tn) =>
      tn match {
        case Success(n) => acc + n
        case Failure(t) => throw t
      }
    }
    var total = -1
    val sub = sum.subscribe {
      s => total = s
    }
    assert(total == (1 + 1 + 2 + 1 + 2 + 3), s"Sum: $total")
  }

  test("WikipediaApi should correctly use concatRecovered, concatenating many responses, even with exceptions") {
    val requests = Observable.just(1, 2, 3)
    val remoteComputation = (num: Int) =>
      if (num != 2) Observable.just(num, num, num)
      else Observable.just(num) ++ Observable.error(new Exception) ++ Observable.just(num)
    val responses = requests concatRecovered remoteComputation
    val sum = responses.foldLeft(0) { (acc, tn) =>
      tn match {
        case Success(n) => acc + n
        case Failure(t) => acc // Failures are 0
      }
    }
    var total = -1
    val sub = sum.subscribe {
      s => total = s
    }
    assert(total == (1 + 1 + 1 + 2 + 3 + 3 + 3), s"Sum: $total")
  }

  test("concatRecovered given test case 1") {
    val requestStream = Observable.from(1 to 5)
    def requestMethod(num: Int) = if (num != 4) Observable.just(num) else Observable.error(new Exception)
    val actual = requestStream.concatRecovered(requestMethod).toBlocking.toList
    assert(actual.toString == "List(Success(1), Success(2), Success(3), Failure(java.lang.Exception), Success(5))")
  }

}
