package eu.krcz.examples

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import org.scalatest.FunSuite
import org.scalatest.prop.{Checkers, PropertyChecks}
import org.scalacheck.Gen
import org.scalacheck.Prop
import org.scalacheck.Prop._
import org.apache.spark.streaming.dstream.DStream
import com.holdenkarau.spark.testing.StreamingSuiteBase

class SparkEventTimeSpec extends StreamingSuiteBase with PropertyChecks with Checkers {

    implicit override val generatorDrivenConfig =
      PropertyCheckConfig(minSize = 10, maxSize = 1000, minSuccessful = 10)

    type Key = String

    def minTime = 1000000L
    def maxTime = minTime + 20000
    def maxLag = 3000L
    def timeGen = Gen.choose(minTime, maxTime)
    def lagGen = Gen.choose(0, maxLag)
    def windowSizeGen = Gen.choose(1, 5).map(_ * 1000L)
    def keyGen = Gen.oneOf("A", "B", "C", "D", "E")
    def valueGen = Gen.choose(0, 100)
    def eventGen = for {
        key <- keyGen
        eventTime <- timeGen
        lag <- lagGen
        processingTime = eventTime + lag
        value <- valueGen
    } yield processingTime -> (key, eventTime, value)

    def fullDataGen = Gen.listOf(eventGen)

    def windowStarts(windowSize: Long): Seq[Long] = {
        for(i <- (minTime / windowSize) to (maxTime / windowSize)) yield i * windowSize
    }

    def streamDataGen: Gen[Seq[Seq[(Key, Long, Int)]]] = for(data <- fullDataGen) yield {
        val s = ((maxTime - minTime + maxLag) / 1000).toInt
        val buf = ArrayBuffer.fill(s) { ArrayBuffer.empty[(Key, Long, Int)] }
        for((t, el) <- data) {
            buf(((t - minTime)/1000).toInt) += el
        }
        buf
    }

    def sumResultsBy[T](data: Seq[Seq[T]], filter: T => Boolean, getter: T => Int): Long = {
        data.toStream.flatMap(_.filter(filter).map(getter)).foldLeft(0L)(_ + _)
    }

    def inWindow(et: Long, windowStart: Long, windowSize: Long) = {
        et >= windowStart && et < (windowStart + windowSize)
    }

    def runOperation[U : ClassTag, V : ClassTag](operation: DStream[U] => DStream[V], input: Seq[Seq[U]], batches: Int = -1): Seq[Seq[V]] = {
        val trueBatches = if (batches == -1) input.size else batches
        val (stream, ssc) = setupStreams(input, operation, 4)
        val result = runStreams(stream, ssc, trueBatches, trueBatches)
        result
    }

    def propertyTest(name: String)(prop: => Prop) = test(name) {
        check(prop)
    }

    propertyTest("sumByWindow partial results should sum properly") {
        forAllNoShrink(streamDataGen, windowSizeGen) { (data, windowSize) =>
            val noKeyData: Seq[Seq[(Long, Int)]] = data.map(_.map {
                case (key, time, value) => (time, value)
            })
            val operation = (stream: DStream[(Long, Int)]) => SparkEventTime.sumByWindow(stream, windowSize)
            val result = runOperation(operation, noKeyData)

            def checkWindow(windowStart: Long): Prop = {
                val expectedSum = sumResultsBy[(Long, Int)](noKeyData, et => inWindow(et._1, windowStart, windowSize), _._2)
                val actualSum = sumResultsBy[(Long, Int)](result, _._1 == windowStart, _._2)
                (actualSum ?= expectedSum) :| ("wrong sum for window starting at " + windowStart)
            }

            all(windowStarts(windowSize).map(checkWindow): _*)
        }
    }

    def keyedCheck(winSize: Long, winStarts: Seq[Long], input: Seq[Seq[(Key, Long, Int)]], output: Seq[Seq[((Long, Key), Int)]]): Prop = {
        def checkWindow(windowStart: Long): Prop = {
            val expectedSum = sumResultsBy[(Key, Long, Int)](input, et => inWindow(et._2, windowStart, winSize), _._3)
            val actualSum = sumResultsBy[((Long, Key), Int)](output, _._1._1 == windowStart, _._2)
            (actualSum ?= expectedSum) :| ("wrong sum for window starting at " + windowStart)
        }

        all(winStarts.map(checkWindow): _*)
    }

    propertyTest("sumByWindowAndKey partial results should sum properly") {
        forAllNoShrink(streamDataGen, windowSizeGen) { (data, windowSize) =>
            val operation = (stream: DStream[(Key, Long, Int)]) => SparkEventTime.sumByWindowAndKey(stream, windowSize)
            val result = runOperation(operation, data)

            keyedCheck(windowSize, windowStarts(windowSize), data, result)
        }
    }

    propertyTest("sumBySlidingWindowAndKey partial results should sum properly") {
        forAllNoShrink(streamDataGen, windowSizeGen, windowSizeGen) { (data, windowSlide, windowSize) =>
            val operation = (stream: DStream[(Key, Long, Int)]) => SparkEventTime.sumBySlidingWindowAndKey(stream, windowSlide, windowSize)
            val result = runOperation(operation, data)

            keyedCheck(windowSize, windowStarts(windowSlide), data, result)
        }
    }
}
