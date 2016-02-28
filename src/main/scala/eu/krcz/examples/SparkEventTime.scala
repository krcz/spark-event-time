package eu.krcz.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._

object SparkEventTime {

    def sumByWindow(stream: DStream[(Long, Int)], windowSize: Long): DStream[(Long, Int)] = {  
        stream.map(el => (el._1 - el._1 % windowSize, el._2)).reduceByKey(_ + _)
    }

    def sumByWindowAndKey[K](
        stream: DStream[(K, Long, Int)],
        windowSize: Long
    ): DStream[((Long, K), Int)] = {  
        def getWindow(ts: Long) = ts - ts % windowSize
        stream.map(el => ((getWindow(el._2), el._1), el._3)).
               reduceByKey(_ + _)
    }

    def sumBySlidingWindowAndKey[K](
        stream: DStream[(K, Long, Int)],
        windowSlide: Long,
        windowSize: Long
    ): DStream[((Long, K), Int)] = {  
        def getWindows(ts: Long) = {
            val last = ts - ts % windowSlide
            val maxSlideIt = if(last + windowSize <= ts) -1 else (last - (ts - windowSize + 1)) / windowSlide
            for(i <- 0 to maxSlideIt.toInt) yield (last - i * windowSlide)
        }

        stream.flatMap(el => getWindows(el._2).map(w => ((w, el._1), el._3))).
               reduceByKey(_ + _)
    }


}
