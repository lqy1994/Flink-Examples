package com.meituan.meishi.data.lqy.flink.sink

import java.util.Collections

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.types.Row
import org.junit.Assert._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object StreamITCase {
  var testResults: mutable.MutableList[String] = mutable.MutableList.empty[String]
  var retractedResults: ArrayBuffer[String] = mutable.ArrayBuffer.empty[String]

  def clear(): Unit = {
    StreamITCase.testResults.clear()
    StreamITCase.retractedResults.clear()
  }

  def compareWithList(expected: java.util.List[String]): Unit = {
    Collections.sort(expected)
    assertEquals(expected, StreamITCase.testResults.sorted)
  }

}

final class StringSink[T] extends RichSinkFunction[T]() {
  override def invoke(value: T): Unit = {
    StreamITCase.testResults.synchronized {
      StreamITCase.testResults += value.toString
    }
  }
}

final class RetractMessageSink extends RichSinkFunction[(Boolean, Row)]() {
  override def invoke(v: (Boolean, Row)): Unit = {
    StreamITCase.testResults.synchronized {
      StreamITCase.testResults += (if (v._1) "+" else "-") + v._2
    }
  }
}

final class RetractingSink() extends RichSinkFunction[(Boolean, Row)] {

  override def invoke(v: (Boolean, Row)) {
    StreamITCase.retractedResults.synchronized {
      val value = v._2.toString
      if (v._1) {
        StreamITCase.retractedResults += value
      } else {
        val idx = StreamITCase.retractedResults.indexOf(value)
        if (idx >= 0) {
          StreamITCase.retractedResults.remove(idx)
        } else {
          throw new RuntimeException("Tried to retract a value that wasn't added first. " +
            "This is probably an incorrectly implemented test. " +
            "Try to set the parallelism of the sink to 1.")
        }
      }
    }
  }
}