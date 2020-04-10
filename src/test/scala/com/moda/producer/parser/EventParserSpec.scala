package com.moda.producer.parser

import cats.effect.IO
import com.moda.producer.Event
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

class EventParserSpec extends AnyWordSpec with Matchers with EventParser {

  "parseLine" should {
    "return an event from a valid line" in {
      parseLine("key|value") must be(Some(Event("key", "value")))
    }

    "be able to parse a line with multiple separators" in {
      parseLine("key|value|blank") must be(Some(Event("key", "value|blank")))
    }

    "return None if the line is not valid" in {
      parseLine("key,value") must be(None)
    }
  }

  implicit val logger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLoggerFromName("test")

  "parseEvent" should {
    "return all parsed events" in {
      parseEvents(
        List(
          "one|mississippi",
          "two|mississippi",
          "ten|mississippi"
        )
      ) must contain theSameElementsAs List(
        Event("one", "mississippi"),
        Event("two", "mississippi"),
        Event("ten", "mississippi")
      )
    }

    "return good events and log bad events" in {
      parseEvents(
        List(
          "one|mississippi",
          "two,mississippi",
          "ten|mississippi"
        )
      ) must contain theSameElementsAs List(
        Event("one", "mississippi"),
        Event("ten", "mississippi")
      )
    }
  }

  private def parseEvents: List[String] => List[Event] =
    lines =>
      fs2.Stream
        .emits(lines)
        .lift[IO]
        .through(EventParser.parseEvent)
        .compile
        .toList
        .unsafeRunSync()
}
