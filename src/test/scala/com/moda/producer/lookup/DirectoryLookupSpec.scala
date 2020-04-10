package com.moda.producer.lookup

import cats.effect.{ Blocker, ContextShift, IO }
import fs2.Stream
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.global

class DirectoryLookupSpec extends AnyWordSpec with Matchers {

  implicit val ioContextShift: ContextShift[IO] = IO.contextShift(global)

  "DirectoryLookup" should {
    "fetch records from all files in a directory" in {
      Stream
        .resource(Blocker[IO])
        .flatMap { blocker =>
          new DirectoryLookup("data").stream[IO](blocker)
        }
        .compile
        .toList
        .unsafeRunSync() must contain theSameElementsAs Seq(
        """01|{"id": 1, "data": "Alabama"}""",
        """02|{"id": 2, "data": "Alaska"}""",
        """03|{"id": 3, "data": "Arizona"}""",
        """04|{"id": 4, "data": "Arkansas"}""",
        """05|{"id": 5, "data": "California"}""",
        """06|{"id": 6, "data": "Colorado"}""",
        """07|{"id": 7, "data": "Connecticut"}""",
        """47|{"id": 47, "data": "Washington"}""",
        """48|{"id": 48, "data": "West Virginia"}""",
        """49|{"id": 49, "data": "Wisconsin"}""",
        """50|{"id": 50, "data": "Wyoming"}"""
      )
    }
  }
}
