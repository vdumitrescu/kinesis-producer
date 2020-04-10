package com.moda.producer.lookup

import cats.effect.{ Blocker, ContextShift, IO }
import fs2.Stream
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.global

class FileLookupSpec extends AnyWordSpec with Matchers {

  implicit val ioContextShift: ContextShift[IO] = IO.contextShift(global)

  "FileLookup" should {
    "fetch all records from a file" in {
      Stream
        .resource(Blocker[IO])
        .flatMap { blocker =>
          new FileLookup("data/a.txt").stream[IO](blocker)
        }
        .compile
        .toList
        .unsafeRunSync() must contain theSameElementsAs Seq(
        """01|{"id": 1, "data": "Alabama"}""",
        """02|{"id": 2, "data": "Alaska"}""",
        """03|{"id": 3, "data": "Arizona"}""",
        """04|{"id": 4, "data": "Arkansas"}"""
      )
    }
  }
}
