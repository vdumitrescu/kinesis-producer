package com.moda.producer

import cats.effect._
import cats.implicits._
import com.moda.producer.lookup.Lookup
import com.moda.producer.parser.EventParser
import com.moda.producer.publisher.{ ConsolePublisher, KinesisPublisher }
import fs2.Stream
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import scala.concurrent.ExecutionContext.Implicits.global

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =
    Parameters.withConfig(args)(exec)(IO.pure(ExitCode.Error))

  private[this] def exec(config: Config): IO[ExitCode] = {
    implicit val logger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLoggerFromName("application")

    // val publisher = new ConsolePublisher[IO]
    val publisher = new KinesisPublisher[IO](config.streamName, config.roleArnOpt, config.regionOpt)

    Stream
      .resource(Blocker[IO])
      .flatMap { blocker =>
        Lookup(config)
          .stream[IO](blocker)
          .through(EventParser.parseEvent)
          .through(publisher.publishEvent)
      }
      .compile
      .drain
      .as(ExitCode.Success)
  }
}
