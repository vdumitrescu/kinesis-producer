package com.moda.producer.parser

import cats.Applicative
import cats.implicits._
import com.moda.producer.Event
import fs2.Pipe
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger

trait EventParser {
  def parseEvent[F[_]: Applicative](implicit logger: SelfAwareStructuredLogger[F]): Pipe[F, String, Event] = {

    def parse(line: String): F[Option[Event]] = parseLine(line) match {
      case Some(event) => Applicative[F].pure(Some(event))
      case None        => logger.warn(s"Failed to parse line $line, key not found!") *> Applicative[F].pure(None)
    }

    _.through(_.evalMap(parse)).unNone
  }

  private[parser] def parseLine: String => Option[Event] = line => {
    val commaPos = line.indexOf('|')
    if (commaPos > 0) {
      val key   = line.take(commaPos)
      val value = line.drop(commaPos + 1)
      Some(Event(key, value))
    } else None
  }
}

object EventParser extends EventParser
