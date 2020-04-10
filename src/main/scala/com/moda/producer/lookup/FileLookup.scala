package com.moda.producer.lookup

import java.nio.file.Paths

import cats.Applicative
import cats.effect.{ Blocker, ContextShift, Effect }
import fs2.{ Pipe, Stream }

class FileLookup(fileName: String) extends Lookup {
  override def stream[F[_]: Effect: ContextShift](blocker: Blocker): Stream[F, String] =
    fs2.io.file
      .readAll(Paths.get(fileName), blocker, 16384)
      .through(fs2.text.utf8Decode)
      .through(fs2.text.lines)
      .through(filterNonEmpty)

  private[this] def filterNonEmpty[F[_]: Applicative]: Pipe[F, String, String] = {
    def filter(string: String): F[Option[String]] = string.trim match {
      case line if line.nonEmpty => Applicative[F].pure(Some(line))
      case _                     => Applicative[F].pure(None)
    }
    _.through(_.evalMap(filter)).unNone
  }
}
