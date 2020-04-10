package com.moda.producer.lookup
import java.nio.file.{ Files, Paths }

import cats.effect.{ Blocker, ContextShift, Effect }
import fs2.Stream

import scala.collection.JavaConverters._

class DirectoryLookup(directory: String) extends Lookup {
  override def stream[F[_]: Effect: ContextShift](blocker: Blocker): Stream[F, String] =
    Files
      .newDirectoryStream(Paths.get(directory))
      .asScala
      .filter { path =>
        !Files.isDirectory(path)
      }
      .map { path =>
        new FileLookup(path.toString).stream(blocker)
      }
      .foldLeft(Stream[F, String]())(_ ++ _)
}
