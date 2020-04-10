package com.moda.producer.lookup

import cats.effect.{ Blocker, ContextShift, Effect }
import com.moda.producer.Config
import fs2.Stream

trait Lookup {
  def stream[F[_]: Effect: ContextShift](blocker: Blocker): Stream[F, String]
}

object Lookup {
  def apply(config: Config): Lookup =
    (config.fileNameOpt, config.directoryOpt, config.bucketOpt, config.prefixOpt) match {
      case (Some(fileName), None, None, None)       => new FileLookup(fileName)
      case (None, Some(dir), None, None)            => new DirectoryLookup(dir)
      case (None, None, Some(bucket), Some(prefix)) => new S3Lookup(bucket, prefix)
      case _                                        => throw new RuntimeException("Invalid configuration.")
    }
}
