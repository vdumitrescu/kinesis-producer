package com.moda.producer.publisher

import cats.effect.Sync
import fs2.Pipe
import cats.implicits._
import com.moda.producer.Event
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger

class ConsolePublisher[F[_]] extends Publisher[F] {
  override def publishEvent(implicit F: Sync[F], logger: SelfAwareStructuredLogger[F]): Pipe[F, Event, Unit] =
    _.evalMap { event =>
      logger.info(s"Key: ${event.key}, Data: ${event.data}") *> F.unit
    }
}
