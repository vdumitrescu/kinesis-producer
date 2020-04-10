package com.moda.producer.publisher

import cats.effect.Sync
import com.moda.producer.Event
import fs2.Pipe
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger

trait Publisher[F[_]] {
  def publishEvent(implicit F: Sync[F], logger: SelfAwareStructuredLogger[F]): Pipe[F, Event, Unit]
}
