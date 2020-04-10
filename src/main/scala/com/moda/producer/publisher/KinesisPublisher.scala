package com.moda.producer.publisher
import java.nio.ByteBuffer
import java.util.UUID

import cats.effect.Concurrent
import cats.implicits._
import com.amazonaws.auth.{
  AWSCredentialsProviderChain,
  DefaultAWSCredentialsProviderChain,
  STSAssumeRoleSessionCredentialsProvider
}
import com.amazonaws.regions.Regions
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder
import com.moda.producer.Event
import fs2.Pipe
import fs2.aws.internal.KinesisProducerClientImpl
import fs2.aws.kinesis.publisher
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger

import scala.concurrent.ExecutionContext

class KinesisPublisher[F[_]](streamName: String, roleArnOpt: Option[String], regionOpt: Option[String]) {

  private[this] val stsClient   = AWSSecurityTokenServiceClientBuilder.standard().withRegion(Regions.US_EAST_1).build()
  private[this] val sessionName = "kinesis-producer-" + UUID.randomUUID().toString

  private[this] val credentialsProvider: AWSCredentialsProviderChain = roleArnOpt.fold(
    DefaultAWSCredentialsProviderChain.getInstance().asInstanceOf[AWSCredentialsProviderChain]
  ) { roleArn =>
    new AWSCredentialsProviderChain(
      new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, sessionName)
        .withStsClient(stsClient)
        .build()
    )
  }

  private[this] val kinesisProducer = new KinesisProducerClientImpl[F] {
    override val credentials: AWSCredentialsProviderChain = credentialsProvider
    override val region: Option[String]                   = regionOpt.orElse(Some("us-east-1"))
  }

  def publishEvent(
    implicit F: Concurrent[F],
    ec: ExecutionContext,
    logger: SelfAwareStructuredLogger[F]
  ): Pipe[F, Event, Unit] =
    _.evalMap { event =>
      logger.info(s"Publishing event $event") *>
        F.delay(event.key -> ByteBuffer.wrap(event.data.getBytes))
    }.through(publisher.writeToKinesis(streamName, producer = kinesisProducer))
      .evalMap { result =>
        logger.info(s"Result: ${if (result.isSuccessful) "success" else "failed"}") *> F.unit
      }
}
