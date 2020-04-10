package com.moda.producer.lookup

import cats.effect.{ Blocker, ContextShift, Effect }
import com.amazonaws.services.s3.{ AmazonS3, AmazonS3ClientBuilder }
import fs2.Stream
import fs2.aws.s3

import scala.collection.JavaConverters._

class S3Lookup(bucket: String, prefix: String) extends Lookup {

  override def stream[F[_]: Effect: ContextShift](blocker: Blocker): Stream[F, String] =
    client
      .listObjectsV2(bucket, prefix)
      .getObjectSummaries
      .asScala
      .map { objectData =>
        s3.readS3File[F](bucket, objectData.getKey, blocker.blockingContext, client)
          .through(fs2.text.utf8Decode)
          .through(fs2.text.lines)
      }
      .foldLeft(Stream[F, String]())(_ ++ _)

  private[lookup] def client: AmazonS3 = AmazonS3ClientBuilder.defaultClient()
}
