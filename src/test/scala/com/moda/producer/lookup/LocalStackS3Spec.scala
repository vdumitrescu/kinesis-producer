package com.moda.producer.lookup

import com.amazonaws.services.s3.{ AmazonS3, AmazonS3ClientBuilder }
import com.dimafeng.testcontainers.{ ForAllTestContainer, LocalStackContainer }
import org.scalatest.Suite
import org.slf4j.Logger
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.localstack.LocalStackContainer.Service.S3

trait LocalStackS3Spec extends ForAllTestContainer {
  self: Suite =>

  val logger: Logger = org.slf4j.LoggerFactory.getLogger(getClass.getSimpleName)

  override val container: LocalStackContainer = {
    DockerClientFactory.instance().client()
    logger.warn("Starting an S3 localstack container, this might take a few seconds..")
    LocalStackContainer(services = Seq(S3))
  }

  implicit lazy val localStackS3: AmazonS3 = AmazonS3ClientBuilder
    .standard()
    .withEndpointConfiguration(container.endpointConfiguration(S3))
    .withPathStyleAccessEnabled(true)
    .withCredentials(container.defaultCredentialsProvider)
    .build()
}
