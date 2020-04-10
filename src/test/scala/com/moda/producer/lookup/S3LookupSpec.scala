package com.moda.producer.lookup

import java.io.File

import cats.effect.{ Blocker, ContextShift, IO }
import fs2.Stream
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.global

class S3LookupSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll with LocalStackS3Spec {

  val BucketName = "test-bucket"

  override def beforeAll(): Unit = {
    logger.info(s"About to create bucket $BucketName...")
    localStackS3.createBucket(BucketName)
    logger.info(s"About to copy some objects to the bucket...")
    localStackS3.putObject(BucketName, "data/a.txt", new File("data/a.txt"))
    localStackS3.putObject(BucketName, "data/c.txt", new File("data/c.txt"))
    localStackS3.putObject(BucketName, "data/w.txt", new File("data/w.txt"))
    logger.info("Done.")
  }

  override def afterAll(): Unit = {
    logger.info(s"About to delete bucket $BucketName and its contents...")
    localStackS3.deleteObject(BucketName, "data/a.txt")
    localStackS3.deleteObject(BucketName, "data/c.txt")
    localStackS3.deleteObject(BucketName, "data/w.txt")
    localStackS3.deleteBucket(BucketName)
    logger.info("Done.")
  }

  lazy val s3Lookup: S3Lookup = new S3Lookup(BucketName, "data") {
    override private[lookup] def client = localStackS3
  }

  implicit val ioContextShift: ContextShift[IO] = IO.contextShift(global)

  "S3Lookup" should {
    "fetch records from files in an S3 bucket matching a prefix" in {
      Stream
        .resource(Blocker[IO])
        .flatMap { blocker =>
          s3Lookup.stream[IO](blocker)
        }
        .compile
        .toList
        .unsafeRunSync() must contain theSameElementsAs Seq(
        """01|{"id": 1, "data": "Alabama"}""",
        """02|{"id": 2, "data": "Alaska"}""",
        """03|{"id": 3, "data": "Arizona"}""",
        """04|{"id": 4, "data": "Arkansas"}""",
        """05|{"id": 5, "data": "California"}""",
        """06|{"id": 6, "data": "Colorado"}""",
        """07|{"id": 7, "data": "Connecticut"}""",
        """47|{"id": 47, "data": "Washington"}""",
        """48|{"id": 48, "data": "West Virginia"}""",
        """49|{"id": 49, "data": "Wisconsin"}""",
        """50|{"id": 50, "data": "Wyoming"}"""
      )
    }
  }
}
