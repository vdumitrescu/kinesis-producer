package com.moda.producer

import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ParametersSpec extends AnyWordSpec with Matchers {

  import Parameters._
  private val Given = Some("value")

  "isValid" should {

    "detect missing stream name" in {
      Parameters().isValid.left.get must be(Error_Stream_Needed)
    }

    "reject illegal combination of directory and any other source parameter" in {
      Parameters(streamNameOpt = Given, directoryOpt = Given, fileNameOpt = Given).isValid.left.get must be(
        Error_Directory_Only
      )
      Parameters(streamNameOpt = Given, directoryOpt = Given, bucketOpt = Given).isValid.left.get must be(
        Error_Directory_Only
      )
      Parameters(streamNameOpt = Given, directoryOpt = Given, prefixOpt = Given).isValid.left.get must be(
        Error_Directory_Only
      )
    }

    "reject illegal combination of filename and other source parameter" in {
      Parameters(streamNameOpt = Given, fileNameOpt = Given, bucketOpt = Given).isValid.left.get must be(
        Error_Filename_Only
      )
      Parameters(streamNameOpt = Given, fileNameOpt = Given, prefixOpt = Given).isValid.left.get must be(
        Error_Filename_Only
      )
    }

    "reject unless both bucket and prefix are specified" in {
      Parameters(streamNameOpt = Given, bucketOpt = Given).isValid.left.get must be(Error_Bucket_And_Prefix)
      Parameters(streamNameOpt = Given, prefixOpt = Given).isValid.left.get must be(Error_Bucket_And_Prefix)
    }

    "accept valid combinations of parameters" in {
      Parameters(streamNameOpt = Given, directoryOpt = Given).isValid.isRight must                 be(true)
      Parameters(streamNameOpt = Given, fileNameOpt = Given).isValid.isRight must                  be(true)
      Parameters(streamNameOpt = Given, bucketOpt = Given, prefixOpt = Given).isValid.isRight must be(true)
    }
  }
}
