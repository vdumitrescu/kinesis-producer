package com.moda.producer

import scopt.OptionParser

final case class Config(
  streamName: String,
  roleArnOpt: Option[String],
  regionOpt: Option[String],
  directoryOpt: Option[String],
  fileNameOpt: Option[String],
  bucketOpt: Option[String],
  prefixOpt: Option[String]
)

final case class Parameters(
  streamNameOpt: Option[String] = None,
  roleArnOpt: Option[String] = None,
  regionOpt: Option[String] = None,
  directoryOpt: Option[String] = None,
  fileNameOpt: Option[String] = None,
  bucketOpt: Option[String] = None,
  prefixOpt: Option[String] = None
) {

  def isValid: Either[String, Unit] = {
    import Parameters._
    if (streamNameOpt.isEmpty) Left(Error_Stream_Needed)
    else if (directoryOpt.nonEmpty) {
      if (fileNameOpt.nonEmpty || bucketOpt.nonEmpty || prefixOpt.nonEmpty)
        Left(Error_Directory_Only)
      else Right(())
    } else if (fileNameOpt.nonEmpty) {
      if (bucketOpt.nonEmpty || prefixOpt.nonEmpty)
        Left(Error_Filename_Only)
      else Right(())
    } else {
      if (bucketOpt.isEmpty || prefixOpt.isEmpty)
        Left(Error_Bucket_And_Prefix)
      else Right(())
    }
  }

  def toConfig: Config =
    Config(streamNameOpt.get, roleArnOpt, regionOpt, directoryOpt, fileNameOpt, bucketOpt, prefixOpt)
}

object Parameters {
  private[producer] val Error_Stream_Needed     = "Stream must be specified."
  private[producer] val Error_Directory_Only    = "Must not specify file, bucket or prefix, when directory is provided."
  private[producer] val Error_Filename_Only     = "Must not specify bucket or prefix, when a file name is provided."
  private[producer] val Error_Bucket_And_Prefix = "Must specify both bucket and prefix to publish files from S3."

  private[this] def parser(appName: String, versionStr: String): OptionParser[Parameters] =
    new scopt.OptionParser[Parameters](appName) {
      head(appName, versionStr)

      opt[String]('s', "stream")
        .valueName("<stream>")
        .text("(Required) Specify the name of the Kinesis stream to publish to.")
        .action { (value, params) =>
          params.copy(streamNameOpt = Some(value))
        }

      opt[String]('r', "role")
        .valueName("<role arn>")
        .text("(Optional) The STS role to assume before connecting to AWS Kinesis")
        .action { (value, params) =>
          params.copy(roleArnOpt = Some(value))
        }

      opt[String]('g', "region")
        .valueName("<region>")
        .text("(Optional) The region where the Kinesis stream is defined, defaults to us-east-1")
        .action { (value, params) =>
          params.copy(roleArnOpt = Some(value))
        }

      opt[String]('d', "dir")
        .valueName("<directory>")
        .text("(Optional) The directory containing the files to be published on the stream")
        .action { (value, params) =>
          params.copy(directoryOpt = Some(value))
        }

      opt[String]('f', "file")
        .valueName("<file name>")
        .text("(Optional) The file name containing the data to be published on the stream, one event per line")
        .action { (value, params) =>
          params.copy(fileNameOpt = Some(value))
        }

      opt[String]('b', "bucket")
        .valueName("<bucket>")
        .text("(Optional) The name of the S3 bucket containing the file(s) to be published")
        .action { (value, params) =>
          params.copy(bucketOpt = Some(value))
        }

      opt[String]('p', "prefix")
        .valueName("<prefix>")
        .text("(Optional) Prefix for object name(s) to find in the S3 bucket")
        .action { (value, params) =>
          params.copy(prefixOpt = Some(value))
        }

      checkConfig { params =>
        params.isValid match {
          case Left(message) => failure(message)
          case Right(_)      => success
        }
      }

      note(
        "You can publish the contents of a single file using the --file parameter, or\n" +
          "you can publish all files from a directory, given with the --dir parameter, or\n" +
          "you can publish files from an S3 bucket, given with --bucket and --prefix parameters."
      )

      help("help").text("Prints this usage text.")
    }

  def withConfig[T](args: List[String])(body: Config => T)(err: =>T): T =
    parser(BuildInfo.name, BuildInfo.version).parse(args, Parameters()) match {
      case Some(parameters) => body(parameters.toConfig)
      case None             => err
    }
}
