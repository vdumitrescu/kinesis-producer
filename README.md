# kinesis-producer
A generic Kinesis producer.

### Usage

```
$ ./kinesis-producer --help
kinesis-producer 1.0.0
Usage: kinesis-producer [options]

  -s, --stream <stream>   (Required) Specify the name of the Kinesis stream to publish to.
  -r, --role <role arn>   (Optional) The STS role to assume before connecting to AWS Kinesis
  -g, --region <region>   (Optional) The region where the Kinesis stream is defined, defaults to us-east-1
  -d, --dir <directory>   (Optional) The directory containing the files to be published on the stream
  -f, --file <file name>  (Optional) The file name containing the data to be published on the stream, one event per line
  -b, --bucket <bucket>   (Optional) The name of the S3 bucket containing the file(s) to be published
  -p, --prefix <prefix>   (Optional) Prefix for object name(s) to find in the S3 bucket
You can publish the contents of a single file using the --file parameter, or
you can publish all files from a directory, given with the --dir parameter, or
you can publish files from an S3 bucket, given with --bucket and --prefix parameters.
  --help                  Prints this usage text.
```

### Credentials

The tool accesses AWS by using credentials set up in the same manner as the AWS CLI. For more information, see [here](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).

If the data is published from an S3 bucket, the region will be picked up from the profile, or the `AWS_REGION` environment variable.

If the Kinesis stream resides in a different account, and possibly different region, the `--role-arn` and `--region` parameters can be used.

### Build
`sbt assembly`

### Data

The application expects data files to have one record per line, in this format:

`key|value`

The key cannot contain the pipe (`|`) character, but the value can.
