package MapData

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import com.amazonaws.services.s3.event.S3EventNotification
import awscala._
import dynamodbv2.{DynamoDB, Table}
import s3.{Bucket, S3, S3Object}
import play.api.libs.json._

import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

class Handler extends RequestHandler[S3EventNotification, Either[Throwable, String]] {

  def handleRequest(input: S3EventNotification, context: Context): Either[Throwable, String] = {
    val region: Region = Region.US_EAST_1
    implicit val s3: S3 = S3.at(region)

    val bucketName: String = "database-0"

    val records = input.getRecords.asScala.toList

    if (records.isEmpty) {
      Right("Function finished executing on 0 records")
    } else {
      //If either S3 fails connecting, or bucket of the correct name is not found. Return the exception.
      val bucket: Either[Throwable, Bucket] = retrieveS3Bucket(bucketName, s3)

      bucket.flatMap(b => {
        val slackMessageList = records.map(_.getS3.getObject.getKey).map(parseS3Object(b, s3)).flatMap(_.getOrElse(List.empty))
        writeToDynamo(tableName = "database-one", region = region)(slackMessageList)
      })
    }
  }

  def retrieveS3Bucket(bucketName: String, s3: S3): Either[Throwable, Bucket] = {
    val attemptedBucket: Either[Throwable, Option[Bucket]] = Try(s3.bucket(bucketName)).toEither

    attemptedBucket.flatMap(_.toRight(new Exception("Bucket " + bucketName + " not found")))
  }

  def parseS3Object(b: Bucket, s3: S3)(x: String): Either[Throwable, List[SlackMessage]] = {
    val obj: Try[Option[S3Object]] = Try(s3.get(b, x))

    obj.toEither.flatMap(ob => {
      ob.toRight(new Exception("Object not found in s3")).map(o => {
        Helper.parseMessages(parseJSON(o))
      })
    })
  }

  def parseJSON(obj: S3Object): JsValue = {
    val stream = obj.content
    val jsonInput: JsValue = try {
      Json.parse(stream)
    } finally {
      stream.close()
    }
    jsonInput
  }

  def writeToDynamo(tableName: String, region: Region)(slackMessages: List[SlackMessage]): Either[Throwable, String] = {
    implicit val dynamoDB: DynamoDB = DynamoDB.at(region)

    Try(dynamoDB.table(tableName)).toEither.flatMap(tab => {
      tab.toRight(new Exception("Table " + tableName + " not found")).map(t => {
        slackMessages.foreach(putToDynamo(t))
        return Right("Messages put successfully.")
      })
    })
  }

  def putToDynamo(table: Table)(message: SlackMessage)(implicit dynamoDB: DynamoDB): Unit = {
    table.put(message.user, message.ts, "Text" -> message.text)
  }
}
