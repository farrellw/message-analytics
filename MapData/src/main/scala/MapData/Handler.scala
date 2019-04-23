package MapData

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import com.amazonaws.services.s3.event.S3EventNotification
import awscala._, dynamodbv2.{DynamoDB, Table}
import s3.{Bucket, S3, S3Object}
import play.api.libs.json._
import scala.util.Try
import scala.collection.JavaConverters._

class Handler extends RequestHandler[S3EventNotification, Either[Throwable, String]] {

  def handleRequest(input: S3EventNotification, context: Context): Either[Throwable, String] = {
    val region: Region = Region.US_EAST_1

    implicit val s3: S3 = S3.at(region)

    val bucketName: String = "farrell-data-engineering-target"

    val records = input.getRecords

    if (records.isEmpty) {
      Right("Function finished executing on 0 records")
    } else {
      val bucket: Either[Exception, Bucket] = s3.bucket(bucketName).toRight(new Exception("Retrieving Records from S3"))

      bucket.flatMap(b => {
        val recordsScala = records.asScala.toList
        val slackMessageList = recordsScala.map(_.getS3.getObject.getKey).map(parseS3Object(b, s3)).flatMap(_.getOrElse(List.empty))
        writeToDynamo(tableName = "messages-one", region = region)(slackMessageList)
      })
    }
  }

  def parseS3Object(b: Bucket, s3: S3)(x: String): Either[Throwable, List[SlackMessage]] = {
    val obj: Try[Option[S3Object]] = Try(s3.get(b, x))

    obj.toEither.flatMap(ob => {
      ob.toRight(new Exception("Object not found in s3 service")).map(o => {
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
        return Right("Messages put successfully")
      })
    })
  }

  def putToDynamo(table: Table)(message: SlackMessage)(implicit dynamoDB: DynamoDB): Unit = {
    table.put(message.user, message.ts, "Text" -> message.text)
  }
}
