package MapData

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import com.amazonaws.services.s3.event.S3EventNotification
import awscala._, dynamodbv2._
import s3.{Bucket, S3, S3Object}
import play.api.libs.json._
import scala.util.{Try}
import scala.collection.JavaConverters._

class Handler extends RequestHandler[S3EventNotification, Either[Throwable, String]] {

  def handleRequest(input: S3EventNotification, context: Context): Either[Throwable, String] = {
    val region: Region = Region.US_EAST_1

    implicit val s3 = S3.at(region)

    val bucketName: String = "farrell-data-engineering-target"

    val records = input.getRecords

    if (records.isEmpty) {
      Right("Function finished executing on 0 records")
    } else {
      val bucket: Either[Exception, Bucket] = s3.bucket(bucketName).toRight(new Exception("Retrieving Records from S3"))

      bucket.map(b => {
        val recordsScala = records.asScala.toList
        recordsScala.map(_.getS3.getObject.getKey).flatMap(parseS3Object(b, s3))
      }).flatMap(writeToDynamo(tableName = "messages-one", region = region))
    }
  }

  def parseS3Object(b: Bucket, s3: S3)(x: String): List[SlackMessage] = {
    val obj: Option[S3Object] = s3.get(b, x)

    obj match {
      case Some(o) => {
        val stream = o.content
        val jsonInput: JsValue = try {
          Json.parse(stream)
        } finally {
          stream.close()
        }

        // TODO allow for individual message parse failure, without failing the whole thing
        val slackMessages: JsResult[SlackMessages] = jsonInput.validate[SlackMessages]
        slackMessages match {
          case JsSuccess(slackMessages: SlackMessages, jsPath) => {
            slackMessages.messages
          }
          case JsError(e) => List.empty[SlackMessage]
        }
      }
      case None => List.empty[SlackMessage]
    }
  }

  def writeToDynamo(tableName: String, region: Region)(slackMessages: List[SlackMessage]): Either[Throwable, String] = {
    implicit val dynamoDB: DynamoDB = DynamoDB.at(region)

    Try(dynamoDB.table(tableName)).toEither.flatMap(tab => {
      tab.toRight(new Exception("Table " + tableName + " not found")).map(t => {
        slackMessages.foreach(m => t.put(m.user, m.ts, "Text" -> m.text))
        return Right("Messages put successfully")
      })
    })
  }

  def putToDynamo(table: Table, message: SlackMessage): Unit ={
    table.put(m.user)
  }
}
