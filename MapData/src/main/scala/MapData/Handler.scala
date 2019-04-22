package MapData

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import com.amazonaws.services.s3.event.S3EventNotification
import awscala._, dynamodbv2._
import s3.{Bucket, S3, S3Object}
import play.api.libs.json._
import scala.util.{Try, Success, Failure}

class Handler extends RequestHandler[S3EventNotification, Either[Exception, String]] {

  def handleRequest(input: S3EventNotification, context: Context): Either[Exception, String] = {
    val region: Region = Region.US_EAST_1

    implicit val s3 = S3.at(region)

    val bucketName: String = "farrell-data-engineering-target"

    val records = input.getRecords
    if (records.isEmpty()) {
      return Right("Function finished executing on 0 records")
    } else {
      val bucket: Option[Bucket] = s3.bucket(bucketName)

      bucket match {
        case Some(b) => {
          records.forEach((x: S3EventNotification.S3EventNotificationRecord) => {
            val exceptionOrString = transferRecords(x, b, region, s3)
            exceptionOrString match {
              case Left(value) => {
                return Left(value)
              }
            }
          })
          return Right("Your function finished executing successfully")
        }
        case None => Left(new Exception("No bucket found with name " + bucketName))
      }
    }
  }

  def transferRecords(record: S3EventNotification.S3EventNotificationRecord, bucket: Bucket, region: Region, s3: S3): Either[Exception, String] = {
    val key: String = record.getS3.getObject.getKey
    val obj: Option[S3Object] = s3.get(bucket, key)

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
            val tableName: String = "messages-one"

            return writeToDynamo(tableName, slackMessages, region)
          }
          case JsError(e) => return Left(new Exception("An error occurred validating JSON"))
        }
      }
      case None => return Left(new Exception("Error Accessing key " + key))
    }
  }

  def writeToDynamo(tableName: String, slackMessages: SlackMessages, region: Region): Either[Exception, String] = {
    implicit val dynamoDB = DynamoDB.at(region)

    val tableOrError: Try[Option[Table]] = Try(dynamoDB.table(tableName))
    tableOrError match {
      case Success(table: Option[Table]) => {
        table match {
          case Some(t) => {
            val messages = slackMessages.messages
            messages.foreach(m => t.put(m.user, m.ts, "Text" -> m.text))
            return Right("Messages put successfully")
          }
          case None => {
            return Left(new Exception("Cannot find table name " + tableName))
          }
        }
      }
      case Failure(e) => return Left(new Exception("Failed connecting to dynamoDB"))
    }
  }
}
