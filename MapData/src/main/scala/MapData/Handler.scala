package MapData

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import com.amazonaws.services.s3.event.S3EventNotification
import awscala._, dynamodbv2._
import s3.{Bucket, S3, S3Object}
import play.api.libs.json._
import scala.util.{Try, Success, Failure}

class Handler extends RequestHandler[S3EventNotification, (String, Exception)] {

  def handleRequest(input: S3EventNotification, context: Context): (String, Exception) = {
    val region: Region = Region.US_EAST_1

    implicit val s3 = S3.at(region)
    implicit val dynamoDB = DynamoDB.at(region)

    val bucketName: String = "farrell-data-engineering-target"
    val executionFinishedError: String = "Your function finished executing with an error: "
    val executionFinished: String = "Your function finished executing successfully"

    // TODO only access bucket if input.getRecords is not empty
    val maybeBucket: Option[Bucket] = s3.bucket(bucketName)

    maybeBucket match {
      case Some(bucket) => {
        val records = input.getRecords


        records.forEach((x: S3EventNotification.S3EventNotificationRecord) => {
          val key: String = x.getS3.getObject.getKey
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
                  val tableOrError: Try[Option[Table]] = Try(dynamoDB.table(tableName))

                  tableOrError match {
                    case Success(table: Option[Table]) => {
                      table match {
                        case Some(t) => {
                          val messages = slackMessages.messages
                          messages.foreach(m => t.put(m.user, m.ts, "Text" -> m.text))
                        }
                        case None => return (executionFinishedError, new Exception("Cannot find table name " + tableName))
                      }

                    }
                    case Failure(e) => return (executionFinishedError, new Exception("Failed connecting to dynamoDB"))
                  }
                }
                case JsError(e) => return (executionFinishedError, new Exception("An error occurred validating JSON"))
              }
            }
            case None => return (executionFinishedError, new Exception("Error Accessing key " + key))
          }
        })
        (executionFinished, null)
      }
      case None => return (executionFinishedError, new Exception("No bucket found with name " + bucketName))
    }
  }
}
