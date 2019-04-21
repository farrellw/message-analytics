package MapData

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import com.amazonaws.services.s3.event.S3EventNotification
import awscala._
import s3.{Bucket, S3, S3Object}
import play.api.libs.json._

class Handler extends RequestHandler[S3EventNotification, (String, Exception)] {

  def handleRequest(input: S3EventNotification, context: Context): (String, Exception) = {
    implicit val s3 = S3.at(Region.US_EAST_1)

    val bucketName: String = "farrell-data-engineering-target"
    val executionFinished: String = "Your function finished executing"

    val maybeBucket: Option[Bucket] = s3.bucket(bucketName)

    maybeBucket match {
      case Some(bucket) => {
        val records = input.getRecords

        records.forEach((x: S3EventNotification.S3EventNotificationRecord)  => {
          val key = x.getS3.getObject.getKey
          val obj: Option[S3Object] = s3.get(bucket, key)

          obj match {
            case Some(o) => {
              val stream = o.content
              val jsonInput: JsValue = try {  Json.parse(stream) } finally { stream.close() }

              val slackMessages: JsResult[SlackMessages] = jsonInput.validate[SlackMessages]

              slackMessages match {
                case JsSuccess(slackMessages: SlackMessages, jsPath) => {
                  slackMessages.messages.foreach(println)
                  return (executionFinished + "successfully", null)
                }
                case JsError(e) => return (executionFinished, new Exception("An error occurred validating JSON"))
              }
            }
            case None => return (executionFinished, new Exception("Error Accessing key " + key))
          }
        })
      }
      case None => return (executionFinished, new Exception("No bucket found with name " + bucketName))
    }


    (executionFinished, null)
  }
}
