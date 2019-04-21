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

    val maybeBucket: Option[Bucket] = s3.bucket(bucketName)

    maybeBucket match {
      case Some(bucket) => {
        val records = input.getRecords

        // TODO how to use map instead of forEach on something that isn't an array
        records.forEach((x: S3EventNotification.S3EventNotificationRecord)  => {
          val key = x.getS3.getObject.getKey
          val obj: Option[S3Object] = s3.get(bucket, key)

          obj.foreach(o => {
            val stream = o.content
            val jsonInput: JsValue = try {  Json.parse(stream) } finally { stream.close() }

            val slackMessages: JsResult[SlackMessages] = jsonInput.validate[SlackMessages]

            slackMessages match {
              case JsSuccess(slackMessages: SlackMessages, jsPath) => {
                slackMessages.messages.foreach(println)
              }
              case JsError(e) => println(e)
            }
          })
        })
      }
      case None => return ("Your function finished executing", new Exception("No bucket found with key " + bucketName))
    }


    ("Go Serverless v1.0! Your function executed successfully!", null)
  }
}
