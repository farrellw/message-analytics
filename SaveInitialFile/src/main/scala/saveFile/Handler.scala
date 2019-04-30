package saveFile

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}

import scala.collection.JavaConverters
import awscala._
import com.amazonaws.services.lambda.runtime.events.{APIGatewayProxyRequestEvent}
import s3._

class ApiGatewayHandler extends RequestHandler[APIGatewayProxyRequestEvent, ApiGatewayResponse] {

  def handleRequest(input: APIGatewayProxyRequestEvent, context: Context): ApiGatewayResponse = {
    implicit val s3 = S3.at(Region.US_EAST_1)

    val result = s3.putObject("database-0", "records/" + new DateTime().toString() + ".json", input.getBody())

    val headers = Map("x-custom-response-header" -> "my custom response header value")
    ApiGatewayResponse(200, " Your function executed successfully! File Successfully saved to S3.",
      JavaConverters.mapAsJavaMap[String, Object](headers),
      true)
  }
}
