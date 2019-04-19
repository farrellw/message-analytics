package MapData

import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}

import awscala._
import com.amazonaws.services.lambda.runtime.events.{S3Event}

class Handler extends RequestHandler[S3Event, (String, Error)] {

  def handleRequest(input: S3Event, context: Context): (String, Error) = {
    println("Triggered S3 handle request")
    ("Go Serverless v1.0! Your function executed successfully!", null)
  }
}
