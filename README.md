# message-data-pipeline
Pair of scala serverless applications. Overall goal is to pipe a set of raw slack messages into data that can be analyzed.

## Save File
Takes the post body from a request and saves it to s3 as a json file.

## Map Data
Triggered by s3 uploads. Parses the JSON file into distinct slack messages. Saves each slack message to DynamoDB.
