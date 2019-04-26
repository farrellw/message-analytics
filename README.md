# message-data-pipeline
Pair of scala serverless applications. Overall goal is to pipe a set of raw slack messages into data that can be analyzed.

## Applications
### Save File
Takes the post body from a request and saves it to s3 as a json file.

### Map Data
Triggered by s3 uploads. Parses the JSON file into distinct slack messages. Saves each slack message to DynamoDB.

## Dependencies
- Scala
- Node/NPM is used to manage the serverless framework, used in this case to deploy to AWS.
- SBT is used to import and package the scala dependencies into a .jar
