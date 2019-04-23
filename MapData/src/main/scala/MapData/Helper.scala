package MapData

import play.api.libs.json.{JsError, JsResult, JsSuccess, JsValue}

object Helper {
  def parseMessages(jsonInput: JsValue): List[SlackMessage] = {
    // TODO allow for individual message parse failure, without failing the whole thing
    val slackMessages: JsResult[SlackMessages] = jsonInput.validate[SlackMessages]
    slackMessages match {
      case JsSuccess(slackMessages: SlackMessages, jsPath) => {
        slackMessages.messages
      }
      case JsError(e) => List.empty[SlackMessage]
    }
  }
}