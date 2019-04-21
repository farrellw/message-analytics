package MapData

import play.api.libs.json.{JsPath, Reads}
import play.api.libs.functional.syntax._


case class SlackMessage(text: String, user: String)

object SlackMessage {
  implicit val slackMessageReads: Reads[SlackMessage] = (
    (JsPath \ "text").read[String] and
      (JsPath \ "user").read[String]
    ) (SlackMessage.apply _)
}


case class SlackMessages(messages: List[SlackMessage], ok: Boolean)

object SlackMessages {
  implicit val slackMessagesReads: Reads[SlackMessages] = (
    (JsPath \ "messages").read[List[SlackMessage]] and
      (JsPath \ "ok").read[Boolean]
    ) (SlackMessages.apply _)
}
