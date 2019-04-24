package MapData

import org.scalatest.FunSpec
import play.api.libs.json.{JsValue, Json}


class MapDataTest extends  FunSpec {
  describe("MapData.MapDataTest") {
    it("MapData.MapDataTest Passes A Test") {
      val expected: Int = 27
      val actual: Int = 27
      assert(expected === actual)
    }
  }
  describe("Parsing messages returns a list of slack messages") {
    val validJson: JsValue = Json.parse("""
  {
   "ok": true,
   "messages": [
     {
       "client_msg_id": "80a0f35b-b200-4027-8231-f4328bb0bce0",
       "type": "message",
       "text": "Well done! That looks great!",
       "user": "U4LQ2K3K3",
       "ts": "1555703797.088900",
       "thread_ts": "1554937193.128600",
       "parent_user_id": "U4LQ2K3K3"
     },
     {
       "client_msg_id": "a86d6017-44ca-4316-b63a-35372a06b8be",
       "type": "message",
       "text": "So <@U2QT4UW3W> how will you 'monitor that topic'? Like on Twitter? :stuck_out_tongue_winking_eye:",
       "user": "UA6JYSMFU",
       "ts": "1555696969.088300",
       "thread_ts": "1555591467.053200",
       "parent_user_id": "U2QT4UW3W"
     },
     {
       "client_msg_id": "cde34c3b-56f7-470b-a33e-5e66399882be",
       "type": "message",
       "text": "I'm using the Mark I version of this camera: <https://www.amazon.com/Olympus-Mirrorless-Camera-Megapixels-5-Axis/dp/B01M4MB3DK/ref=sr_1_2?keywords=olympus+omd+em1+mark+i&amp;qid=1555686764&amp;s=gateway&amp;sr=8-2>",
       "user": "U2R1AG876",
       "ts": "1555686790.087400",
       "thread_ts": "1555628532.078900",
       "parent_user_id": "U2R1AG876",
       "reactions": [
         {
           "name": "cool",
           "users": ["U2QTW9R7W"],
           "count": 1
         }
       ]
     }
   ],
   "has_more": true
 }
  """)

    val invalidJson: JsValue = Json.parse("""
  {
   "ok": true,
   "messages": [
     {
       "client_msg_id": "80a0f35b-b200-4027-8231-f4328bb0bce0",
       "type": "message",
       "user": "U4LQ2K3K3",
       "ts": "1555703797.088900",
       "thread_ts": "1554937193.128600",
       "parent_user_id": "U4LQ2K3K3"
     }
   ],
   "has_more": true
 }
  """)
    it("Returns an empty List when invalid json is parsed"){
      val response = Helper.parseMessages(invalidJson)
      val expected = true
      val actual = response.isEmpty
      assert(expected === actual)
    }
    it("Succeeds with List of messages when valid json is parsed"){
      val response = Helper.parseMessages(validJson)
      val expected = false
      val actual = response.isEmpty
      assert(expected === actual)
    }
  }
}
