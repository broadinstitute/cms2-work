package group.research.aging.cromwell.web.communication

import group.research.aging.cromwell.web.utils.SimpleSourceFormatter
import org.scalajs.dom
import org.scalajs.dom.raw.{Event, MessageEvent, WebSocket}
import wvlet.log.{LogLevel, LogSupport, Logger}
import io.circe.parser.decode
import mhtml.Var

object WebsocketClient {

  def fromRelativeURL(rel: String): WebsocketClient = {
    val wsProtocol = if (dom.document.location.protocol == "https:") "wss" else "ws"
    val url = s"$wsProtocol://${dom.document.location.host}/${rel}"
    new WebsocketClient(url)
  }

}

class WebsocketClient(url: String) extends WebsocketSubscriber(url) with LogSupport{

  Logger.setDefaultFormatter(SimpleSourceFormatter)

  Logger.setDefaultLogLevel(LogLevel.DEBUG)
  debug(s"starting websocket with address ${url}")

  lazy val opened = Var(false)

  lazy val messages: Var[WebsocketMessages.WebsocketAction] = Var(WebsocketMessages.WebsocketAction.empty)
  lazy val toSend: Var[WebsocketMessages.WebsocketAction] = Var(WebsocketMessages.WebsocketAction.empty)

  override def subscribe(w:WebSocket) = {
    super.subscribe(w)
    uglyUpdate()
  }

  protected def uglyUpdate() = {
    onOpen.impure.run{v =>
      debug(v)
      opened := true
    }
    onClose.impure.run{
      v =>
        debug(v)
        opened := false
    }

    onError.impure.run{
      e =>
        debug(e)
    }

    onMessage.impure.run{m =>
      /*
      Option(m.data) match {
        case Some(data) =>
          decode[WebsocketMessages.WebsocketAction](data.toString) match {
            case Left(er)=>
              error(er)
            case Right(message) =>
              debug(message)
              messages := message
          }

        case None =>
          error("NULL MESSAGE!")
          debug(m)
      }
      */
    }
    toSend.zip(opened).impure.run{
      case (m, false) =>
        import io.circe.generic._
        debug(s"sending a message $m while websocket is still closed")

      case (m, true) =>
        import io.circe.syntax._
        //val json = m.asJson
        //send(json.toString())
    }
  }

}
