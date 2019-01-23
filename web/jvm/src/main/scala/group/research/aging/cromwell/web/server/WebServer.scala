package group.research.aging.cromwell.web.server

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.xml.ScalaXmlSupport._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server._
import akka.http.scaladsl.{Http, HttpExt, model}
import akka.stream.ActorMaterializer
import cats.effect.IO
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import de.heikoseeberger.akkahttpcirce._
import group.research.aging.cromwell.web.api.RestAPI
import group.research.aging.cromwell.web.communication.WebsocketServer
import hammock.akka.AkkaInterpreter
import io.circe.generic.auto._
import scalacss.DevDefaults._
import wvlet.log.LogFormatter.SourceCodeLogFormatter
import wvlet.log.{LogLevel, LogSupport, Logger}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.xml.Unparsed

// Server definition
object WebServer extends HttpApp with FailFastCirceSupport with LogSupport {

  //override val apiClasses: Set[Class[_]] = Set.empty//(classOf[PetService], classOf[UserService], classOf[StoreService])


  // Set the default log formatter
  Logger.setDefaultFormatter(SourceCodeLogFormatter)
  Logger.setDefaultLogLevel(LogLevel.DEBUG)

  lazy val webjarsPrefix = "lib"
  lazy val resourcePrefix = "public"
  implicit val materializer = ActorMaterializer()(system)

  //implicit  protected def getInterpreter: Interpreter[IO] = Interpreter[IO]
  implicit protected def getInterpreter: AkkaInterpreter[IO] =
    new AkkaInterpreter[IO](http)

  def mystyles: Route =  path("styles" / "mystyles.css"){
    complete  {
      HttpResponse(  entity = HttpEntity(MediaTypes.`text/css`.withCharset(HttpCharsets.`UTF-8`),  MyStyles.render   ))   }
  }

  def loadResources: Route = pathPrefix(resourcePrefix ~ Slash) {
    getFromResourceDirectory("")
  }


  def webjars: Route = pathPrefix(webjarsPrefix ~ Slash)  {  getFromResourceDirectory(webjarsPrefix)  }


  def un(str: String): Unparsed = scala.xml.Unparsed(str)

  def index: Route =  pathSingleSlash{ ctx=> ctx.complete {
      <html>
        <head title="Cromwell client UI">
          <script type="text/javascript" src="/lib/jquery/jquery.min.js"></script>
          <link rel="stylesheet" href="/lib/Semantic-UI/semantic.css"/>
          <script type="text/javascript" src="/lib/Semantic-UI/semantic.js"></script>
          <link rel="stylesheet" href="/styles/mystyles.css"/>
        </head>
        <body id="main">

        </body>
        <script src="/public/cromwell-web-fastopt.js" type="text/javascript"></script>
      </html>
    }
  }

  def assets: Route =  pathPrefix("assets" / Remaining) { file =>
    // optionally compresses the response with Gzip or Deflate
    // if the client accepts compressed responses
    encodeResponse {
      getFromResource("public/" + file)
    }
  }

  def browse: Route = pathPrefix("data" / Remaining) { file =>
    val folder = scala.util.Properties.envOrElse("DATA", "/data") + "/" + file
    getFromBrowseableDirectories(folder)
  }


  implicit lazy val http: HttpExt = Http(this.systemReference.get())

  def proxy(request: HttpRequest, url: model.Uri): Future[RouteResult] = {
    println("url to send is: " + url.toString())
    val proxyRequest = HttpRequest(uri = url,
      headers = Nil,
      entity = request.entity,
      method = request.method)
    http.singleRequest(proxyRequest).map{ p=>
      val r  = p.addHeader(headers.`Access-Control-Allow-Origin`.*)
      RouteResult.Complete(r)
    }
  }
  def redirect: Route =  {
    pathPrefix("http" ~ Remaining) {
      u => ctx =>
        val uri  = model.Uri("http" + u)
        proxy(ctx.request, uri)
        //redirect(url, StatusCodes.TemporaryRedirect)
      }
  }

  import cats.effect.IO
  import cats.free.Free
  import hammock._
  import hammock.marshalling._

  def get(path: String, headers: Map[String, String] = Map.empty): Free[HttpF, HttpResponse] = Hammock.request(Method.GET, Uri.unsafeParse(path), headers)

  def getIO[T](subpath: String, headers: Map[String, String] = Map.empty)(implicit D: Decoder[T], M: MarshallC[HammockF]): IO[T] =
    get(subpath, headers).as[T](D, M).exec[IO]

  implicit lazy val system = ActorSystem("chat")

  lazy val websocketServer: WebsocketServer = new WebsocketServer(system)

  override def routes: Route = cors(){
    index~
    websocketServer.route ~
    RestAPI.routes ~
    webjars ~
    mystyles ~
    assets ~
    browse ~
    loadResources ~
    redirect
  }

}
