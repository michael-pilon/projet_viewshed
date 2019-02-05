package demo

import java.util.concurrent.atomic.LongAdder
import java.net.URLDecoder

import akka.actor._
import akka.event.{Logging, LoggingAdapter}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.{ActorMaterializer, Materializer}
import com.typesafe.config.ConfigFactory

import spray.json._
import spray.json.DefaultJsonProtocol._

import scala.util.Try
import scala.concurrent._

object ServeHello {
  def main(args: Array[String]) {

    implicit val system = ActorSystem("my-system")
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher

    val greetings = args(0)
    val port = args(1).toInt

    def jsonAsHttpResponse(content: String): HttpResponse =
      HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/json`), content))

    def htmlAsHttpResponse(content: String): HttpResponse =
      HttpResponse(entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, content))

    def root =
      pathEndOrSingleSlash {
        parameters("name".as[String].?) { (name) =>

          complete(htmlAsHttpResponse("<h1>" + greetings + " " + name.getOrElse("World") + "</h1>"))
          
        }
      }

    val bindingFuture = Http().bindAndHandle(root, "0.0.0.0", port)
  }
}
