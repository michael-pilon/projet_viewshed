package demo

import java.util.concurrent.atomic.LongAdder
import java.net.URLDecoder

import akka.actor._
import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.{ActorMaterializer, Materializer}
import com.typesafe.config.ConfigFactory
import geotrellis.raster._
import geotrellis.raster.render.{CustomizedDoubleColorMap, _}
import geotrellis.raster.render.png._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.s3.{S3AttributeStore, S3CollectionLayerReader, S3CollectionReader, S3ValueReader}
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vector.io.json.JsonFeatureCollection
import geotrellis.vector.reproject.Reproject
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.vector.io.json.GeoJson
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.immutable.ListMap
import spray.json._
import spray.json.DefaultJsonProtocol._

import scala.collection.concurrent.TrieMap
import scala.util.Try
import scala.concurrent._

object ServeViewshed_TEST1 {
  def main(args: Array[String]) {
    val conf =
      new SparkConf()
        .setMaster("local[*]")
        .setAppName("Spark Tile Reader")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)

    val wgs84 = LatLng
    val webMercator = WebMercator

    val bucket = args(0)
    val catalog = args(1)
    val port = args(2).toInt

    def htmlAsHttpResponse(content: String): HttpResponse =
      HttpResponse(entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, content))

    def pngAsHttpResponse(png: Png): HttpResponse =
      HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`image/png`), png.bytes))

    //final case class FeatureData(DB_data_DAUID: String)
    //implicit val featureDataFormat: JsonReader[FeatureData] = jsonFormat1(FeatureData)


    // Create a reader that will read in the indexed tiles we produced in IngestImage.
    val valueReader = S3ValueReader("geobase-data", "hand")
    val collectionReader = S3CollectionLayerReader("geobase-data", "hand")

    def greyscale(stops: Int): ColorRamp = {
      val colors = (0 to stops)
        .map(i ⇒ {
          val c = java.awt.Color.HSBtoRGB(0f, 0f, i / stops.toFloat)
          (c << 8) | 0xFF // Add alpha channel.
        })
      ColorRamp(colors)
    }

    val ramp = greyscale(254)

    def reader(layerId: LayerId) = valueReader.reader[SpatialKey, Tile](layerId)

    implicit val system = ActorSystem("my-system")
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher

    def root = pathEndOrSingleSlash {
   //   parameters("name".as[String].?) { (name) =>
      val array = Array[Int](0,1,0,0,0,0,0,0,0)
      val tile = IntArrayTile(array,3,3)
      val test1 = geotrellis.raster.viewshed.ApproxViewshed(tile,0,0)

      val pngTile = tile.renderPng(ramp)
      val pngTest1 = test1.renderPng(ramp)

      //complete(htmlAsHttpResponse("<h1>" + tile.asciiDraw() + "</h1>" + "<br>" + test1.asciiDraw() + "</br>"))
      complete(pngAsHttpResponse(pngTile))
      //complete(pngAsHttpResponse(pngTest1))
    }

    val bindingFuture = Http().bindAndHandle(root, "0.0.0.0", port)
    }
}
