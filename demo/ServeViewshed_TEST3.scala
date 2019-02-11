package demo
import java.util.concurrent.atomic.LongAdder
import java.net.URLDecoder

import akka.actor._
import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.{ActorMaterializer, Materializer}
import com.sun.org.apache.xpath.internal.operations.Or
import com.typesafe.config.ConfigFactory
import fs2.async.mutable
import geotrellis.raster._
import geotrellis.raster.mapalgebra.focal
import geotrellis.raster.render.{CustomizedDoubleColorMap, _}
import geotrellis.raster.render.png._
import geotrellis.raster.mapalgebra.focal.Square
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

object ServeViewshed_TEST3 {
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
    val layer_name = args(3)
    val layer_name_dsm = args(4)

    //final case class FeatureData(DB_data_DAUID: String)
    //implicit val featureDataFormat: JsonReader[FeatureData] = jsonFormat1(FeatureData)


    // Create a reader that will read in the indexed tiles we produced in IngestImage.
    val valueReader = S3ValueReader("geobase-data", "dem")
    val collectionReader = S3CollectionLayerReader(bucket, catalog)

    def reader(layerId: LayerId) = valueReader.reader[SpatialKey, Tile](layerId)
   // def reader2(layerId: LayerId) = valueReader.reader[SpatialKey, Tile](layerId)

    implicit val system = ActorSystem("my-system")
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher

    def pngAsHttpResponse(png: Png): HttpResponse =
      HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`image/png`), png.bytes))

    def jsonAsHttpResponse(point: String): HttpResponse =
      HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/json`), point))

    def htmlAsHttpResponse(content: String): HttpResponse =
      HttpResponse(entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, content))

    def greyscale(stops: Int): ColorRamp = {
      val colors = (0 to stops)
        .map(i ⇒ {
          val c = java.awt.Color.HSBtoRGB(0f, 0f, i / stops.toFloat)
          (c << 8) | 0xFF // Add alpha channel.
        })
      ColorRamp(colors)
    }

    val ramp = greyscale(1)

    def root =
      path("datacube" / "dem" / "value") {
        parameters("lat".as[Double].?, "lon".as[Double].?, "x".as[Double].?, "y".as[Double].?, "zoom".as[Int] ? 17, "buff".as[Double] ? 1000.0, "algo".as[Int] ? 1, "height".as[Double] ? 0.0) { (lat, lon, x, y, zoom, buff, algo, height) =>

          val p: Option[Point] = (lat, lon, x, y) match {
            case (Some(lat), Some(lon), None, None) => Some(Reproject(Point(lon, lat), wgs84, webMercator))
            case (None, None, Some(x), Some(y)) => Some(Point(x, y))
            case _ => None
          }

          if (p.isEmpty) {
            complete(StatusCodes.BadRequest)
          } else {
            complete {
              Future {
                val layerId_dem = LayerId(layer_name, zoom)
                val layerId_dsm = LayerId(layer_name_dsm, zoom)

                // On va chercher la valeur d'élévation sur le DEM
                val dem = collectionReader
                  .query[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId_dem)
                  .where(Contains(p.get)).result.stitch

                val pt = dem.rasterExtent.mapToGrid(p.get)

                // On ajoute la hauteur entrée en paramètre à l'élévation
                val offseta = dem.tile.getDouble(pt._1,pt._2) + height

                // Requête qui va chercher les tuiles du DSM
                val poly = p.get.buffer(buff)
                val dsm = collectionReader
                  .query[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId_dsm)
                  .where(Intersects(poly)).result.stitch

                // Algo 1 sort un résultat png => mosaïquage et polygonization ne fct pas
                // avec l'utilisation de mutable

                if (algo == 1) {
                  dsm.tile.mutable.setDouble(pt._1,pt._2,offseta)
                  val result = geotrellis.raster.viewshed.ApproxViewshed(dsm.tile,pt._1,pt._2)
                  val filter = result.focalMedian(Square(1))
                  val png = filter.renderPng(ramp)
                  pngAsHttpResponse(png)
                }

                // Algo 2 sort un résultat GeoJSON => hauteur et mosaïquage ne fct pas
                else{
                  dsm.tile.mutable.setDouble(pt._1,pt._2,offseta)
                  val result = geotrellis.raster.viewshed.ApproxViewshed(dsm.tile,pt._1,pt._2)
                  val filter = result.focalMedian(Square(1))
                  filter.mapDouble { v => if(v == 0) NODATA else v }
                  val resultpoly = geotrellis.raster.vectorize.Vectorize(filter,dsm.extent)
                  val geojson: String = resultpoly.toGeoJson
                  jsonAsHttpResponse(geojson)

                  //val mutableTile = raster2.tile.mutable
                  //raster2.tile.mutable.setDouble(pt._1,pt._2,dem_height)
                  //val nodata: Option[Double] = 0.0
                  //val customCellType = IntUserDefinedNoDataCellType(0)
                  //val customTile = IntArrayTile(result,result.length,result.length,customCellType)
                  // val png = result.renderPng(ramp)
                }
              }
            }
          }
        }
      } ~
        pathEndOrSingleSlash {
          complete(StatusCodes.OK)
        }

    val bindingFuture = Http().bindAndHandle(root, "0.0.0.0", port)
  }
}
