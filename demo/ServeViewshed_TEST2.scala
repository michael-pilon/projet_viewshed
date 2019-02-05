package demo

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
import geotrellis.spark.io.s3.{S3AttributeStore, S3ValueReader}
import geotrellis.spark.io.hadoop.HadoopValueReader
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try
import scala.concurrent._

object ServeViewshed_TEST2 {
  def main(args: Array[String]) {
    val conf =
      new SparkConf()
        .setMaster("local[*]")
        .setAppName("Spark Tile Reader")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)

    val bucket = args(0)
    val catalog = args(1)
    val port = args(2).toInt

    val directions = List((0, -1), (-1, 0), (0, 0), (1, 0), (0, 1))
    val valueReader = S3ValueReader(bucket, catalog)

    def reader(layerId: LayerId) = valueReader.reader[SpatialKey, Tile](layerId)


    implicit val system = ActorSystem("my-system")
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher

    // ref: https://geotrellis.io/0.9docs/rendering.html
    def greyscale(stops: Int): ColorRamp = {
      val colors = (0 to stops)
        .map(i ⇒ {
          val c = java.awt.Color.HSBtoRGB(0f, 0f, i / stops.toFloat)
          (c << 8) | 0xFF // Add alpha channel.
        })
      ColorRamp(colors)
    }

    def parseColorMapDefinition(str: String): Option[CustomizedDoubleColorMap] = {
      val split = str.split(';').map(_.trim.split(':'))
      Try {
        val limits = split.map { pair => pair(0).toDouble }
        val colors = split.map { pair => BigInt(pair(1), 16).toInt }
        require(limits.size == colors.size)
        new CustomizedDoubleColorMap((limits zip colors).toMap)
      }.toOption
    }

    val ramp = greyscale(254)

    val demColorMap = parseColorMapDefinition(ConfigFactory.load().getString("tutorial.demColormap")).get
    val aspectColorMap = ColorMap.fromStringDouble(ConfigFactory.load().getString("tutorial.aspectColormap")).get

    def getSingleTile(layer: LayerId, x: Int, y: Int): Option[Tile] =
      try {
        Some(reader(layer).read(x, y))
      } catch {
        case _: ValueNotFoundError =>
          None
      }

    def getMultipleTile(layer: LayerId, x: Int, y: Int): Option[Tile] =
      try {
        val metadata = valueReader.attributeStore.readMetadata[TileLayerMetadata[SpatialKey]](layer)
        val keyBounds = metadata.bounds
        val tileOpt: Option[Tile] =
          if (keyBounds.includes(SpatialKey(x, y))) {
            val tiles =
              for (d <- directions)
                yield {
                  val col = x + d._1
                  val row = y + d._2

                  try {
                    (SpatialKey(col, row), reader(layer).read(col, row))
                  } catch {
                    case _: ValueNotFoundError =>
                      (SpatialKey(col, row), ArrayTile.empty(metadata.cellType, 256, 256))
                  }
                }

            Some(tiles.stitch())
          }
          else {
            None
          }
        tileOpt
      } catch {
        case _: ValueNotFoundError =>
          None
      }

    def pngAsHttpResponse(png: Png): HttpResponse =
      HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`image/png`), png.bytes))

    def root =
      pathPrefix("datacube" / Segment / Segment / IntNumber / IntNumber / IntNumber) { (layername, render, zoom, x, y) =>
        //parameters("altitude".as[Int] ? 60, "azimuth".as[Int] ? 315) { (altitude, azimuth) =>
        parameters("altitude".as[Int] ? 60, "azimuth".as[Int] ? 315) { (altitude, azimuth) =>
          complete {
            Future {
              val equidistance: Map[Int, Int] = Map(17 -> 1, 16 -> 2, 15 -> 4, 14 -> 5, 13 -> 10)
              val layer = LayerId(layername, zoom)

              //val altitude = 60
              //val azimuth = 315

              render match {
                case "grey" =>
                  val tileOpt: Option[Tile] = getSingleTile(layer, x, y)
                  tileOpt.map { tile =>
                    // ApproxViewshed
                    val test = geotrellis.raster.viewshed.R2Viewshed(tile,200,200)
                    // Render as a PNG
                    val png = test.renderPng(GreyPngEncoding(1))
                    pngAsHttpResponse(png)
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
