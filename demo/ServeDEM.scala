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

object ServeDEM {
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
                case "color" =>
                  val tileOpt: Option[Tile] = getSingleTile(layer, x, y)
                  tileOpt.map { tile =>
                    val test = tile.mapDouble({ cellValue: Double => demColorMap.mapDouble(cellValue) })

                    // Render as a PNG
                    val png = test.renderPng(RgbPngEncoding(-1))
                    pngAsHttpResponse(png)
                  }
                case "grey" =>
                  val tileOpt: Option[Tile] = getSingleTile(layer, x, y)
                  tileOpt.map { tile =>
                    // Render as a PNG
                    val png = tile.renderPng(ramp)
                    pngAsHttpResponse(png)
                  }
                case "contour" =>
                  val tileOpt: Option[Tile] = getSingleTile(layer, x, y)
                  tileOpt.map { tile =>
                    val contour = tile.mapDouble(z => z / equidistance(zoom)).flowDirection()
                    // Render as a PNG
                    val png = contour.renderPng(GreyPngEncoding)
                    pngAsHttpResponse(png)
                  }
                case "hillshade" =>
                  //val altitude = 60
                  val scale = 0.0000007440927 * Math.pow(zoom, 5.678745) //(0,0.01),(12,1),(18,10)
                //println(scale)
                val tileOpt: Option[Tile] = getMultipleTile(layer, x, y)
                  tileOpt.map { tile =>
                    val hillshade = tile.crop(254, 254, 513, 513).mapDouble { z => z * scale }.hillshade(CellSize.fromString("3,3"), altitude = altitude, azimuth = azimuth).crop(2, 2, 257, 257)
                    //val hillshade = tile.mapDouble{ z => z*scale }.hillshade(CellSize.fromString("3,3"), altitude=altitude)

                    val normalized = hillshade.normalize(0, 127, 1, 254).combine(getSingleTile(layer, x, y).get) { (z1, z2) =>
                      if (z2 == -2147483648) 0 // TODO: extract no data value from tile object
                      else z1
                    }
                    //println(hillshade.asciiDrawRange(0,10,0,10))
                    val png = normalized.renderPng(GreyPngEncoding(0))
                    pngAsHttpResponse(png)
                  }
                case "aspect" =>
                  val tileOpt: Option[Tile] = getSingleTile(layer, x, y)
                  tileOpt.map { tile =>
                    val aspect = tile.aspect(CellSize.fromString("3,3"))

                    //                val normalized = hillshade.normalize(0, 127, 1, 254).combine(tile){ (z1, z2) =>
                    //                  if (z2 == -2147483648) 0  // TODO: extract no data value from tile object
                    //                  else z1
                    //                }

                    val png = aspect.renderPng(aspectColorMap.withBoundaryType(GreaterThanOrEqualTo))
                    pngAsHttpResponse(png)
                  }
                case "slope" =>
                  val tileOpt: Option[Tile] = getMultipleTile(layer, x, y)
                  //val scale = 0.00000001063148 * Math.pow(zoom, 7.388385) //(0,0.01),(10,4),(18,20)
                  // y = 4255322 + (0.7242286 - 4255322)/(1 + (x/122.3784)^6.419806) //(5,0.5) (10, 1.5) (13,3) (18,20)
                  val scale = 0.00001746438 * Math.pow(zoom, 4.826353) //(6,0.5),(10,1),(13,4),(18,20)
                  tileOpt.map { tile =>
                    //println((zoom, scale))

                    //val slope = tile.mapDouble { z => z * scale }.slope(CellSize.fromString("3,3"))
                    val slope = tile.crop(254, 254, 513, 513).mapDouble { z => z * scale }.slope(CellSize.fromString("3,3")).crop(2, 2, 257, 257)
                    //println(slope.findMinMaxDouble)
                    val normalized = slope.normalize(0, 100, 1, 254).combineDouble(getSingleTile(layer, x, y).get) { (z1, z2) =>
                      if (z2 == -2147483648) 0 // TODO: extract no data value from tile object
                      else z1
                    }
                    //println(normalized.findMinMaxDouble)

                    val png = normalized.renderPng(GreyPngEncoding(0))
                    pngAsHttpResponse(png)
                  }
                case "color-hillshade" =>
                  // FIXME: review color blending and use getMultipleTile instead
                  val tileOpt: Option[Tile] = getMultipleTile(layer, x, y)
                  tileOpt.map { tile =>
                    val dem = getSingleTile(layer, x, y).get
                    val scale = 0.0000007440927 * Math.pow(zoom, 5.678745)
                    val hillshade = tile.crop(254, 254, 513, 513).mapDouble { z => z * scale }.hillshade(CellSize.fromString("3,3"), altitude = altitude, azimuth = azimuth).crop(2, 2, 257, 257).normalize(0, 127, 1, 254)

                    val color = dem.mapDouble({ cellValue: Double => demColorMap.mapDouble(cellValue) })

                    val output = color.combine(hillshade) { (z1, z2) =>
                      val delta = 255 - z2
                      val rgb = RGBA(z1).unzipRGBA
                      RGBA((rgb._2 - delta).max(1), (rgb._3 - delta).max(1), (rgb._4 - delta).max(1), 255).toARGB
                    }

                    val masked = output.combine(dem) { (z1, z2) =>
                      if (z2 == -2147483648) 0 // TODO: extract no data value from tile object
                      else z1
                    }

                    val png = masked.renderPng(RgbPngEncoding(0))
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
