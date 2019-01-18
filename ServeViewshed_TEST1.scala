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
import geotrellis.raster.Tile
import geotrellis.raster.viewshed.ApproxViewshed
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
    val layer_name = args(3)


    //final case class FeatureData(DB_data_DAUID: String)
    //implicit val featureDataFormat: JsonReader[FeatureData] = jsonFormat1(FeatureData)


    // Create a reader that will read in the indexed tiles we produced in IngestImage.
    val valueReader = S3ValueReader("geobase-data", "hand")
    val collectionReader = S3CollectionLayerReader(bucket, catalog)

    def reader(layerId: LayerId) = valueReader.reader[SpatialKey, Tile](layerId)

    implicit val system = ActorSystem("my-system")
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher

//    case class ResponseData(response: Map[String, Double])
//    object RequestResponseProtocol extends DefaultJsonProtocol {
//      implicit val responseFormat = jsonFormat1(ResponseData)
//    }
    //val format = jsonFormat1(ResponseData)
    //implicit val responseDataFormat: JsonWriter[ResponseData] = format
    //implicit def mapFormat[K :JsonFormat, V :JsonFormat, M[_, _] <: Map[_, _]]: RootJsonFormat[M[K, V]] =
    // DefaultJsonProtocol.mapFormat[K, V].asInstanceOf[RootJsonFormat[M[K, V]]]

//    implicit object AnyJsonFormat extends JsonFormat[Any] {
//      def write(x: Any) = x match {
//        case n: Int => JsNumber(n)
//        case s: String => JsString(s)
//        case b: Boolean if b == true => JsTrue
//        case b: Boolean if b == false => JsFalse
//      }
//      def read(value: JsValue) = value match {
//        case JsNumber(n) => n.intValue()
//        case JsString(s) => s
//        case JsTrue => true
//        case JsFalse => false
//      }
//    }

    // ref: https://geotrellis.io/0.9docs/rendering.html
//    def greyscale(stops: Int): ColorRamp = {
//      val colors = (0 to stops)
//        .map(i ⇒ {
//          val c = java.awt.Color.HSBtoRGB(0f, 0f, i / stops.toFloat)
//          (c << 8) | 0xFF // Add alpha channel.
//        })
//      ColorRamp(colors)
//    }
//
//    def parseColorMapDefinition(str: String): Option[CustomizedDoubleColorMap] = {
//      val split = str.split(';').map(_.trim.split(':'))
//      Try {
//        val limits = split.map { pair => pair(0).toDouble }
//        val colors = split.map { pair => BigInt(pair(1), 16).toInt }
//        require(limits.size == colors.size)
//        new CustomizedDoubleColorMap((limits zip colors).toMap)
//      }.toOption
//    }

//    val ramp = greyscale(254)

    //val handColorMap = parseColorMapDefinition(ConfigFactory.load().getString("tutorial.handColormap")).get
    //val aspectColorMap = ColorMap.fromStringDouble(ConfigFactory.load().getString("tutorial.aspectColormap")).get

    def pngAsHttpResponse(png: Png): HttpResponse =
      HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`image/png`), png.bytes))

    def jsonAsHttpResponse(point: String): HttpResponse =
      HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/json`), point))

    def root =
      path("datacube" / "hand" / "value") {
        parameters("lat".as[Double].?, "lon".as[Double].?, "x".as[Double].?, "y".as[Double].?) { (lat, lon, x, y) =>

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
                val layerId = LayerId(layer_name, 17)

                val raster = collectionReader
                  .query[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
                  .where(Contains(p.get)).result.stitch

                //val point = raster.rasterExtent.mapToGrid(p.get)
                //jsonAsHttpResponse("{ \"value\": " + raster.tile.getDouble(point._1, point._2) + " }")
                pngAsHttpResponse(raster.tile.renderPng(ApproxViewshed(raster.tile,20,20)))
              }
            }
          }
        }
      } ~
//      path("datacube" / "hand" / IntNumber) { (zoom) =>
//        parameters("level".as[Double], "geom".as[String].?, "collection".as[String].?, "step".as[Double]) { (level, geom, collection, step) =>
//          // Largely inspired from this tutorial: https://www.azavea.com/blog/2018/02/13/how-to-build-a-geoprocessing-app-with-geotrellis-and-react/
//          //val json = "{\"type\":\"Polygon\",\"coordinates\":[[[-66.6503959999999,45.9545810000001],[-66.657253,45.957386],[-66.6515219999999,45.9641760000001],[-66.649683,45.9664000000001],[-66.649639,45.9664530000001],[-66.649594,45.966457],[-66.649491,45.966466],[-66.649259,45.9664760000001],[-66.649065,45.9664940000001],[-66.6486399999999,45.9664940000001],[-66.648601,45.966503],[-66.648511,45.966504],[-66.648472,45.9665130000001],[-66.64842,45.9665130000001],[-66.648343,45.966531],[-66.6482139999999,45.9665400000001],[-66.647904,45.9665400000001],[-66.647866,45.966531],[-66.64753,45.9665230000001],[-66.6474009999999,45.9665230000001],[-66.6473109999999,45.966505],[-66.647233,45.9664960000001],[-66.647195,45.966487],[-66.647117,45.9664780000001],[-66.6470909999999,45.9664690000001],[-66.646833,45.9664700000001],[-66.6467949999999,45.966461],[-66.6467039999999,45.966461],[-66.646666,45.9664520000001],[-66.646627,45.9664520000001],[-66.6464589999999,45.9664070000001],[-66.6463689999999,45.966389],[-66.646033,45.966372],[-66.64593,45.9663540000001],[-66.645904,45.9663540000001],[-66.645878,45.966345],[-66.6458009999999,45.9663360000001],[-66.6456719999999,45.9663090000001],[-66.645517,45.9662730000001],[-66.645413,45.9662650000001],[-66.645336,45.966256],[-66.6453099999999,45.9662470000001],[-66.6451939999999,45.9662200000001],[-66.6448329999999,45.9661570000001],[-66.6446779999999,45.966122],[-66.6445229999999,45.9660950000001],[-66.644471,45.966077],[-66.6444069999999,45.966059],[-66.644213,45.9660230000001],[-66.6441609999999,45.9660050000001],[-66.644019,45.965969],[-66.643942,45.965943],[-66.643929,45.965943],[-66.643813,45.965925],[-66.64358,45.9658440000001],[-66.643296,45.9657540000001],[-66.64318,45.9657010000001],[-66.6430119999999,45.965647],[-66.64287,45.9656110000001],[-66.642805,45.965584],[-66.6427669999999,45.9655660000001],[-66.6427409999999,45.965557],[-66.642521,45.9654590000001],[-66.6424949999999,45.96545],[-66.642353,45.9653870000001],[-66.642172,45.9652970000001],[-66.6420689999999,45.965243],[-66.6420429999999,45.9652340000001],[-66.6419789999999,45.96518],[-66.6419529999999,45.9651710000001],[-66.641811,45.9651000000001],[-66.6416819999999,45.9650550000001],[-66.64163,45.9650280000001],[-66.6415519999999,45.9649920000001],[-66.641449,45.9649380000001],[-66.641423,45.9649220000001],[-66.64132,45.9648570000001],[-66.6411649999999,45.9647670000001],[-66.640881,45.9646060000001],[-66.6407379999999,45.964543],[-66.6405829999999,45.964453],[-66.640428,45.964372],[-66.640247,45.964256],[-66.640144,45.9641930000001],[-66.640002,45.964094],[-66.639963,45.964076],[-66.6398859999999,45.964031],[-66.63964,45.963852],[-66.639601,45.963825],[-66.6394979999999,45.963762],[-66.639356,45.9636630000001],[-66.6393169999999,45.963645],[-66.6391619999999,45.9635460000001],[-66.639136,45.963537],[-66.639059,45.963492],[-66.638722,45.963241],[-66.6384639999999,45.963088],[-66.638438,45.9630790000001],[-66.63827,45.962998],[-66.638115,45.9628910000001],[-66.638076,45.9628730000001],[-66.63787,45.9627380000001],[-66.63765,45.962585],[-66.637624,45.9625580000001],[-66.637211,45.962253],[-66.637185,45.962226],[-66.637081,45.9621540000001],[-66.637055,45.9621270000001],[-66.636965,45.9620550000001],[-66.636952,45.9620370000001],[-66.6369129999999,45.962001],[-66.636719,45.9618130000001],[-66.63668,45.961777],[-66.636655,45.961759],[-66.6366419999999,45.9617410000001],[-66.6365249999999,45.9616330000001],[-66.6364349999999,45.961534],[-66.636409,45.9614980000001],[-66.636267,45.961372],[-66.636241,45.9613360000001],[-66.63615,45.9612470000001],[-66.636021,45.9611120000001],[-66.6358529999999,45.960878],[-66.63571,45.96068],[-66.635619,45.9605540000001],[-66.6355809999999,45.9605090000001],[-66.635503,45.960429],[-66.6354249999999,45.960357],[-66.635374,45.9602850000001],[-66.635283,45.9601680000001],[-66.635257,45.9601410000001],[-66.635193,45.960087],[-66.6350889999999,45.9599880000001],[-66.6349729999999,45.959862],[-66.634895,45.959727],[-66.6348169999999,45.9596290000001],[-66.6347529999999,45.9595570000001],[-66.634688,45.9594940000001],[-66.634598,45.9594220000001],[-66.634559,45.9593860000001],[-66.63452,45.9593410000001],[-66.634404,45.959233],[-66.6342999999999,45.9591340000001],[-66.634197,45.9590260000001],[-66.634158,45.9589730000001],[-66.6340539999999,45.958874],[-66.6339639999999,45.958811],[-66.633899,45.9587480000001],[-66.633835,45.958694],[-66.63377,45.958649],[-66.633705,45.958559],[-66.63364,45.958424],[-66.633589,45.9582800000001],[-66.6335759999999,45.958262],[-66.6335109999999,45.958173],[-66.6334459999999,45.95811],[-66.633343,45.9579390000001],[-66.6333299999999,45.9579210000001],[-66.633317,45.9578940000001],[-66.633317,45.957858],[-66.633278,45.9578040000001],[-66.6331739999999,45.95766],[-66.6331089999999,45.9575520000001],[-66.633097,45.9575340000001],[-66.633006,45.957363],[-66.633006,45.9573540000001],[-66.6329929999999,45.9573360000001],[-66.632954,45.957255],[-66.632782,45.9569460000001],[-66.632776,45.956936],[-66.633323,45.956304],[-66.63928,45.9494080000001],[-66.640436,45.9502400000001],[-66.641876,45.95117],[-66.6427389999999,45.9517430000001],[-66.643428,45.9520910000001],[-66.646581,45.9534370000001],[-66.647163,45.9535950000001],[-66.647731,45.953673],[-66.648309,45.9537010000001],[-66.6503959999999,45.9545810000001]]]}"
//
//          val polygon: Option[Polygon] = (geom, collection) match {
//            case (Some(geom), None) => Some(GeoJson.parse[Polygon](geom))
//            case (None, Some(collection)) => {
//              val url = URLDecoder.decode(collection, "UTF-8")
//              println(url)
//              val content = scala.io.Source.fromURL(url).mkString
//              val geometries = GeoJson.parse[JsonFeatureCollection](content).getAllPolygons()
//              if (geometries.nonEmpty) Some(geometries.head) else None
//            }
//            case _ => None
//          }
//
//          if (polygon.isEmpty) {
//            complete(StatusCodes.BadRequest)
//          } else {
//            val p = polygon.get.reproject(wgs84, webMercator)
//
//            val counters: TrieMap[Int, LongAdder] = TrieMap.empty
//            val init = () => new LongAdder
//            val update = (_: LongAdder).increment()
//
//            //val layerId = LayerId("handdepth", zoom)
//            val layerId = LayerId("handextent-lidar", zoom)
//            val raster = collectionReader
//              .query[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
//              .where(Intersects(p)).result
//            val metadata = raster.metadata
//
//            val total = new LongAdder
//            val flooded = new LongAdder
//
//            raster.foreach({ case (key: SpatialKey, tile: Tile) =>
//              val extent = metadata.mapTransform(key)
//              val re = RasterExtent(extent, metadata.layout.tileCols, metadata.layout.tileRows)
//              Rasterizer.foreachCellByPolygon(p, re) { case (col, row) =>
//                total.increment()
//                val value = tile.getDouble(col, row)
//
//                if (value < level) {
//                  flooded.increment()
//                  val depth = math.ceil((level - value) / step).toInt
//                  update(counters.getOrElseUpdate(depth, init()))
//                } else {
//                  update(counters.getOrElseUpdate(0, init()))
//                }
//              }
//            })
//
//
//            //          complete(jsonAsHttpResponse("{\n" +
//            //            "\t\"flooded\": {\n" +
//            //            "\t\t\"low\": " + "%.4f".format(low.doubleValue() / total.longValue()) + ",\n" +
//            //            "\t\t\"medium\": " + "%.4f".format(medium.doubleValue() / total.longValue()) + ",\n" +
//            //            "\t\t\"high\": " + "%.4f".format(high.doubleValue() / total.longValue()) + ",\n" +
//            //            "\t\t\"total\": " + "%.4f".format(flooded.doubleValue() / total.longValue()) + "\n" +
//            //            "\t},\n" +
//            //            "\t\"area\": " + polygon.area.toInt + "\n" +
//            //            "}"))
//
//            val content = counters.map { case (k, v) => k.toString -> BigDecimal(v.sum.toDouble / total.longValue()).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble }.toMap
//            //val sorted = ListMap(content.toSeq.sortBy(_._1.toInt):_*)
//            //val output = sorted.map { case(k,v) => k.toString -> v }.toMap
//
//            //println(sorted)
//            //println(output)
//            //println(output.toJson.toString())
//
//            complete(jsonAsHttpResponse(("{" +
//              //"\t\"id\": " +  + ", \n" +
//              "\"area\": " + p.area.toInt + "," +
//              "\"flooded\": { " +
//              "\"coverage\": " + "%.4f".format(flooded.doubleValue() / total.longValue()) + "," +
//              "\"intervals\": " +
//              content.toJson +
//              "} }").parseJson.prettyPrint))
//          }
//        }
//      } ~
//      pathPrefix("datacube" / Segment / Segment / IntNumber / IntNumber / IntNumber) { (render, suffix, zoom, x, y) =>
//        parameters("level".as[Double]) { (level) =>
//          complete {
//            Future {
//              // Read in the tile at the given z/x/y coordinates.
//              val handExtentLayerName = "handextent-" + suffix
//              val handDepthLayerName = "handdepth-" + suffix
//
//              val handextent: Option[Tile] =
//                try {
//                  //ServeDEMAccumulo.valueReader.reader[SpatialKey, Tile](LayerId("layer_name", zoom)).read(x, y)
//                  val layer = LayerId(handExtentLayerName, zoom)
//                  Some(reader(layer).read(x, y))
//                } catch {
//                  case _: ValueNotFoundError =>
//                    None
//                }
//
//              val handdepth: Option[Tile] =
//                try {
//                  //ServeDEMAccumulo.valueReader.reader[SpatialKey, Tile](LayerId("layer_name", zoom)).read(x, y)
//                  val layer = LayerId(handDepthLayerName, zoom)
//                  Some(reader(layer).read(x, y))
//                } catch {
//                  case _: ValueNotFoundError =>
//                    None
//                }
//
//              //println(handextent.get.getDouble(0,0))
//              //println(handdepth.get.getDouble(0,0))
//              val hand = Some(handdepth.get.combineDouble(handextent.get) { (depth, ext) =>
//                ext match {
//                  case value if value == 0.0 => Double.NaN
//                  case value if value <= level && value > -1.0 => level - depth
//                  case _ => Double.NaN
//                }
//              })
//              //println(hand.get.getDouble(0,0))
//
//              render match {
//                case "hand" =>
//                  hand.map { tile =>
//                    // Render as a PNG
//                    val flood = tile.mapDouble({ cellValue: Double =>
//                      cellValue match {
//                        case x if x.isNaN => 0
//                        //case Double.NaN => 0
//                        case _ => handColorMap.mapDouble(cellValue)
//                      }
//                    })
//                    //println(test.getDouble(0,0))
//                    val png = flood.renderPng(RgbPngEncoding(0))
//                    pngAsHttpResponse(png)
//                  }
//              }
//            }
//          }
//        }
//      } ~
      pathEndOrSingleSlash {
        complete(StatusCodes.OK)
      }

    val bindingFuture = Http().bindAndHandle(root, "0.0.0.0", port)
  }
}
