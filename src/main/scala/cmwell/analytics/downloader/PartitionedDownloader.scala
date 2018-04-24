package cmwell.analytics.downloader

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpMethods.POST
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpRequest}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.util.ByteString
import cmwell.analytics.data.{DataWriter, ObjectExtractor}
import cmwell.analytics.util.{EsTopology, EsUtil, HttpUtil, Shard}
import com.typesafe.config.ConfigFactory
import org.apache.avro.generic.GenericRecord
import org.apache.log4j.LogManager

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration.{Duration, FiniteDuration, MILLISECONDS}
import scala.concurrent.{Await, ExecutionContextExecutor, Promise}
import scala.util.{Failure, Random, Success}

object PartitionedDownloader {

  private val logger = LogManager.getLogger(PartitionedDownloader.getClass)

  private val config = ConfigFactory.load
  private val maxContentLength = config.getMemorySize("akka.http.client.parsing.max-content-length").toBytes
  private val maxFetchSize = config.getInt("extract-index-from-es.maximum-fetch-size")
  // FIXME: This readTimeout is not currently being used
  private val readTimeout = FiniteDuration(config.getDuration("extract-index-from-es.read-timeout").toMillis, MILLISECONDS)
  private val maxReadAttempts = config.getInt("extract-index-from-es.max-read-attempts")

  /** Create a Source that produces a stream of objects from a given shard. */
  private def infotonsFromShard[T <: GenericRecord](shard: Shard,
                                                    httpAddress: String,
                                                    currentOnly: Boolean,
                                                    extractor: ObjectExtractor[T],
                                                    format: String = "parquet")
                                                   (implicit system: ActorSystem,
                                                    executionContext: ExecutionContextExecutor,
                                                    actorMaterializer: ActorMaterializer): Source[T, NotUsed] = {

    // This will be fetching from a single shard at a time, so there is no multiplier by the number of shards.
    // Calculate a fetch size that will fit within the maximum-content-length.
    val fetchSize = 1 max (maxContentLength / extractor.infotonSize / 1.1).toInt min maxFetchSize // 10% margin of error

    case class ScrollState(scrollId: Option[String] = None,
                           fetched: Long = 0,
                           totalCount: Long = EsUtil.countDocumentsInShard(httpAddress, shard, extractor.filter(currentOnly))) {

      def isInitialRequest: Boolean = scrollId.isEmpty

      def request: HttpRequest = scrollId.fold(initialRequest)(subsequentRequest)

      val query = s"{${extractor.filter(currentOnly)},${extractor.includeFields}}"

      private def initialRequest = HttpRequest(
        method = POST,
        uri = s"http://$httpAddress/${shard.indexName}/_search" +
          s"?scroll=1m" +
          s"&search_type=scan" +
          s"&size=$fetchSize" +
          s"&preference=_shards:${shard.shard}",
        entity = ByteString(query))

      private def subsequentRequest(scrollId: String) = HttpRequest(
        method = POST,
        uri = s"http://$httpAddress/_search/scroll?scroll=1m",
        entity = HttpEntity.Strict(ContentTypes.`text/plain(UTF-8)`, ByteString(scrollId)))
    }

    Source.unfoldAsync(ScrollState()) { scrollState: ScrollState =>

      if (scrollState.isInitialRequest)
        logger.info(s"Requesting initial _scroll_id for index:${shard.indexName}, shard:${shard.shard} from:$httpAddress.")
      else
        logger.info(s"Requesting next $fetchSize uuids for index:${shard.indexName}, shard:${shard.shard} from:$httpAddress.")

      HttpUtil.jsonResultAsync(scrollState.request, "fetch next block").map { json =>

        val nextScrollId = json.findValue("_scroll_id").asText

        val objects = json.findValue("hits").findValue("hits").iterator.asScala.map(extractor.extractFromJson).toVector
        val objectsFetched = objects.length

        if (scrollState.isInitialRequest) {
          assert(objects.isEmpty)
          logger.info(s"Received initial _scroll_id.")
        } else {
          logger.info(s"Received $objectsFetched infotons [${scrollState.fetched + objectsFetched}/${scrollState.totalCount}].")
        }

        if (objects.isEmpty && !scrollState.isInitialRequest) {
          None
        } else {
          val nextScrollState = scrollState.copy(
            scrollId = Some(nextScrollId),
            fetched = scrollState.fetched + objectsFetched)

          Some(nextScrollState -> objects)
        }
      }
    }
      .map(Source(_)).flatMapConcat(identity)
  }


  /** Get a stream of records from an ES index, and pump them into a data writer sink.
    * Each shard from the source is processed in parallel.
    */
  def runDownload[T <: GenericRecord](esTopology: EsTopology,
                                      parallelism: Int,
                                      currentOnly: Boolean,
                                      objectExtractor: ObjectExtractor[T],
                                      dataWriterFactory: Shard => DataWriter[T])
                                     (implicit system: ActorSystem,
                                      executionContext: ExecutionContextExecutor,
                                      actorMaterializer: ActorMaterializer): Unit = {
    // esTopology will only contain shards that are to be read.
    val shardsToDownload = esTopology.shards.keys.toSeq

    // If we were very clever, we would ensure that we only sent one request to a node at a time.
    // Instead, we use a simpler approach and just select shards randomly, and it should be fairly even.
    val shards = mutable.Queue(Random.shuffle(shardsToDownload): _*)
    var inFlight = 0

    val isDone = Promise[Boolean]()

    def launchShard(shard: Shard, attempt: Int = 0): Unit = {
      val nodesHostingShard = esTopology.shards(shard)
      // pick one node randomly
      val node = nodesHostingShard(Random.nextInt(nodesHostingShard.length))
      val address = esTopology.nodes(node)

      val writer: DataWriter[T] = dataWriterFactory(shard)

      infotonsFromShard[T](
        shard = shard,
        httpAddress = address,
        currentOnly = currentOnly,
        extractor = objectExtractor)
        // An exception thrown in the source needs to be handled here.
        // It will not be propagated down to the completion of the entire stream.
        .watchTermination() { (_, done) =>
        done.onComplete {
          case Failure(ex) =>
            shards.synchronized {
              if (attempt < maxReadAttempts) {
                // This retry is launched immediately, so no backing off.
                // However, it could be re-launched against a different node, so it has a chance of succeeding.
                logger.warn(s"Failed download for index:${shard.indexName} shard:${shard.shard}. Retry attempt ${attempt + 1}", ex)
                inFlight = inFlight + 1
                launchShard(shard, attempt = attempt + 1)
              }
              else {
                logger.warn(s"Failed download for index:${shard.indexName} shard:${shard.shard}. Giving up!", ex)
                isDone.tryFailure(ex)
              }
            }
          case Success(_) => // Do nothing
        }
      }
        // TODO: Since the writer will be blocking, should it be dispatched on a separated (bounded) thread pool?
        .toMat(Sink.foreach((infoton: T) => writer.write(infoton)))(Keep.right)
        .run().onComplete {
        case Success(_) =>
          writer.close()

          shards.synchronized {
            inFlight = inFlight - 1

            if (shards.nonEmpty) {
              val nextShard = shards.dequeue()
              inFlight = inFlight + 1
              launchShard(nextShard)
            }
            else if (inFlight == 0) {
              if (isDone.trySuccess(true)) // Failure might have been signaled upstream
                logger.info(s"Completed download for index:${shard.indexName} shard:${shard.shard}.")
            }
            else {
              // There are downloads still running - let them finish
            }
          }

        case Failure(ex) =>
          shards.synchronized {
            inFlight = inFlight - 1

            if (attempt == maxReadAttempts) {
              logger.error(s"Failed download for index:${shard.indexName} shard:${shard.shard}. Retries exceeded $maxReadAttempts - giving up.", ex)
              isDone.tryFailure(ex)
            }
            else {
              logger.warn(s"Failed download for index:${shard.indexName} shard:${shard.shard}. Retry attempt ${attempt + 1}", ex)
              inFlight = inFlight + 1
              launchShard(shard, attempt = attempt + 1)
            }
          }
      }
    }

    // Launch the first batch
    if (shards.nonEmpty) {

      shards.synchronized {
        (1 to (shards.length min parallelism)).foreach { _ =>
          val shard = shards.dequeue()
          inFlight = inFlight + 1
          launchShard(shard)
        }
      }

      if (Await.result(isDone.future, Duration.Inf)) {
        logger.info("Completed successfully.")
      }
    }
    else {
      logger.info("No shards to download.")
    }
  }
}

