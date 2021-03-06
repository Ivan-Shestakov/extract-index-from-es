package cmwell.analytics.main

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cmwell.analytics.data.{DataWriterFactory, IndexWithCompleteDocument}
import cmwell.analytics.downloader.PartitionedDownloader
import cmwell.analytics.util.{DiscoverEsTopology, FindContactPoints}
import org.apache.commons.io.FileUtils
import org.apache.log4j.LogManager
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.concurrent.ExecutionContextExecutor

object DumpCompleteDocumentFromEs {

  def main(args: Array[String]): Unit = {

    val logger = LogManager.getLogger(DumpCompleteDocumentFromEs.getClass)

    // Since we expect this to be run on a CM-Well node, the default parallelism is to use half the processors
    // so as to avoid starving the CM-Well node from processor resources. A higher level of parallelism might
    // be possible (without interfering with CM-Well) since most of the work will actually be on the ES side.
    val defaultParallelism = 1 max (Runtime.getRuntime.availableProcessors / 2)

    implicit val system: ActorSystem = ActorSystem("dump-complete-document-from-es")
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher
    implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()

    try {

      object Opts extends ScallopConf(args) {

        val readIndex: ScallopOption[String] = opt[String]("read-index", short = 'i', descr = "The name of the index to read from (default: cm_well_all)", required = false)
        val parallelism: ScallopOption[Int] = opt[Int]("parallelism", short = 'p', descr = "The parallelism level", default = Some(defaultParallelism))
        val currentOnly: ScallopOption[Boolean] = opt[Boolean]("current-only", short = 'c', descr = "Only download current infotons", default = Some(false))
        val out: ScallopOption[String] = opt[String]("out", short = 'o', descr = "The path to save the output to", required = true)
        val format: ScallopOption[String] = opt[String]("format", short = 'f', descr = "The data format: either 'parquet' or 'csv'", default = Some("parquet"))
        val url: ScallopOption[String] = trailArg[String]("url", descr = "A CM-Well URL", required = true)

        verify()
      }

      val esContactPoint = FindContactPoints.es(Opts.url())
      val indexesOrAliasesToRead = Opts.readIndex.toOption.fold(Seq("cm_well_all"))(Seq(_))
      val esTopology = DiscoverEsTopology(esContactPoint = esContactPoint, aliases = indexesOrAliasesToRead)

      // Calling script should clear output directory as necessary.

      val objectExtractor = IndexWithCompleteDocument
      val dataWriterFactory = DataWriterFactory.file(format = Opts.format(), objectExtractor, outDirectory = Opts.out())

      PartitionedDownloader.runDownload(
        esTopology = esTopology,
        parallelism = Opts.parallelism(),
        currentOnly = Opts.currentOnly(),
        objectExtractor = objectExtractor,
        dataWriterFactory = dataWriterFactory)

      // The Hadoop convention is to touch the (empty) _SUCCESS file to signal successful completion.
      FileUtils.touch(Paths.get(Opts.out(), "_SUCCESS").toFile)
    }
    catch {
      case ex: Throwable =>
        logger.error(ex.getMessage, ex)
        System.exit(1)
    }
    finally {
      system.terminate()
    }
  }
}
