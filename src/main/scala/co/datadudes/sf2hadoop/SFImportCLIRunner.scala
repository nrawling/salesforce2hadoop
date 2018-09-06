package co.datadudes.sf2hadoop

import java.io.File
import java.net.URI
import java.util.Calendar
import co.datadudes.wsdl2avro.WSDL2Avro
import AvroUtils._
import com.typesafe.scalalogging.LazyLogging
// Used for configuration options - file
import com.typesafe.config.ConfigFactory
// ENV variables
import scala.util.Properties

object SFImportCLIRunner extends App with LazyLogging {

  def envOrElseConfig(name: String): String = {
    val config = ConfigFactory.load()

    Properties.envOrElse(
      name.toUpperCase.replaceAll("""\.""", "_"),
      config.getString(name)
    )
  }

case class Config(command: String = envOrElseConfig("s2h.command"),
                    sfUsername: String = envOrElseConfig("s2h.sfUsername"),
                    sfPassword: String = envOrElseConfig("s2h.sfPassword"),
                    datasetBasePath: String = envOrElseConfig("s2h.datasetBasePath"),
                    sfWSDL: File = new File(envOrElseConfig("s2h.sfWSDL")),
                    stateFile: URI = new URI(envOrElseConfig("s2h.stateFile")), //new URI("file://" + System.getProperty("user.home") + "/.sf2hadoop/state"),
                    apiBaseUrl: String = envOrElseConfig("s2h.apiBaseUrl"),
                    apiVersion: String = envOrElseConfig("s2h.apiVersion"),
                    records: Seq[String] = Seq())

  val parser = new scopt.OptionParser[Config]("sf2hadoop") {
    head("sf2hadoop", "1.0")
    cmd("init") optional() action { (_, c) => c.copy(command = "init") } text "Initialize one or more new datasets and do initial full imports"
    cmd("update") optional() action { (_, c) => c.copy(command = "update") } text "(default)Update one or more datasets using incremental imports"
    note("\n")
    opt[String]('u', "username") optional() action { (x, c) => c.copy(sfUsername = x)} text "Salesforce username"
    opt[String]('p', "password") optional() action { (x, c) => c.copy(sfPassword = x)} text "Salesforce password"
    opt[String]('b', "basepath") optional() action { (x, c) => c.copy(datasetBasePath = x)} text "Datasets basepath"
    opt[File]('w', "wsdl") optional() valueName "<file>" action { (x, c) => c.copy(sfWSDL = x)} text "Path to Salesforce Enterprise WSDL"
    opt[URI]('s', "state") optional() valueName "<URI>" action { (x, c) => c.copy(stateFile = x)} text "URI to state file to keep track of last updated timestamps"
    opt[String]('a', "api-base-url") optional() valueName "<URL>" action { (x, c) => c.copy(apiBaseUrl = x)} text "Base URL of Salesforce instance"
    opt[String]('v', "api-version") optional() valueName "<number>" action { (x, c) => c.copy(apiVersion = x)} text "API version of Salesforce instance"
    arg[String]("<record>...") unbounded() action { (x, c) => c.copy(records = c.records :+ x)} text "List of Salesforce record types to import"
    help("help") text "prints this usage text"
  }

  parser.parse(args, Config()) match {
    case Some(config) => handleCommand(config)

    case None => System.exit(1)
  }

  def handleCommand(config: Config) = {
    if(config.command.isEmpty || (config.command.trim != "init" && config.command.trim != "update")) {
      println("You need to enter a valid command (init|update)")
    } else {
      val schemas = WSDL2Avro.convert(config.sfWSDL.getCanonicalPath, filterSFInternalFields)
      val connection = SalesforceService(config.sfUsername, config.sfPassword, config.apiBaseUrl, config.apiVersion)
      val importer = new SFImporter(schemas, config.datasetBasePath, connection)
      val state = new ImportState(config)
      state.createDirs

      val existingStates = state.readStates

      if(config.command == "init") {
        val newStates = config.records.map { recordType =>
          val now = Calendar.getInstance()
          importer.initialImport(recordType)
          recordType -> now
        }.toMap
        val updatedStates = existingStates ++ newStates
        state.saveStates(updatedStates)
      }
      else {
        val newStates = config.records.flatMap { recordType =>
          existingStates.get(recordType) match {
            case Some(previous) => {
              val now = Calendar.getInstance()
              importer.incrementalImport(recordType, previous, now)
              Option(recordType -> now)
            }
            case _ => {
              println(s"$recordType has never been initialized! Skipping...")
              None
            }
          }
        }.toMap
        state.saveStates(newStates)
      }
    }
  }

}
