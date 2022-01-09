
import io.circe._
import io.circe.parser._
import slick.jdbc.PostgresProfile.api._
import slick.jdbc.meta.MTable
import java.sql
import java.text.SimpleDateFormat
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.{Duration, DurationInt}
import scala.io.Source
import org.apache.commons.cli._
import com.typesafe.scalalogging.Logger

import scala.concurrent.ExecutionContext.Implicits.global

object Wiki {

  private val NAMETABLEARTICLES = "ArticleTable"
  private val NAMETABLECATEGORY = "CategoryTable"

  private val DATEFORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"

  private val CREATETIMESTAMP = "create_timestamp"
  private val TIMESTAMP = "timestamp"
  private val LANGUAGE = "language"
  private val WIKI = "wiki"
  private val CATEGORY = "category"
  private val TITLE = "title"
  private val AUXILIARYTEXT = "auxiliary_text"

  private val FLAGTOCREATEOPTION = "c"
  private val TITLENAMEOPTION = "t"

  val db = Database.forConfig("postgres")
  val wikiTable = TableQuery[WikiTable]

  val wikiData: Iterator[String] = Source.fromResource("ruwikiquote-20211220-cirrussearch-general.json").getLines

  val logger = Logger("Wiki service")
  val options = new Options

  options.addOption("", false, "usage scenario: \n" +
    "1. Creating tables in postgresql and filling in data.\n" +
    "Required parameters: > flag 'C'.\n" +
    "2. Search for articles in the database by name. \n" +
    "Required parameters: > title of the article.\n"
  )
  options.addOption(FLAGTOCREATEOPTION, false, "use this parameter if you want to create a table and fill it with data from the wikipedia dump file")
  options.addOption(TITLENAMEOPTION, true, "title of the article")


  val dataIterator: Iterator[String] =
    wikiData.filter(
      x =>
        x.contains(CREATETIMESTAMP) &&
          x.contains(TIMESTAMP) &&
          x.contains(LANGUAGE) &&
          x.contains(WIKI) &&
          x.contains(CATEGORY) &&
          x.contains(TITLE) &&
          x.contains(AUXILIARYTEXT)
    )

  case class Article(
                      create_timestamp: java.sql.Date,
                      timestamp: java.sql.Date,
                      language: String,
                      wiki: String,
                      category: String,
                      title: String,
                      auxiliary_text: String)

  case class Category(
                       category: String
                     )

  class CategoryTable(tag: Tag) extends Table[String](tag, Some("public"), NAMETABLECATEGORY) {
    def category = column[String](CATEGORY)

    def * = (category)
  }

  val categoryTable = TableQuery[CategoryTable]


  class WikiTable(tag: Tag) extends Table[(java.sql.Date, java.sql.Date, String, String, String, String, String)](tag, Some("public"), NAMETABLEARTICLES) {

    def create_timestamp = column[java.sql.Date](CREATETIMESTAMP)

    def timestamp = column[java.sql.Date](TIMESTAMP)

    def language = column[String](LANGUAGE)

    def category = column[String](CATEGORY)

    def wiki = column[String](WIKI)

    def title = column[String](TITLE)

    def auxiliary_text = column[String](AUXILIARYTEXT)

    override def * = (create_timestamp, timestamp, language, category, wiki, title, auxiliary_text) //<> (Player.tupled, Player.unapply)

 }


  def checkDB(name: String): Boolean = {
    val tables = Await.result(db.run(MTable.getTables), Duration.Inf).toList
    tables.exists(_.name.name == name)
  }

  def createDB: Unit = {
    val setup = (categoryTable.schema ++ wikiTable.schema).create
    Await.result(db.run(setup), Duration.Inf)
  }

  def addToDB(map: Map[String, String]): Unit = {
    val setup = wikiTable += (
      convertToDate(map(CREATETIMESTAMP)),
      convertToDate(map(TIMESTAMP)),
      map(LANGUAGE),
      map(CATEGORY),
      map(WIKI),
      map(TITLE),
      map(AUXILIARYTEXT)
    )

    Await.result(db.run(setup), Duration.Inf)
  }

  def addToCategoryTable(newCategory: String): Unit = {

    val transaction =
      categoryTable.filter(_.category === newCategory.trim).exists.result.flatMap { exists =>
        if (!exists) {
          categoryTable += newCategory.trim
        } else {
          DBIO.successful(None)
        }
      }.transactionally

    Await.result(db.run(transaction), Duration.Inf)
  }

  def splitCategoryString(map: Map[String, String]): List[String] = {
    map(CATEGORY).toString.split(',').toList
  }

  def addCategoryList(list: List[String]): Unit = {
    list match {
      case List(x) => addToCategoryTable(x)
      case x :: xs =>
        addToCategoryTable(x)
        addCategoryList(xs)
    }
  }

  def dropArticleDB: Unit = Await.result(db.run(wikiTable.schema.drop), Duration.Inf)

  def dropCategoryDB: Unit = Await.result(db.run(categoryTable.schema.drop), Duration.Inf)

  def convertToDate(dateString: String): java.sql.Date = {
    val formatter = new SimpleDateFormat(DATEFORMAT)
    //formatter.setTimeZone(TimeZone.getTimeZone("UTC"))
    new sql.Date(formatter.parse(dateString).getTime)
  }

  def takingValueFromJson(json: Json, key: String): String =
    json.\\(key).head.toString
      .replaceAll("[\"\\[\\]\n]", "")
      .trim

  def parseArticle(dataString: String): Map[String, String] = {

    val parseResult: Either[ParsingFailure, Json] = parse(dataString)

    parseResult match {
      case Left(parsingError) =>
        throw new IllegalArgumentException(s"Invalid JSON object: ${parsingError.message}")
      case Right(json) =>
        Map(
          CREATETIMESTAMP -> takingValueFromJson(json, CREATETIMESTAMP),
          TIMESTAMP -> takingValueFromJson(json, TIMESTAMP),
          LANGUAGE -> takingValueFromJson(json, LANGUAGE),
          WIKI -> takingValueFromJson(json, WIKI),
          CATEGORY -> takingValueFromJson(json, CATEGORY),
          TITLE -> takingValueFromJson(json, TITLE),
          AUXILIARYTEXT -> takingValueFromJson(json, AUXILIARYTEXT)
        )
    }
  }

  def main(args: Array[String]): Unit = {

    val parser = new DefaultParser

    val cmd = parser.parse(options, args)
    cmd match {
      case x if x.hasOption(FLAGTOCREATEOPTION) =>
        if (checkDB(NAMETABLEARTICLES)) dropArticleDB
        if (checkDB(NAMETABLECATEGORY)) dropCategoryDB
        createDB
        for (elem <- dataIterator) {
          addCategoryList(splitCategoryString(parseArticle(elem)))
          addToDB(parseArticle(elem))
        }
        db.close()

      case x if x.hasOption(TITLENAMEOPTION) =>
        scala.Option(cmd.getOptionValue(TITLENAMEOPTION)) match {
          case Some(name) =>

            Await.result(db.run(wikiTable.filter(_.title.toLowerCase === name.toLowerCase).result).map(_.foreach {
              case (create_timestamp, timestamp, language, category, wiki, title, auxiliary_text) =>

                println(s"FOUND ARTICLE")
                println(s"create-timestamp: ${create_timestamp.getTime}")
                println(s"timestamp: ${timestamp.getTime}")
                println(s"language: ${language}")
                println(s"category: ${category}")
                println(s"wiki: ${wiki}")
                println(s"title: ${title}")
                println(s"auxiliary-text: ${auxiliary_text}")
            }), Duration.Inf)
            db.close()

          case None => System.out.println("Invalid title of the article")
        }

      case _ =>
        val formatter = new HelpFormatter
        formatter.printHelp("Wiki service", options).toString
    }
  }
}