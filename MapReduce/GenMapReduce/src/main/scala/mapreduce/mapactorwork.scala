package mapreduce

import akka.actor.SupervisorStrategy.Resume
import akka.actor.{Actor, OneForOneStrategy}
import org.jsoup.Jsoup
import scala.collection.mutable.HashSet
import scala.concurrent.duration.DurationInt
import scala.io.Source


class mapactorwork extends Actor {
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 2, withinTimeRange = 1.minute) {
      case _: Exception                => Resume
    }

  println(s"MapWorker: $self")
  val STOP_WORDS_LIST = List("a", "am", "an", "and", "are", "as", "at", "be",
    "do", "go", "if", "in", "is", "it", "of", "on", "the", "to")
  def receive = {
    case Book(title, url) =>
      process(title, url)
    case Website(url)=>
      process2(url)
    case File(filename,content)=>
      process3(filename,content)
    case Flush =>
      sender() ! "done"

    case ""=> throw new IllegalArgumentException("URL ISN'T WORKING/VALID")

  }

  def process3(filename:String,filepath:String ): Unit = {
    val content = Source.fromFile(filepath).mkString
    for (word <- content.toLowerCase.split("[\\p{Punct}\\s]+"))
      if ((!STOP_WORDS_LIST.contains(word))) {
        sender() ! Count(word, 1)
      }
  }


  def process2(url: String): Unit = {
    val content = getContent(url)
    var urlsFound = HashSet[String]()
    val baseurl = url.substring(0, url.indexOf("/", 8))
    var yy = List[String]()
    Jsoup.parse(content).select("a").forEach(cc => yy = cc.attr("href") :: yy)
    var urlroot = ""
    for (urls <- yy) {
      urlroot = urls
      if (!urls.contains("http")) {
        urlroot = baseurl + urls
      }
      if (!urlsFound.contains(urlroot)) {
        sender() ! Link(url, urlroot)
        urlsFound += urlroot
      }
    }
  }

  // Process book
  def process(title: String, url: String) = {
    val content = getContent(url)
    var namesFound = HashSet[String]()
    var z = collection.immutable.Set[String]()
    /** collects the text and splits them into word, made it into a set so no words are repeated.**/
    z =  Source.fromURL(url).mkString.split("\\W+").toSet
    //    println(content._1,z.size)
    /** for each word check if its capitalized or not in word list or number**/
    z.foreach(c=>if((!STOP_WORDS_LIST.contains(c)) && c.equals(c.capitalize)&& !c.forall(Character.isDigit) && !c.equals(c.toUpperCase())) {
      sender() !  Word(c, title)
    })

  }

  // Get the content at the given URL and return it as a string
  def getContent( url: String ) = {
    try {
      Source.fromURL(url).mkString
    } catch {     // If failure, just return an empty string
      case e: Exception => ""
    }
  }
}
