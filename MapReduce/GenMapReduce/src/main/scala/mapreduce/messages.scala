package mapreduce

import scala.collection.mutable

trait Messages
case class Book(title: String, url: String) extends Messages
case class Word(word:String, title: String) extends Messages
case class File(filename:String,filepath:String) extends Messages
case class Count(word:String, count :Int ) extends Messages
case class Website(url:String) extends Messages
case class Link(link:String,urls:String) extends Messages
case object Flush extends Messages
case object Done extends Messages
final case class JobFailed(reason: String) extends Messages
case class ResultsWord(results:mutable.HashMap[String, List[String]]) extends Messages
case class ResultsURL(results:mutable.HashMap[String, List[String]]) extends Messages
case class ResultsCount(results:mutable.HashMap[String, Int]) extends Messages
case class nodesize(num:Int)extends Messages

