package mapreduce

import akka.actor.SupervisorStrategy.{Restart, Resume}
import akka.actor.{Actor, OneForOneStrategy}

import scala.collection.mutable.HashMap
import scala.concurrent.duration.DurationInt

class reduceractor extends Actor{

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minute) {
      case _: Exception =>
        println(s"This is the error: from: $self")
        Restart
    }
  var remainingMappers = 1
  var counter=0
  println(s"reduceWorker: $self")
  //  var remainingMappers = ConfigFactory.load.getInt("")
/**Keeps track of the words coming in*/
  var reduceMap = HashMap[String, List[String]]()
  var reduceMapHTML = HashMap[String, List[String]]()
  var reduceMapCount = HashMap[String, Int]()


  def receive = {

    /**Same logic as previous assignments*/
    case Word(word, title) =>
      counter+=1
      if (reduceMap.contains(word)) {
        if (!reduceMap(word).contains(title)) {
          reduceMap += (word -> (title :: reduceMap(word)))
        }
      }
      else {
        reduceMap += (word -> List(title))
      }
    case Link(link,urls)=>
      if (reduceMapHTML.contains(link)) {
        if (!reduceMapHTML(link).contains(urls))
          reduceMapHTML += (link -> (urls :: reduceMapHTML(link)))
      }
      else
        reduceMapHTML += (link -> List(urls))

    case Count(word,count) =>
      if (reduceMapCount.contains(word)) {
        reduceMapCount += (word -> (reduceMapCount(word) + 1))
      }
      else
        reduceMapCount += (word -> 1)


    /**Same logic as previous assignments, adjusted for each type, and when its finish then it will return "add" to notify its done*/
    case Flush =>

      remainingMappers-=1

      if(remainingMappers == 0) {

        if(reduceMap.nonEmpty){
          println(reduceMap)
          println("Size of :"+reduceMap.size)

        }

        if(reduceMapHTML.nonEmpty){
          println(reduceMapHTML)
          println(s"Total url searched: ${reduceMapHTML.size}")

        }

        if(reduceMapCount.nonEmpty){
          println(reduceMapCount)
          println(s"Total words for Count size: ${reduceMapCount.size}")

        }
        sender() ! ResultsWord(reduceMap)
        sender() ! ResultsURL(reduceMapHTML)
        sender() ! ResultsCount(reduceMapCount)
        sender() ! "add"


      }
  }

  def printmap(): Unit ={

    reduceMapHTML.clear()
    reduceMapCount.clear()
    reduceMap.clear()

  }

}
