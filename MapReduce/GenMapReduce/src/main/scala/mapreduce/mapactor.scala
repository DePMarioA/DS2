package mapreduce

import akka.actor.SupervisorStrategy.Resume
import akka.actor.{Actor, ActorRef, OneForOneStrategy, PoisonPill, ReceiveTimeout, Terminated}
import akka.routing.ConsistentHashingRouter.{ConsistentHashMapping, ConsistentHashableEnvelope}
import akka.routing.{Broadcast, FromConfig}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.io.Source
//import akka.actor.PoisonPill
import akka.actor.Props



class mapactor extends Actor { // mapactor The service not the worker
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 0, withinTimeRange = 1.seconds) {
      case _: Exception                => Resume
    }
  var ss =collection.mutable.Set[String]()
  val MapActorWorker = context.actorOf(FromConfig.props(Props[mapactorwork]()),
    name = "workerRouter")
  val MapReduceWorker = context.actorOf(FromConfig.props(Props[reduceractor]()),
    name = "workerRouter2")
  var aggregator = context.actorOf(Props(
    classOf[MapAggregator], MapReduceWorker,self,1))
  println(s"Starting aggregator $aggregator from $self")
  var death =self
  var count=0

  def hashMapping: ConsistentHashMapping ={
    case Word(word,title)=> word
  }
  val STOP_WORDS_LIST = List("a", "am", "an", "and", "are", "as", "at", "be",
    "do", "go", "if", "in", "is", "it", "of", "on", "the", "to")

  def receive = {
    case nodesize(int) =>
      aggregator = context.actorOf(Props(
        classOf[MapAggregator], MapReduceWorker,sender(),int))
      println(s"I am aggregator ${aggregator.path}")
      death = context.watch(aggregator)
    case Flush=>
      MapActorWorker.tell(
        Broadcast(Flush), aggregator)
    case Book(title, url) =>
      println((title,url))
      MapActorWorker.tell(
        ConsistentHashableEnvelope(message = Book(title, url), hashKey = title), aggregator)
    case Website(url)=>
      println(url)
      MapActorWorker.tell(
        ConsistentHashableEnvelope(message = Website(url), hashKey = url), aggregator)
    case File(filename,content)=>
      println(content)
      MapActorWorker.tell(
        ConsistentHashableEnvelope(message = File(filename,content), hashKey = filename), aggregator)
    case Terminated(death)=>
      if(death==aggregator)
      context.system.terminate()

  }



}
/**Aggregates the mappers results. Size of nodes are to ensure how many reply it should receive, main sends final data to the client, replyto is for reducer address*/
class MapAggregator(replyTo: ActorRef, replyToMain :ActorRef,nodesize:Int) extends Actor {
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 0, withinTimeRange = 1.minute) {
      case _: Exception                => Resume
    }
  var deathcounter =0
  var results = 0
  var results2 = 0
  var results3 = 0
  var mapresults =0
  println(s"Starting Aggregator: ${self.path}")



  //aggregates from reducer // this part is just extra its fine if output is in the serverside
  var Rword =mutable.HashMap[String, List[String]]()
  var RLink =mutable.HashMap[String, List[String]]()
  var RCount =mutable.HashMap[String, Int]()
  def receive = {
        /** Per type**/
    case ResultsWord(lst) =>
      Rword.addAll(lst)
    case ResultsURL(lst) =>
      RLink.addAll(lst)
    case ResultsCount(lst) =>
      RCount.addAll(lst)
      /**Forwarding the mappers results*/
    case word: Word=>
      replyTo ! ConsistentHashableEnvelope(message = Word(word.word, word.title), hashKey = word.word)
    case Link(link,urls) =>
//      println(s"These are the links $link")
      replyTo ! ConsistentHashableEnvelope(message = Link(link, urls), hashKey = link)
    case Count(word, int)=>
      replyTo ! ConsistentHashableEnvelope(message = Count(word, int), hashKey = word)

      /**add tells the agregator that all the reducers are done.. then it will poison pill the reducers and stop itself.*/
    case "add"=>
      results+=1
      if((nodesize*3)==results){
        replyToMain ! ResultsWord(Rword)
        replyToMain ! ResultsURL(RLink)
        replyToMain ! ResultsCount(RCount)
        replyTo ! Broadcast(PoisonPill) // since reduce actors are storing values end the process after everything
        replyToMain ! "end"
        context.stop(self)
      }

      /**Notifies when the mapactors are done*/
    case "done" =>

      mapresults+=1
      println(s"This is the map result counter: $mapresults")
      println(s"MapWoker: ${sender()}")
      if(mapresults==(3*nodesize)){

        replyTo ! Broadcast(Flush)
      }
    case ReceiveTimeout =>
      replyTo ! JobFailed("Service unavailable, try again later")
      context.stop(self)
  }
}