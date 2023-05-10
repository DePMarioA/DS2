package mapreduce

import akka.actor.SupervisorStrategy.Resume
import akka.actor.{Actor, ActorSelection, Address, OneForOneStrategy}

import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.duration._



//import akka.actor.PoisonPill

import akka.actor.{RelativeActorPath, RootActorPath}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, MemberStatus}


class MapReduceClient(servicePath:String) extends Actor {
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 2, withinTimeRange = 1.minute) {
      case _: Exception                => Resume
    }
  val cluster = Cluster(context.system)
  val servicePathElements = servicePath match {
    case RelativeActorPath(elements) => elements
    case _ => throw new IllegalArgumentException(
      "servicePath [%s] is not a valid relative actor path" format servicePath)
  }
  import context.dispatcher
//  val tickTask = context.system.scheduler.scheduleWithFixedDelay(2.seconds, 2.seconds, self, "tick")
val tickTask = context.system.scheduler.scheduleOnce(7.seconds, self, "tick")

  var nodes = Set.empty[Address]
  var   service2 : ActorSelection = null

  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberEvent], classOf[ReachabilityEvent])
  }
  override def postStop(): Unit = {
    cluster.unsubscribe(self)
//    tickTask.cancel()
  }


  def receive = {
    case "tick" if nodes.nonEmpty =>
      // just pick any one
      val address = nodes.toIndexedSeq(ThreadLocalRandom.current.nextInt(nodes.size))
      val service = context.actorSelection(RootActorPath(address) / servicePathElements)
      service2=service
      service! nodesize(nodes.size)
      service !  Book("A Tale of Two Cities", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg98.txt")
      service ! Book("The Pickwick Papers", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg580.txt")
      service ! Book("A Child's History of England", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg699.txt")
      service ! Book("ERROR", "https://google.com/dsfks")
        service ! Book("ERROR", "https://google.com/dsfks1")
        service ! Book("ERROR", "https://google.com/dsfks2")
      service ! Book("ERROR", "https://google.com/dsfks5")
      service ! Book("ERROR", "https://google.com/dsfks11")
      service ! Book("ERROR", "https://google.com/dsfks21")

      service ! Book("The Old Curiosity Shop", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg700.txt")
      service ! Book("Oliver Twist", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg730.txt")
      service ! Book("David Copperfield", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg766.txt")
      service ! Book("Hunted Down", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg807.txt")
      service ! Book("Dombey and Son", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg821.txt")
      service ! Book("Our Mutual Friend", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg883.txt")
      service ! Book("Little Dorrit", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg963.txt")
      service ! Book("ERROR", "https://google.com/dsfks0")
      service ! Book("ERROR", "https://google.com/dsfks12")
      service ! Book("ERROR", "https://google.com/dsfks22")
      service ! Book("Life And Adventures Of Martin Chuzzlewit", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg967.txt")
      service ! Book("The Life And Adventures Of Nicholas Nickleby", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg968.txt")
      service ! Book("Bleak House", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg1023.txt")
      service ! Book("Great Expectations", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg1400.txt")
      service ! Book("A Christmas Carol", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg19337.txt")
      service ! Book("The Cricket on the Hearth", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg20795.txt")
      service!Website("https://apnews.com/hub/us-news?utm_source=apnewsnav&utm_medium=navigation")
      service ! Book("ERROR", "https://google.com/dsfks3")
      service ! Book("ERROR", "https://google.com/dsfks13")
      service!Website("https://apnews.com/hub/world-news?utm_source=apnewsnav&utm_medium=navigation")
      service!Website("https://apnews.com/hub/politics?utm_source=apnewsnav&utm_medium=navigation")
      service!Website("https://apnews.com/hub/sports?utm_source=apnewsnav&utm_medium=navigation")
      service ! Book("ERROR", "https://google.com/dsfks23")
      service!File("Example.txt","src/main/resources/Example.txt")
//      Thread sleep 5000
      println("\n\n FLUSHING \n\n\n")
      service ! Flush
    case result: ResultsWord =>
      println(result)
      println(s"Size of list: ${result.results.size}")
    case result: ResultsURL =>
      println(result)
      println(s"Size of URLlist: ${result.results.size}")
    case ResultsCount(s)=>
      println(s)
      println(s.size)
    case failed: JobFailed =>
      println(failed)
    case "end"=>
      Thread.sleep(2000)
      context.system.terminate()
    case state: CurrentClusterState =>
      nodes = state.members.collect {
        case m if m.hasRole("mapping") && m.status == MemberStatus.Up => m.address
      }
    case MemberUp(m) if m.hasRole("mapping")        => nodes += m.address
    case other: MemberEvent                         => nodes -= other.member.address
    case UnreachableMember(m)                       => nodes -= m.address
    case ReachableMember(m) if m.hasRole("mapping") => nodes += m.address
  }
}