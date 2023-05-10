package mapreduce

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings}
import com.typesafe.config.ConfigFactory

object MapReduceMaster {
  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      startup(Seq("2551", "2552", "0"))
      MapReduceMasterClient.main(Array.empty)
    } else {
      startup(args.toIndexedSeq)
    }
}
  def startup(ports: Seq[String]): Unit = {
    ports foreach { port =>

      val config =
        ConfigFactory.parseString(s"""
          akka.remote.artery.canonical.port=$port
          """).withFallback(
          ConfigFactory.parseString("akka.cluster.roles = [mapping]")).
          withFallback(ConfigFactory.load("map"))

      val system = ActorSystem("ClusterSystem", config)

      system.actorOf(ClusterSingletonManager.props(
        singletonProps = Props[mapactor](),
        terminationMessage = PoisonPill,
        settings = ClusterSingletonManagerSettings(system).withRole("mapping")),
        name = "mapactor")

      system.actorOf(ClusterSingletonProxy.props(singletonManagerPath = "/user/mapactor",
        settings = ClusterSingletonProxySettings(system).withRole("mapping")),
        name = "mapactorProxy")

    }

  }
}
object MapReduceMasterClient {
  def main(args: Array[String]): Unit = {
    // note that client is not a compute node, role not defined
    val system = ActorSystem("ClusterSystem")
    val  master = system.actorOf(Props(classOf[MapReduceClient], "/user/mapactorProxy"), "client")
//    val tickTask = system.scheduler.scheduleOnce(6.seconds, master, "tick")
//
  }
}