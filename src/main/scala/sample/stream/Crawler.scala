package sample.stream

import java.io.{FileOutputStream, PrintWriter}

import akka.actor.ActorSystem
import akka.stream.FlowMaterializer
import akka.stream.actor.ActorPublisher
import akka.stream.impl.ExposedPublisher
import akka.stream.javadsl.FlexiRoute
import akka.stream.scaladsl._
import org.reactivestreams.{Processor, Subscriber, Publisher}
import sun.java2d.pipe.DuctusRenderer
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Future

object Crawler {

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("Sys")
    import system.dispatcher

    implicit val materializer = FlowMaterializer()

    val externalSource: Source[Node] = Source(List(Node(1)))

    val responses = ForeachSink[Node] { prime =>
      println("response: " + prime)
    }

    FlowGraph { implicit builder =>
      import FlowGraphImplicits._
      builder.allowCycles()
      val broadcast = Broadcast[Node]
      val newNodesBroadcast = Broadcast[List[Node]]

      val mergedSources = Merge[Node]


      externalSource ~> mergedSources
      broadcast ~> Flow[Node].mapAsync(next) ~> newNodesBroadcast

      newNodesBroadcast ~> Flow[List[Node]].mapConcat(identity) ~> mergedSources ~> broadcast
      broadcast ~> responses

      @volatile var sum: Integer = 0
      newNodesBroadcast ~> ForeachSink[List[Node]] { list =>
        sum += list.size - 1
        println("Nodes being processed: " + sum)
        if (sum == 0) {
          println("Shutting down")
          system.shutdown()
        }
      }

    }.run()

  }

  def next(node: Node): Future[List[Node]] = Future {
    Thread.sleep(50)
    println ("Next of " + node)
    if (node.value < 250) {
      List(Node(node.value * 2), Node(node.value * 2 + 1))
    } else {
      List()
    }
  }
}

case class Node(value: Integer)
