package com.gravitydev.traction
package amazonswf

import akka.actor._
import akka.agent.Agent
import com.amazonaws.{AbortedException, AmazonClientException}

import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.concurrent.duration._

trait ConstantAsyncListener extends Actor with ActorLogging {

  val emptyMessage = ()

  implicit val system = context.system

  import system.dispatcher

  val retriesSoFar = Agent(0)

  def listen: Future[Unit]

  // auto start
  override def preStart() = self ! emptyMessage

  def receive = {
    case _ =>
      log.debug(s"[$this] will listen.")
      listen map { _ =>
        // keep going
        self ! emptyMessage
      } recover {
        handleListenerException
      }
  }

  /**
   * Handles an exception that is produced by the SWF async polling tasks.
   */
  def handleListenerException: PartialFunction[Throwable, Unit] = {
    case e: AbortedException => {
      // An aborted exception will be received if the SWF client is shutdown.
      log.debug(s"Listener [$this] closing due to SWF client shutdown.")
    }
    case e: AmazonClientException if e.getMessage == "sleep interrupted" => {
      // A client exception with a 'sleep interrupted' message will be received if the SWF client is shutdown.
      log.debug(s"Listener [$this] closing due to SWF client shutdown.")
    }
    case NonFatal(e) => {
      log.error(e, s"Error while listening for [$this]. Retries so far [${retriesSoFar.get()}].")

      // update retries
      retriesSoFar send (_ + 1)

      // retry
      system.scheduler.scheduleOnce(math.min(retriesSoFar(), 40).second, self, ())
    }
  }

}

