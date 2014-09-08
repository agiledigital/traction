package com.gravitydev.traction
package amazonswf

import java.util.concurrent.TimeUnit

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowAsyncClient
import com.amazonaws.services.simpleworkflow.model._
import com.gravitydev.awsutil.awsToScala

import scala.concurrent.duration.{Duration}
import scala.language.postfixOps

class ActivityWorker[C, T, A <: Activity[C, T]](
                                                 domain: String, swf: AmazonSimpleWorkflowAsyncClient, meta: SwfActivityMeta[T, A], context: C
                                                 ) extends ConstantAsyncListener {

  import system.dispatcher

  def listen = {
    log.debug(s"[${meta.identity}] listening for activities tasks on [${meta.defaultTaskList}].")

    val pollForActivityTaskRequest = new PollForActivityTaskRequest()
      .withDomain(domain)
      .withTaskList(new TaskList().withName(meta.defaultTaskList))
      .withIdentity(meta.identity)

    awsToScala(swf.pollForActivityTaskAsync)(pollForActivityTaskRequest).map { task =>
      // if there is a task
      Option(task.getTaskToken) match {
        case Some(token) => {
          //val activity = meta.format.reads(Json.parse((task.getInput span (_!=':') _2) drop 1)) get;
          val activity: A = meta.parseActivity(task.getInput)

          log.info(s"[${meta.identity}] executing activity [${activity}]...")

          val heartbeat = meta.defaultTaskHeartbeatTimeout match {
            case x if x > 0 => {
              val timeout: Int = Math.max(x / 10, 5)
              log.debug(s"Will send heartbeat every [$timeout] seconds. Timeout is set to [$x].")

              val cancellable = system.scheduler.schedule(Duration(1L, TimeUnit.SECONDS), Duration(timeout, TimeUnit.SECONDS)) {
                log.debug(s"Sending heart beat for activity")
                // This may fail if the activity has been cancelled while the task is running.
                // As an extension we could cancel the underlying task.
                swf.recordActivityTaskHeartbeatAsync(new RecordActivityTaskHeartbeatRequest().withTaskToken(token))
              }

              Some(cancellable)
            }
            case _ => None
          }

          try {

            val result: T = activity(context)

            log.info(s"[${meta.identity}] executed activity [${activity}].")

            swf.respondActivityTaskCompletedAsync {
              new RespondActivityTaskCompletedRequest()
                .withTaskToken(token)
                .withResult(meta.serializeResult(result))
            }
          } catch {
            case e: Throwable => {
              log.error(e, s"[${meta.identity}] failed to execute activity [${activity}].")
              swf.respondActivityTaskFailedAsync(
                new RespondActivityTaskFailedRequest()
                  .withTaskToken(token)
                  .withReason("Exception thrown")
                  .withDetails(e.getMessage() + "\n" + e.getStackTrace)
              )
            }
          }
          finally {
            log.debug(s"Shutting down heartbeat [$heartbeat] for task token [$token].")
            heartbeat.foreach(_.cancel())
          }
        }
        case None => {
          // This will happen when the long poll for new work times out before work is received - normal.
          log.debug(s"[${meta.identity}] timed out waiting for activity work on [${meta.name}], will poll again.")
        }
      }
      ()
    }
  }

  override def toString: String = s"ActivityWorker($domain, $meta)"
}
