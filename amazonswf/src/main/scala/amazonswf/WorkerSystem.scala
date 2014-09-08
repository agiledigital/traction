package com.gravitydev.traction
package amazonswf

import akka.actor.{ActorSystem, Props}
import akka.routing.BroadcastPool
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowAsyncClient
import com.amazonaws.services.simpleworkflow.model._
import com.gravitydev.awsutil.awsToScala
import com.gravitydev.traction.amazonswf.WorkerSystem.ExecutionId
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Future
import scala.util.control.Exception


class WorkerSystem(domain: String, swf: AmazonSimpleWorkflowAsyncClient)(implicit system: ActorSystem) extends StrictLogging {

  import system.dispatcher

  /**
   * Runs the specified workflow.
   * @param workflow the workflow to run.
   * @param meta the meta information for the workflow.
   * @tparam T the type of object ultimately returned by the workflow[
   * @tparam W the type of the workflow.
   * @return a Future that will contain either the scheduled Run for this workflow or an error.
   */
  def run[T, W <: Workflow[_, T]](workflow: W with Workflow[_, T])(implicit meta: SwfWorkflowMeta[T, W]): Future[Either[String, (Run, ExecutionId)]] = {
    logger.debug(s"Running workflow [${meta.serializeWorkflow(workflow)}]")

    import scala.collection.JavaConverters._

    val startExecutionRequest = new StartWorkflowExecutionRequest()
      .withDomain(domain)
      .withWorkflowType(new WorkflowType().withName(meta.name).withVersion(meta.version))
      .withWorkflowId(meta.id(workflow))
      .withInput(meta.serializeWorkflow(workflow))
      .withTaskList(new TaskList().withName(meta.taskList))
      .withTagList(meta.tags(workflow).asJavaCollection)

    val futureResponse: Future[Run] = awsToScala(swf.startWorkflowExecutionAsync)(startExecutionRequest)
    futureResponse.map {
      case run => Right(run -> startExecutionRequest.getWorkflowId)
    }.recover {
      case e => {
        logger.error("Error when triggering task", e)
        Left(s"Error running workflow [$e].")
      }
    }
  }

  def startWorkflowWorker[C, T, W <: Workflow[C, T]](meta: SwfWorkflowMeta[T, W], context: C)(instances: Int = 1) = {
    registerWorkflow(meta, logger.debug(_))
    system.actorOf(
      Props(new WorkflowWorker(domain, swf, meta, context)).withRouter(BroadcastPool(instances)),
      name = meta.name + "-workflow"
    )
  }

  // TODO: use macros to allow starting the activity worker with just the A type param
  def startActivityWorker[C, T, A <: Activity[C, T]](meta: SwfActivityMeta[T, A], context: C)(instances: Int = 1) = {
    registerActivity(meta, logger.debug(_))
    system.actorOf(
      Props(new ActivityWorker[C, T, A](domain, swf, meta, context)).withRouter(BroadcastPool(instances)),
      name = meta.name + "-activity"
    )
  }

  def registerWorkflow[W <: Workflow[_, _]](implicit meta: SwfWorkflowMeta[_, W]): Unit = {
    registerWorkflow(meta, logger.warn(_))
  }

  /**
   * Registers the workflow in SWF.
   *
   * Note: if the workflow has already been registered, it is not possible to update the timeout defaults.
   *
   * @param meta the metadata that describes the workflow to register.
   * @param logWith the function to use to log duplicates
   * @tparam W the type of workflow to register, must match the type of the metadata.
   */
  def registerWorkflow[W <: Workflow[_, _]](meta: SwfWorkflowMeta[_, W], logWith: (String) => Unit): Unit = {
    // try to register the workflow
    Exception.catching(classOf[TypeAlreadyExistsException]) opt {
      swf registerWorkflowType {
        new RegisterWorkflowTypeRequest()
          .withDomain(domain)
          .withName(meta.name)
          .withVersion(meta.version)
          .withDefaultExecutionStartToCloseTimeout(meta.defaultExecutionStartToCloseTimeout.toString)
          .withDefaultTaskStartToCloseTimeout(meta.defaultTaskStartToCloseTimeout.toString)
          .withDefaultChildPolicy(meta.childPolicy)
      }
    } getOrElse logWith(s"Workflow [${meta.name}] version [${meta.version}] is already registered.")
  }

  def registerActivity[A <: Activity[_, _]](implicit meta: SwfActivityMeta[_, A with Activity[_, _]]): Unit = {
    registerActivity(meta, logger.warn(_))
  }

  /**
   * Registers the activity in SWF.
   *
   * Note: if the activity has already been registered, it is not possible to update the timeout defaults.
   *
   * @param meta the metadata that describes the activity to register.
   * @param logWith the function to use to log duplicate activities.
   * @tparam A the type of the activity register, must match the type of the metadata.
   */
  def registerActivity[A <: Activity[_, _]](meta: SwfActivityMeta[_, A with Activity[_, _]], logWith: (String) => Unit): Unit = {
    // try to register the activity
    Exception.catching(classOf[TypeAlreadyExistsException]) opt {
      swf registerActivityType {
        new RegisterActivityTypeRequest()
          .withDomain(domain)
          .withName(meta.name)
          .withVersion(meta.version)
          .withDefaultTaskList(new TaskList().withName(meta.defaultTaskList))
          .withDefaultTaskScheduleToStartTimeout(meta.defaultTaskScheduleToStartTimeout.toString)
          .withDefaultTaskScheduleToCloseTimeout(meta.defaultTaskScheduleToCloseTimeout.toString)
          .withDefaultTaskHeartbeatTimeout(meta.defaultTaskHeartbeatTimeout.toString)
          .withDefaultTaskStartToCloseTimeout(meta.defaultTaskStartToCloseTimeout.toString)
      }
    } getOrElse logWith("Activity " + meta.name + " already registered")
  }

}

object WorkerSystem {
  type RunId = String
  type ExecutionId = String
}