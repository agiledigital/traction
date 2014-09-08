package com.gravitydev.traction
package amazonswf

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowAsyncClient
import com.amazonaws.services.simpleworkflow.model.{Decision => SwfDecision, _}
import com.gravitydev.awsutil.awsToScala

import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.language.postfixOps

class WorkflowWorker[C, T, W <: Workflow[C, T]](domain: String, swf: AmazonSimpleWorkflowAsyncClient, meta: SwfWorkflowMeta[T, W], ctx: C) extends ConstantAsyncListener {

  import system.dispatcher

  def listen: Future[Unit] = {


    val pollForDecisionTaskRequest = new PollForDecisionTaskRequest()
      .withDomain(domain)
      .withTaskList(new TaskList().withName(meta.taskList))
      .withIdentity(meta.identity)

    log.debug(s"[${meta.identity}] listening for decision tasks on [${meta.taskList}] with [$pollForDecisionTaskRequest].")

    awsToScala(swf.pollForDecisionTaskAsync)(pollForDecisionTaskRequest).map { task =>
      // if there is a task
      Option(task.getTaskToken) match {
        case Some(token) => handleTokenForTask(token, task)
        case _ => {
          // This will happen when the long poll for new work times out before work is received - normal.
          log.debug(s"${meta.identity}] timed out waiting for decision work on [${meta.taskList}], will poll again.")
        }
      }
      ()
    }
  }

  def handleTokenForTask(token: String, task: DecisionTask): Future[Any] = {

    log.debug(s"Received decision task: [${token}].")

    val events = task.getEvents.toList

    // find workflow
    val workflow = events collect { case SwfEvents.WorkflowExecutionStarted(attr) => meta.parseWorkflow(attr.getInput)} head

    // Convert into traction activity events.
    val activityEvents = history(events)
    log.debug("Activity events: " + activityEvents)

    // Find the most recent event for each activity.
    val activityState = state(activityEvents)

    if (log.isDebugEnabled) {
      activityState.foreach {
        case (activityId, state) => log.debug(s"Activity [$activityId] -> [$state].")
      }
    }

    // Apply the activity state to the workflow to determine the next decision that should be taken.
    val decision = workflow.flow(ctx).decide(
      activityState,
      result => CompleteWorkflow(result),
      error => FailWorkflow(error)
    )

    log.debug("Decision: " + decision)

    val decisions = decision match {
      case ScheduleActivities(activities) => activities map { a =>
        log.info(s"${meta.identity}] decided to schedule additional activity [${a.meta.name}] for [${task.getWorkflowExecution.getWorkflowId}] based on token [$token].")
        new SwfDecision()
          .withDecisionType(DecisionType.ScheduleActivityTask)
          .withScheduleActivityTaskDecisionAttributes(
            new ScheduleActivityTaskDecisionAttributes()
              .withActivityType(
                new ActivityType()
                  .withName(a.meta.name)
                  .withVersion(a.meta.version)
              )
              .withActivityId(a.id)
              .withTaskList(
                new TaskList().withName(a.meta.defaultTaskList)
              )
              .withInput(a.input)
          )
      }
      case WaitOnActivities => {
        log.info(s"${meta.identity}] decided to wait on activity for [${task.getWorkflowExecution.getWorkflowId}] based on token [$token].")
        Nil
      }
      case CompleteWorkflow(res) => {
        log.info(s"${meta.identity}] decided to complete [${task.getWorkflowExecution.getWorkflowId}] based on token [$token].")
        List(
          new SwfDecision()
            .withDecisionType(DecisionType.CompleteWorkflowExecution)
            .withCompleteWorkflowExecutionDecisionAttributes(
              new CompleteWorkflowExecutionDecisionAttributes()
                .withResult(meta.serializeResult(res.asInstanceOf[T]))
            )
        )
      }
      case FailWorkflow(reason) => {
        log.error(s"${meta.identity}] failing workflow [${task.getWorkflowExecution.getWorkflowId}] because [$reason].")
        List(new SwfDecision().withDecisionType(DecisionType.FailWorkflowExecution).
          withFailWorkflowExecutionDecisionAttributes(
            new FailWorkflowExecutionDecisionAttributes().
              withReason(reason.takeRight(256))))
      }
      case other => {
        log.error(s"${meta.identity}] received unexpected decision [$other]. Will stop the workflow.")
        List(new SwfDecision().withDecisionType(DecisionType.FailWorkflowExecution).
          withFailWorkflowExecutionDecisionAttributes(
            new FailWorkflowExecutionDecisionAttributes().
              withReason("Decision not handled").
              withDetails(other.toString.takeRight(32768))))
      }
    }

    val request = new RespondDecisionTaskCompletedRequest().withDecisions(decisions).withTaskToken(token)

    log.debug("Sending decision request.")
    awsToScala(swf.respondDecisionTaskCompletedAsync)(request).recover {
      case e => log.error(e, "Error responding to decision task")
    }
  }

  /**
   * Converts the full list of SWF events into the traction activity events. That is, the returned list
   * does not contain any worflow related events.
   *
   * @param events the SWF events to convert.
   * @return the traction activity events.
   */
  def history(events: List[HistoryEvent]): List[ActivityEvent] = {
    /* Helper function to convert an event id into an activity id. */
    def getActivityId(eventId: Long): String = (events.find(_.getEventId == eventId).get.getActivityTaskScheduledEventAttributes.getActivityId)

    events collect {
      case SwfEvents.ActivityTaskScheduled(attr) => ActivityScheduled(attr.getActivityId)
      case SwfEvents.ActivityTaskStarted(attr) => ActivityStarted(getActivityId(attr.getScheduledEventId))
      case SwfEvents.ActivityTaskCompleted(attr) => ActivitySucceeded(getActivityId(attr.getScheduledEventId), attr.getResult)
      case SwfEvents.ActivityTaskFailed(attr) => ActivityFailed(getActivityId(attr.getScheduledEventId), attr.getReason + ": " + attr.getDetails)
      case SwfEvents.ActivityTaskTimedOut(attr) => ActivityTimedOut(getActivityId(attr.getScheduledEventId), attr.getTimeoutType)
      case SwfEvents.ScheduleActivityTaskFailed(attr) => ActivityStartFailed(attr.getActivityId, attr.getCause + ": " + attr.getActivityType)
    }
  }

  /**
   * Converts the most recent interesting event for each activity. Interesting events are those that
   * occur after the event was scheduled.
   *
   * Treats a failure to start an activity as a workflow failure.
   *
   * @param history the full history of the workflow execution to search.
   * @return a mapping from the activity ids to their most recent events.
   */
  def state(history: List[ActivityEvent]): Map[String, ActivityState] = (history collect {
    case scheduled@ActivityScheduled(activityId) => {
      // find the last event relating this activity
      history.reverse.find(ev => ev.activityId == activityId && scheduled != ev) flatMap {
        case ActivitySucceeded(id, res) => Some(id -> ActivityComplete(Right(res)))
        case ActivityFailed(id, reason) => Some(id -> ActivityComplete(Left(reason)))
        case ActivityStarted(id) => Some(id -> ActivityInProcess)
        case ActivityTimedOut(id, reason) => Some(id -> ActivityTimedOutState(reason,
          history.filter(ev => ev.activityId == activityId && scheduled != ev && ev.isInstanceOf[ActivityTimedOut]).size))
        case x => sys.error("Unhandled event: " + x)
      } getOrElse activityId -> ActivityInProcess
    }
    case ActivityStartFailed(activityId, reason) => {
      activityId -> ActivityComplete(Left(reason))
    }
  }).toMap

  override def toString: String = s"WorkflowWorker($domain, $meta)"
}

