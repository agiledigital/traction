package com.gravitydev.traction
package amazonswf

import java.util.UUID

import com.typesafe.scalalogging.StrictLogging


// Necessary to allow the usage of named parameters on the activityMeta macro
class SwfWorkflowMetaBuilder[T: Serializer, W <: Workflow[_, T] : Serializer] {
  def settings(
                name: String,
                version: String,
                taskList: String,
                id: W => String,
                description: String = "",
                defaultExecutionStartToCloseTimeout: Int = 3600,
                defaultTaskStartToCloseTimeout: Int = 60,
                childPolicy: String = "TERMINATE",
                tags: W => Iterable[String] = { w => Seq()},
                identity: String = UUID.randomUUID.toString
                ) = new SwfWorkflowMeta[T, W](
    name, version, taskList, id,
    description,
    defaultExecutionStartToCloseTimeout,
    defaultTaskStartToCloseTimeout,
    childPolicy,
    tags,
    identity
  )
}

class SwfWorkflowMeta[T: Serializer, W <: Workflow[_, T] : Serializer](
                                                                     val name: String,
                                                                     val version: String,
                                                                     val taskList: String,
                                                                     val id: W => String,
                                                                     val description: String,
                                                                     val defaultExecutionStartToCloseTimeout: Int,
                                                                     val defaultTaskStartToCloseTimeout: Int,
                                                                     val childPolicy: String,
                                                                     val tags: W => Iterable[String],
                                                                     val identity: String
                                                                     ) extends StrictLogging {
  def parseWorkflow(data: String): W = implicitly[Serializer[W]].unserialize(data)

  def serializeWorkflow(workflow: W): String = implicitly[Serializer[W]].serialize(workflow)

  def parseResult(data: String): T = implicitly[Serializer[T]].unserialize(data)

  def serializeResult(result: T): String = implicitly[Serializer[T]].serialize(result)

  def withSettings(
                    name: String = name,
                    version: String = version,
                    taskList: String = taskList,
                    id: W => String = id,
                    description: String = description,
                    defaultExecutionStartToCloseTimeout: Int = defaultExecutionStartToCloseTimeout,
                    defaultTaskStartToCloseTimeout: Int = defaultTaskStartToCloseTimeout,
                    childPolicy: String = childPolicy,
                    tags: W => Iterable[String] = tags,
                    identity: String = identity
                    ) = new SwfWorkflowMeta[T, W](name, version, taskList, id, description, defaultExecutionStartToCloseTimeout, defaultTaskStartToCloseTimeout, childPolicy, tags, identity)

  override def toString: String = s"SwfWorkflowMeta($name,$version)"
}

//class SwfSingleActivityWorkflow[C, T: Serializer, A <: Activity[C, T] : Serializer](val activity: A with Activity[C, T])(implicit meta: SwfActivityMeta[T, A]) extends Workflow[C, T] {
//  def flow(context: C) = activity
//}

