package com.gravitydev.traction
package amazonswf

import java.util.UUID

// Necessary to allow the usage of named parameters on the activityMeta macro 
class SwfActivityMetaBuilder[T: Serializer, A <: Activity[_, T] : Serializer] {
  def settings(
                name: String,
                version: String,
                defaultTaskList: String,
                id: A => String,
                description: String = "",
                defaultTaskScheduleToCloseTimeout: Int = 600,
                defaultTaskScheduleToStartTimeout: Int = 600,
                defaultTaskStartToCloseTimeout: Int = 600,
                defaultTaskHeartbeatTimeout: Int = 600,
                numberOfRetries: Int = 3,
                identity: String = UUID.randomUUID.toString
                ) = new SwfActivityMeta[T, A](
    name, version, defaultTaskList, id, description,
    defaultTaskScheduleToCloseTimeout,
    defaultTaskScheduleToStartTimeout,
    defaultTaskStartToCloseTimeout,
    defaultTaskHeartbeatTimeout,
    numberOfRetries,
    identity
  )
}

class SwfActivityMeta[T, A <: Activity[_, T]](
                                               val name: String,
                                               val version: String,
                                               val defaultTaskList: String,
                                               val id: A => String,
                                               val description: String,
                                               val defaultTaskScheduleToCloseTimeout: Int,
                                               val defaultTaskScheduleToStartTimeout: Int,
                                               val defaultTaskStartToCloseTimeout: Int,
                                               val defaultTaskHeartbeatTimeout: Int,
                                               val numberOfRetries: Int,
                                               val identity: String
                                               )(implicit resultS: Serializer[T], activityS: Serializer[A]) {
  def parseActivity(data: String): A = activityS.unserialize(data)

  def serializeActivity(activity: A): String = activityS.serialize(activity)

  def parseResult(data: String): T = resultS.unserialize(data)

  def serializeResult(result: T): String = resultS.serialize(result /*.asInstanceOf[T]*/)

  def withSettings(name: String = name,
                   version: String = version,
                   defaultTaskList: String = defaultTaskList,
                   id: A => String = id,
                   description: String = description,
                   defaultTaskScheduleToCloseTimeout: Int = defaultTaskScheduleToCloseTimeout,
                   defaultTaskScheduleToStartTimeout: Int = defaultTaskScheduleToStartTimeout,
                   defaultTaskStartToCloseTimeout: Int = defaultTaskStartToCloseTimeout,
                   defaultTaskHeartbeatTimeout: Int = defaultTaskHeartbeatTimeout,
                   numberOfRetries: Int = numberOfRetries,
                   identity: String = identity) = new SwfActivityMeta[T, A](name, version, defaultTaskList, id, description, defaultTaskScheduleToCloseTimeout, defaultTaskScheduleToStartTimeout, defaultTaskStartToCloseTimeout
    , defaultTaskHeartbeatTimeout, numberOfRetries, identity
  )(resultS, activityS)

//  def toWorkflowMeta(implicit s2: Serializer[T], s1: Serializer[SwfSingleActivityWorkflow[T, A]]): SwfWorkflowMeta[T, SwfSingleActivityWorkflow[T, A]] = {
//    //val s1: Serializer[SwfSingleActivityWorkflow[T,A]] = ??? //implicitly[Serializer[SwfSingleActivityWorkflow[T,A]]]
//    //val s2 = implicitly[Serializer[T]]
//
//    (new SwfWorkflowMetaBuilder[T, SwfSingleActivityWorkflow[T, A]]()(s2, s1))
//      .settings(
//        name = name,
//        version = version,
//        taskList = name + ".workflow",
//        id = wf => id(wf.activity)
//      )
//  }
  override def toString: String = s"SwfActivityMeta($name, $version)"
}

