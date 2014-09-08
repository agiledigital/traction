package model

import java.util.UUID

import com.gravitydev.traction.{Activity, _}
import com.gravitydev.traction.amazonswf._
import com.gravitydev.traction._
import upickle._

object Workflow {

  implicit val renderFlicW = new SwfWorkflowMetaBuilder[Either[String, Flic], RenderFlic].settings(
    name = "render-flic",
    version = "1.1",
    taskList = "render-flic",
    id = "render-flic-" + _.flic.id
  )

  implicit val updateFlicA = new SwfActivityMetaBuilder[Either[String, Flic], UpdateFlic]().settings(
    name = "update-flic",
    version = "1.1",
    defaultTaskList = "update-flic",
    id = activity => {
      "update-flic-" + activity.flic.id + "-" + activity.toState
    }
  )

  implicit val renderFlicA = new SwfActivityMetaBuilder[Either[String, String], RenderFlicActivity]().settings(
    name = "render-flic",
    version = "1.1",
    defaultTaskList = "render-flic",
    id = activity => {
      "render-flic-" + activity.flic.id
    }
  )

  val updateFlicTypeSer = pickleSerializer[Either[String,model.Flic]]
  val updateFlicSer = pickleSerializer[UpdateFlic]
}

case class Flic(id: String, description: String, state: String, assetId: Option[String])

case class UpdateFlic(flic: Flic, toState: String, assetId: Option[String]) extends Activity[Unit, Either[String, Flic]] {
  def apply(ctx: Unit): Either[String, Flic] = {
    println(s"heyo [$flic] to [$toState] with asset [$assetId].")
    toState match {
//      case "started" => Left("fail whale")
      case _ => Right(flic.copy(state = toState, assetId = assetId))
    }
  }
}

case class RenderFlicActivity(flic: Flic, stuff: List[Int]) extends Activity[Unit, Either[String, String]] {
  def apply(ctx: Unit): Either[String, String] = {
    println(s"Rendering [$flic]...")
    Thread.sleep(10000)
    println(s"Rendered [$flic].")
    //Right(UUID.randomUUID().toString)
    Left("Could not render " + flic)
  }
}



case class RenderFlic(flic: Flic) extends Workflow[Either[String, Flic]] {

  import model.Workflow._

  def flow: Step[Either[String, Flic]] = {

    def complete(flic: Flic): Step[Either[String, Flic]] = toStep1(UpdateFlic(flic, "completed", None))(updateFlicA)

    def rendered(flic: Flic, assetId: String): Step[Either[String, Flic]] = toStep1(UpdateFlic(flic, "rendered", Some(assetId)))(updateFlicA)

    def markFailed(flic: Flic): Step[Either[String, Flic]] = toStep1(UpdateFlic(flic, "failed", None))(updateFlicA)

    def render(flic: Flic): Step[Either[String, Flic]] = toStep1(RenderFlicActivity(flic, List(1)))(renderFlicA).flatMap(result => {
      result match {
        case Right(assetId) => rendered(flic, assetId)
        case Left(error) => {
          Console.out.println(s"Failed to render flic: [$error].")
          markFailed(flic)
        }
      }
    })

    val starter: Step[Either[String, Flic]] = toStep1(UpdateFlic(flic, "started", None))(updateFlicA).flatMap(result => {
      result match {
        case Right(startedFlic) => render(startedFlic)
        case Left(error) => {
          Console.out.println(s"Failed to update flic: [$error].")
          markFailed(flic)
        }
      }
    })

    starter
  }
}
