import com.amazonaws.regions.Region
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowAsyncClient
import com.amazonaws.auth.BasicAWSCredentials

import akka.actor.ActorSystem
import com.gravitydev.traction.amazonswf._
import com.gravitydev.traction._
import model.{RenderFlicActivity, UpdateFlic, RenderFlic}
import model.Workflow._
import upickle._

object Consumer extends App {
  implicit val system = ActorSystem("traction-sample")
  implicit val ec = system.dispatcher

  val creds = new BasicAWSCredentials("xxxxxx", "xxxxxxx")
  val swf = new AmazonSimpleWorkflowAsyncClient(creds)
  swf.setRegion(Region.getRegion(com.amazonaws.regions.Regions.AP_SOUTHEAST_2))

  val ws = new WorkerSystem("dev-render", swf)

  val activityMeta = model.Workflow.updateFlicA

  ws.registerActivity[UpdateFlic](activityMeta)
  ws.registerWorkflow[RenderFlic]

  implicit val r = model.Workflow.updateFlicTypeSer
  implicit val t = model.Workflow.updateFlicSer

  ws.startWorkflowWorker(workflow[RenderFlic])(instances = 1)
  ws.startActivityWorker(activity[UpdateFlic], context = ())(instances = 1)
}
