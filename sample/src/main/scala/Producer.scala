import java.util.UUID

import akka.actor.ActorSystem
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Region
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowAsyncClient
import com.gravitydev.traction.amazonswf._
import model.{Flic, RenderFlic}

object Producer extends App {

  implicit val system = ActorSystem("traction-producer-sample")
  implicit val ec = system.dispatcher



  val creds = new BasicAWSCredentials("xxxxxx", "xxxxxxx")
  val swf = new AmazonSimpleWorkflowAsyncClient(creds)
  swf.setRegion(Region.getRegion(com.amazonaws.regions.Regions.AP_SOUTHEAST_2))

  val ws = new WorkerSystem("dev-render", swf)

  implicit val workflow = model.Workflow.renderFlicW

  var i = 0
  do {
    println(s"Starting workflow for [$i].")
    ws.run(RenderFlic(Flic(UUID.randomUUID().toString, s"description $i", "state", None))) map { result =>
      result match {
        case Right(run) => println(s"Submitted workflow as run [$run].")
        case Left(error) => println(s"Failed to submit workflow: [$error].")
      }

    }
    Console.readLine()
    i += 1
  } while (true)
}
