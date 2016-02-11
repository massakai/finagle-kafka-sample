package jp.gr.java_conf.massakai.application

import com.twitter.finagle.{Http, Service, http}
import com.twitter.util.{Await, Future}

object HelloFinagleServer extends App {
  val service = new Service[http.Request, http.Response] {
    def apply(req: http.Request): Future[http.Response] =
      Future.value(
        http.Response(req.version, http.Status.Ok)
      )
  }
  val server = Http.serve(":8080", service)
  Await.ready(server)
}
