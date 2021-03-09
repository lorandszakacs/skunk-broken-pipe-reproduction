package workaround

import skunk._
import cats.implicits._
import cats.effect._
import scala.concurrent.duration._

object BrokenPipeWorkaround extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    def query(s: Session[IO]): IO[List[String]] = {
      import skunk._
      import skunk.implicits._
      import skunk.codec.all.name
      s.prepare(sql"SELECT schema_name FROM information_schema.schemata;".query(name)).use { pc =>
        pc.stream(Void, 128).compile.toList
      }
    }

    Sessions
      .resource[IO]
      .use { sessions =>
        for {
          _ <- IO(println("Make sure you ran ./docker-init.sh beforehand"))
          _ <- sessions
            .use(query)
            .flatTap(s => IO(println(s"   schemas: ${s.mkString(", ")}")))
          _ <- IO(
            println(
              "Going to sleep 15s, run ./docker-restart.sh to trigger Broken pipe, then see how neatly it gets restarted"
            )
          )
          _ <- timer.sleep(15.seconds)
          _ <- sessions
            .use(query)
            .flatTap(s => IO(println(s"   schemas: ${s.mkString(", ")}")))
        } yield ()

      }
      .as(ExitCode.Success)
  }

  //userland - workaround
  sealed trait Sessions[F[_]] {
    def use[A](t: Session[F] => F[A]): F[A]
  }

  object Sessions {
    import scala.concurrent.duration._

    def resource[F[_]: Concurrent: ContextShift: Timer]: Resource[F, Sessions[F]] = {
      val skunkPool: Resource[F, Resource[F, Session[F]]] = {
        implicit val noop = natchez.Trace.Implicits.noop[F]
        Session.pooled(
          host = "localhost",
          port = 11312,
          user = "skunk-broken-pipe",
          database = "skunk_broken_pipe",
          password = "skunk-broken-pipe".some,
          max = 16
        )
      }

      for {
        ur <- RemakeOnUseFailure.resource[F, Session[F]](
          resource = skunkPool,
          retriesOnRecreate = 60,
          waitBetweenRetries = 1.second,
          pollSpeedOnRemakeWaiting = 100.millis,
          waitTimeout = 5.seconds,
          logSuccess = s => Concurrent[F].delay(println(s)),
          logRestart = (t, s) => Concurrent[F].delay(println(s"$s -> $t")),
          logFailure = (t, s) => Concurrent[F].delay(println(s"$s -> $t"))
        )(
          shouldRestartOn = requiresSkunkPoolRemake,
          healthcheck = testPool[F]
        )
      } yield new SessionsImpl[F](ur)
    }

    private def requiresSkunkPoolRemake: PartialFunction[Throwable, Boolean] = {
      //happens roughly immediately as the connection is killed
      case _: java.net.ConnectException => true

      //happens after network connection to DB was severed for a while
      case e: java.io.IOException => e.getMessage.contains("Broken pipe")

      //add new cases upon discovery
      case _ => false
    }

    private def testPool[F[_]: BracketThrow](s: Session[F]): F[Unit] = {
      import skunk._
      import skunk.implicits._
      import skunk.codec.all.int8
      val q: Query[Void, Long] = sql"SELECT COUNT(*) FROM information_schema.schemata;".query(int8)
      s.prepare(q).use(pc => pc.unique(Void)).void
    }

    private class SessionsImpl[F[_]: MonadThrow](
        private val skunkPool: RemakeOnUseFailure[F, Session[F]]
    ) extends Sessions[F] {

      override def use[A](t: Session[F] => F[A]): F[A] = skunkPool.use(t)

      //TODO: make RemakeOnUseFailure more generic to be able to use it w/ streams properly
    }
  }
}
