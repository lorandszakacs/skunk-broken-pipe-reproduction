package brokenpipe

import cats.effect._
import cats._
import cats.implicits._

import skunk._
import skunk.codec.all._
import skunk.implicits._

import scala.concurrent.duration._

object BrokenPipe extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    pool
      .use { sessions =>
        for {
          _ <- IO(println("Make sure you ran ./docker-init.sh beforehand"))
          _ <- sessions
            .use(query)
            .flatTap(s => IO(println(s"   schemas: ${s.mkString(", ")}")))
          _ <- IO(println("Going to sleep 15s, run ./docker-restart.sh to reproduce bug"))
          _ <- timer.sleep(15.seconds)
          _ <- sessions
            .use(query)
            .flatTap(s => IO(println(s"   schemas: ${s.mkString(", ")}")))
            .void
            .recoverWith { case e: java.io.IOException =>
              if (e.getMessage().contains("Broken pipe")) {
                IO(println("!!!!!")) *>
                  IO(println("  We reproduced the broken pipe exception"))
              } else e.raiseError[IO, Unit]
            }
        } yield ()

      }
      .as(ExitCode.Success)
  }

  private def query(s: Session[IO]): IO[List[String]] = {
    s.prepare(sql"SELECT schema_name FROM information_schema.schemata;".query(name)).use { pc =>
      pc.stream(Void, 128).compile.toList
    }
  }

  def pool: Resource[IO, Resource[IO, Session[IO]]] = {
    implicit val noop = natchez.Trace.Implicits.noop[IO]
    Session.pooled(
      host = "localhost",
      port = 11312,
      user = "skunk-broken-pipe",
      database = "skunk_broken_pipe",
      password = "skunk-broken-pipe".some,
      max = 16
    )
  }

  def single: Resource[IO, Session[IO]] = {
    implicit val noop = natchez.Trace.Implicits.noop[IO]
    Session.single(
      host = "localhost",
      port = 11312,
      user = "skunk-broken-pipe",
      database = "skunk_broken_pipe",
      password = "skunk-broken-pipe".some
    )
  }

}
