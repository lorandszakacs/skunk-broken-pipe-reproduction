package workaround

import cats._
import cats.implicits._

import cats.effect._
import cats.effect.concurrent._
import cats.effect.implicits._
import fs2.Stream

import scala.util.control.NonFatal
import scala.concurrent.duration.FiniteDuration

final class RemakeOnUseFailure[F[_], R](
  private val mkResource:    Resource[F, Resource[F, R]],
  private val remakeableRef: Ref[F, RemakeOnUseFailure.RemakeableResource[F, R]],
  healthcheck:               R => F[Unit],
  restartSemaphore:          Semaphore[F],
  shouldRestartOn:           PartialFunction[Throwable, Boolean],
  logSuccess:                String => F[Unit],
  logFailure:                (Throwable, String) => F[Unit],
  logRestart:                (Throwable, String) => F[Unit],
)(
  retriesOnRecreate:         Int,
  waitBetweenRetries:        FiniteDuration,
  pollSpeedOnRemakeWaiting:  FiniteDuration,
  waitTimeout:               FiniteDuration,
)(implicit F:                Concurrent[F], t: Timer[F]) {
  import RemakeOnUseFailure._

  def use[A](fa: R => F[A]): F[A] =
    remakeableRef.get.flatMap {
      case us: RemakeableResource.Usable[F, R]   => this.useAndRestartOnFailure(us)(fa)
      case _:  RemakeableResource.Remaking[F, R] => this.waitUntilRemake(fa)
      case f:  RemakeableResource.Failed[F, R]   =>
        logFailure(f.cause, s"Attempting to use resource, even though it is known to have failed: ${f.cause}") *>
          f.clean.attempt.void *> f.cause.raiseError[F, A]
    }

  private def useAndRestartOnFailure[A](u: RemakeableResource.Usable[F, R])(fa: R => F[A]): F[A] =
    u.res.use(fa).recoverWith { case NonFatal(e) =>
      if (shouldRestartOn(e)) {
        for {
          _             <- logRestart(e, s"Attempting restart because we encountered recoverable error: '$e''")
          notRestarting <- restartSemaphore.tryAcquire
          result        <-
            if (notRestarting) {
              for {
                _ <- restart(u).guarantee(restartSemaphore.release)
                r <- this.use(fa)
              } yield r

            }
            else {
              // in case someone else got here first we just wait till they're done.
              this.waitUntilRemake(fa)
            }
        } yield result
      }
      else {
        e.raiseError[F, A]
      }
    }

  private def restart(u: RemakeableResource.Usable[F, R]): F[Unit] =
    for {
      _         <- u.clean.attempt //simply attempting to clean previous things
      _         <- remakeableRef.update(f => RemakeableResource.Remaking(f.clean))
      newUsable <- RemakeableResource.untilUsable(mkResource, healthcheck)(retriesOnRecreate, waitBetweenRetries)(
        logRetry = logRestart,
        logFail  = logFailure,
      )
      _         <- logSuccess("Successfully recreated resource")
      _         <- remakeableRef.update(_ => newUsable)
    } yield ()

  private def waitUntilRemake[A](fa: R => F[A]): F[A] = for {
    _           <- Stream
      .awakeEvery[F](pollSpeedOnRemakeWaiting)
      .evalMap { _ =>
        remakeableRef.get.map {
          case _: RemakeableResource.Failed[F, R]   => Option(())
          case _: RemakeableResource.Usable[F, R]   => Option(())
          case _: RemakeableResource.Remaking[F, R] => Option.empty[Unit]
        }
      }
      .unNoneTerminate
      .compile
      .drain
      .timeout(waitTimeout)
    afterRemake <- this.use(fa)
  } yield afterRemake

}

object RemakeOnUseFailure {

  def resource[F[_]: Concurrent: Timer, R](
    resource:                 Resource[F, Resource[F, R]],
    retriesOnRecreate:        Int,
    waitBetweenRetries:       FiniteDuration,
    pollSpeedOnRemakeWaiting: FiniteDuration,
    waitTimeout:              FiniteDuration,
    logSuccess:               String => F[Unit],
    logRestart:               (Throwable, String) => F[Unit],
    logFailure:               (Throwable, String) => F[Unit],
  )(
    shouldRestartOn:          PartialFunction[Throwable, Boolean],
    healthcheck:              R => F[Unit],
  ): Resource[F, RemakeOnUseFailure[F, R]] = {
    val mkRestartable = for {
      restartable <- RemakeableResource.usable(resource)
      resRef      <- Ref.of[F, RemakeableResource[F, R]](restartable)
      restartSem  <- Semaphore.apply(1L)
    } yield new RemakeOnUseFailure[F, R](
      mkResource               = resource,
      remakeableRef            = resRef,
      healthcheck              = healthcheck,
      restartSemaphore         = restartSem,
      shouldRestartOn          = shouldRestartOn,
      logSuccess               = logSuccess,
      logRestart               = logRestart,
      logFailure               = logFailure,
    )(
      retriesOnRecreate        = retriesOnRecreate,
      waitBetweenRetries       = waitBetweenRetries,
      pollSpeedOnRemakeWaiting = pollSpeedOnRemakeWaiting,
      waitTimeout              = waitTimeout,
    )

    Resource.make(mkRestartable)(_.remakeableRef.get.flatMap(_.clean.attempt.void)).widen
  }

  sealed private trait RemakeableResource[F[_], R] {
    def clean: F[Unit]
  }

  private object RemakeableResource {

    def usable[F[_]: Concurrent, R](r: Resource[F, Resource[F, R]]): F[RemakeableResource.Usable[F, R]] =
      for {
        t <- r.allocated
      } yield Usable[F, R](res = t._1, clean = t._2)

    def untilUsable[F[_]: Concurrent: Timer, R](
      r:          Resource[F, Resource[F, R]],
      testUsable: R => F[Unit],
    )(retries:    Int, wait: FiniteDuration)(
      logRetry:   (Throwable, String) => F[Unit],
      logFail:    (Throwable, String) => F[Unit],
    ): F[RemakeableResource.Usable[F, R]] = {
      def iterateUntil(retries: Int): F[RemakeableResource.Usable[F, R]] =
        for {
          t      <- RemakeableResource.usable[F, R](r).attempt
          result <- t match {
            case Left(e) =>
              if (retries >= 0) {
                for {
                  _      <- logRetry(
                    e,
                    s"Failed to acquire resource upon restart. But retrying after $wait. Retries left: $retries",
                  )
                  _      <- Timer[F].sleep(wait)
                  result <- iterateUntil(retries - 1)
                } yield result
              }
              else {
                logFail(e, s"Failed to acquire resource even after retries. Giving up. $e") *> e
                  .raiseError[F, RemakeableResource.Usable[F, R]]
              }

            case Right(newResource) =>
              for {
                att            <- newResource.res.use(testUsable).attempt
                usableResource <- att match {
                  case Left(e)  =>
                    if (retries >= 0) {
                      for {
                        _      <- logRetry(
                          e,
                          s"Acquired resource, but it did not pass the healthcheck. But retrying after $wait. Retries left: $retries",
                        )
                        _      <- newResource.clean.attempt.void
                        _      <- Timer[F].sleep(wait)
                        result <- iterateUntil(retries - 1)
                      } yield result
                    }
                    else {
                      logFail(e, s"Failed to get a successful health check from resource. Giving up. $e") *> e
                        .raiseError[F, RemakeableResource.Usable[F, R]]
                    }
                  case Right(_) => newResource.pure[F]
                }
              } yield usableResource
          }
        } yield result

      iterateUntil(retries)
    }

    case class Remaking[F[_], R](
      clean: F[Unit]
    ) extends RemakeableResource[F, R]

    case class Usable[F[_], R](
      res:   Resource[F, R],
      clean: F[Unit],
    ) extends RemakeableResource[F, R]

    case class Failed[F[_], R](
      cause: Throwable,
      res:   Resource[F, R],
      clean: F[Unit],
    ) extends RemakeableResource[F, R]
  }

}
