/*
 * Copyright 2020 Precog Data
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.lib.jdbc.destination.flow

import slamdata.Predef._

import cats.{~>, MonadError}
import cats.effect._
import cats.implicits._

import doobie._
import doobie.implicits._
import doobie.free.connection.rollback

import scala.concurrent.duration.FiniteDuration

object Retry {
  def apply[F[_]: Effect: Timer](maxN: Int, timeout: FiniteDuration): ConnectionIO ~> ConnectionIO =
    natural(maxN, timeout, 0)

  private def natural[F[_]: Effect: Timer](maxN: Int, timeout: FiniteDuration, n: Int)
      : ConnectionIO ~> ConnectionIO = λ[ConnectionIO ~> ConnectionIO] { action =>
    action.attempt flatMap {
      case Right(a) => a.pure[ConnectionIO]
      case Left(e) if n < maxN =>
        rollback >>
        toConnectionIO[F].apply(Timer[F].sleep(timeout)) >>
        natural[F](maxN, timeout, n + 1).apply(action)
      case Left(e) =>
        MonadError[ConnectionIO, Throwable].raiseError(e)
    }
  }
  private def toConnectionIO[F[_]: Effect]: F ~> ConnectionIO =
    Effect.toIOK[F] andThen LiftIO.liftK[ConnectionIO]
}
