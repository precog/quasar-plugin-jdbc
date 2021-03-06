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

package quasar.lib.jdbc.destination

import quasar.lib.jdbc._

import java.lang.String

import scala.StringContext
import scala.util.{Either, Left, Random, Right}

import argonaut._, Argonaut._, ArgonautCats._

import cats.data.{EitherT, NonEmptyList}
import cats.effect._
import cats.implicits._

import doobie._

import org.slf4s.{Logger, LoggerFactory}

import quasar.api.destination.{DestinationError => DE}
import quasar.connector.MonadResourceErr
import quasar.connector.destination._

abstract class DeferredJdbcDestinationModule[C: DecodeJson] extends DestinationModule {

  type InitError = DE.InitializationError[Json]

  def transactorConfig(config: C): Either[NonEmptyList[String], TransactorConfig]

  def jdbcDestination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
      config: C,
      transactor: Resource[F, Transactor[F]],
      pushPull: PushmiPullyu[F],
      log: Logger)
      : Resource[F, Either[InitError, Destination[F]]]

  ////

  def destination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
      config: Json,
      pushPull: PushmiPullyu[F])
      : Resource[F, Either[InitError, Destination[F]]] = {

    val id = s"${destinationType.name.value}-v${destinationType.version}"

    val cfg0: Either[InitError, C] =
      config.as[C].fold(
        (_, c) =>
          Left(DE.malformedConfiguration[Json, InitError](
            destinationType,
            jString(Redacted),
            s"Failed to decode $id JSON at ${c.toList.map(_.show).mkString(", ")}")),
        Right(_))

    def liftF[X](fa: F[X]): EitherT[Resource[F, ?], InitError, X] =
      EitherT.right(Resource.eval(fa))

    val init = for {
      cfg <- EitherT.fromEither[Resource[F, ?]](cfg0)

      xaCfg <- EitherT.fromEither[Resource[F, ?]] {
        transactorConfig(cfg)
          .leftMap(errs => scalaz.NonEmptyList(errs.head, errs.tail: _*))
          .leftMap(DE.invalidConfiguration[Json, InitError](
            destinationType,
            sanitizeDestinationConfig(config), _))
      }

      tag <- liftF(Sync[F].delay(Random.alphanumeric.take(6).mkString))

      debugId = s"destination.$id.$tag"

      slog <- liftF(Sync[F].delay(LoggerFactory(s"quasar.lib.$debugId")))

      dest <- EitherT(jdbcDestination(cfg, ManagedTransactor[F](debugId, xaCfg), pushPull, slog))

      _ <- liftF(Sync[F].delay(slog.info(s"Initialized $debugId: ${sanitizeDestinationConfig(config)}")))
    } yield dest

    init.value
  }
}
