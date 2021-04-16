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

import cats.data._
import cats.effect._
import cats.implicits._

import doobie._
import doobie.implicits._

import fs2.{Pipe, Stream}

import quasar.api.Column
import quasar.api.push.OffsetKey
import quasar.api.resource._
import quasar.connector._
import quasar.connector.destination.ResultSink, ResultSink.{UpsertSink, AppendSink}
import quasar.connector.render.RenderConfig

import org.slf4s.Logger

import skolems.∀

import implicits._

trait FlowSinks[F[_], T, C] {
  type Consume[Event[_], A] =
    Pipe[F, Event[OffsetKey.Actual[A]], OffsetKey.Actual[A]]

  def flowResource(args: FlowArgs[T]): Resource[F, Flow[C]]
  def render(args: FlowArgs[T]): RenderConfig[C]
  val flowTransactor: Transactor[F]
  val flowLogger: Logger

  def create(path: ResourcePath, cols: NonEmptyList[Column[T]])(implicit F: Bracket[F, Throwable])
      : (RenderConfig[C], Pipe[F, C, Unit]) = {
    val args = FlowArgs.ofCreate(path, cols)
    (render(args), in => for {
      flow <- Stream.resource(flowResource(args))
      _ <- in.through(createPipe(flow))
    } yield ())
  }

  def createPipe[A](flow: Flow[C])(implicit F: Bracket[F, Throwable]): Pipe[F, C, Unit] = { events =>
    events.chunks.evalMap(c => flow.ingest(c).transact(flowTransactor)) ++
      Stream.eval(flow.replace.transact(flowTransactor))
  }

  def upsert(upsertArgs: UpsertSink.Args[T])(implicit F: Sync[F])
      : (RenderConfig[C], ∀[Consume[DataEvent[C, *], *]]) = {
    val args = FlowArgs.ofUpsert(upsertArgs)
    val consume = ∀[Consume[DataEvent[C, *], *]](upsertPipe(args))
    (render(args), consume)
  }

  def append(appendArgs: AppendSink.Args[T])(implicit F: Sync[F])
      : (RenderConfig[C], ∀[Consume[AppendEvent[C, *], *]]) = {
    val args = FlowArgs.ofAppend(appendArgs)
    val consume = ∀[Consume[AppendEvent[C, *], *]](upsertPipe(args))
    (render(args), consume)
  }

  private def upsertPipe[A](args: FlowArgs[T])(implicit F: Sync[F])
      : Pipe[F, DataEvent[C, OffsetKey.Actual[A]], OffsetKey.Actual[A]] = { events =>
    for{
      flow <- Stream.resource(flowResource(args))
      offset <- events.through(upsertPipe0[C, OffsetKey.Actual[A]](flow).withLogger(flowLogger))
    } yield offset
  }

  private def upsertPipe0[A, B](flow: Flow[A])(implicit F: Bracket[F, Throwable])
      : Pipe[F, DataEvent[A, B], B] = { events =>
    def handleEvent(event: DataEvent[A, B]): ConnectionIO[Option[B]] = event match {
      case DataEvent.Create(chunk) =>
        flow.ingest(chunk) >>
        none[B].pure[ConnectionIO]
      case DataEvent.Delete(ids) =>
        flow.delete(ids) >>
        none[B].pure[ConnectionIO]
      case DataEvent.Commit(offset) =>
        flow.replace >>
        offset.some.pure[ConnectionIO]
    }
    events.evalMap(x => handleEvent(x).transact(flowTransactor)).unNone
  }
}

