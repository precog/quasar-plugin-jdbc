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

import cats.effect.Sync

import fs2.{Pipe, Stream}

import quasar.api.push.OffsetKey
import quasar.connector._

import org.slf4s.Logger

object implicits {
  implicit class PipeLogger[F[_]: Sync, A, B, C](pipe: Pipe[F, DataEvent[C, OffsetKey.Actual[A]], B]) {
    def withLogger(logger: Logger): Pipe[F, DataEvent[C, OffsetKey.Actual[A]], B] = { events =>
      def trace(msg: => String) = Sync[F].delay(logger.trace(msg))

      def logEvents(event: DataEvent[_, _]): F[Unit] = trace { event match {
        case DataEvent.Create(chunk) =>
          s"Loading chunk with size: ${chunk.size}"
        case DataEvent.Delete(idBatch) =>
          s"Deleting ${idBatch.size} records"
        case DataEvent.Commit(_) =>
          "Commit"
      }}
      events.evalTap(logEvents).through(pipe.logStartAndEnd(logger))
    }
  }

  implicit class StartEndLogger[F[_]: Sync, A, B](pipe: Pipe[F, A, B]) {
    def logStartAndEnd(logger: Logger): Pipe[F, A, B] = { events =>
      def trace(msg: => String) = Sync[F].delay(logger.trace(msg))
      Stream.eval_(trace("Strating load")) ++
      events.through(pipe) ++
      Stream.eval_(trace("Finished load"))
    }
  }
}
