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

import scala.Unit

import cats.~>

import doobie._

import fs2.Chunk
import quasar.connector.IdBatch

trait Flow[A] { self =>
  def ingest(chunk: Chunk[A]): ConnectionIO[Unit]
  def delete(ids: IdBatch): ConnectionIO[Unit]
  def replace: ConnectionIO[Unit]
  def append: ConnectionIO[Unit]

  def mapK(f: ConnectionIO ~> ConnectionIO) = new Flow[A] {
    def ingest(chunk: Chunk[A]) = f(self.ingest(chunk))
    def delete(ids: IdBatch): ConnectionIO[Unit] = f(self.delete(ids))
    def replace = f(self.replace)
    def append = f(self.append)
  }
}
