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

import slamdata.Predef._

import cats.data.NonEmptyList
import cats.implicits._

import quasar.api.Column
import quasar.api.resource.ResourcePath
import quasar.connector.destination.{WriteMode => QWriteMode, ResultSink}, ResultSink.{UpsertSink, AppendSink}

sealed trait FlowArgs[T] {
  val path: ResourcePath
  val writeMode: QWriteMode
  val columns: NonEmptyList[Column[T]]
  val idColumn: Option[Column[_]]
  val filterColumn: Option[Column[_]]
}

object FlowArgs {
  def ofUpsert[T](upsert: UpsertSink.Args[T]): FlowArgs[T] = new FlowArgs[T] {
    val path = upsert.path
    val writeMode = upsert.writeMode
    val columns = upsert.columns
    val idColumn = upsert.idColumn.some
    val filterColumn = upsert.idColumn.some
  }

  def ofAppend[T](append: AppendSink.Args[T]): FlowArgs[T] = new FlowArgs[T] {
    val path = append.path
    val writeMode = append.writeMode
    val columns = append.columns
    val idColumn = append.pushColumns.primary
    val filterColumn = None
  }

  def ofCreate[T](
      resourcePath: ResourcePath,
      inpColumns: NonEmptyList[Column[T]])
      : FlowArgs[T] = new FlowArgs[T] {
    val path = resourcePath
    val writeMode = QWriteMode.Replace
    val columns = inpColumns
    val idColumn = None
    val filterColumn = None
  }
}




