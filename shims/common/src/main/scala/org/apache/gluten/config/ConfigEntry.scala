/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.config

import org.apache.gluten.config.BackendType.BackendType

trait ConfigEntry[T] {
  def key: String
  def doc: String
  def version: String
  def backend: BackendType.BackendType
  def isPublic: Boolean
  def alternatives: List[String]
  def valueConverter: String => T
  def stringConverter: T => String
  def readFrom(conf: ConfigProvider): T
  def defaultValue: Option[T]
  def defaultValueString: String

  final protected def readString(provider: ConfigProvider): Option[String] = {
    alternatives.foldLeft(provider.get(key))((res, nextKey) => res.orElse(provider.get(nextKey)))
  }

  override def toString: String = {
    s"ConfigEntry(key=$key, defaultValue=$defaultValueString, doc=$doc, " +
      s"public=$isPublic, version=$version)"
  }

  ConfigEntry.registerEntry(this)
}

private[gluten] class OptionalConfigEntry[T](
    _key: String,
    _doc: String,
    _version: String,
    _backend: BackendType,
    _isPublic: Boolean,
    _alternatives: List[String],
    _valueConverter: String => T,
    _stringConverter: T => String)
  extends ConfigEntry[Option[T]] {
  override def key: String = _key

  override def doc: String = _doc

  override def version: String = _version

  override def backend: BackendType = _backend

  override def isPublic: Boolean = _isPublic

  override def alternatives: List[String] = _alternatives

  override def valueConverter: String => Option[T] = s => Option(_valueConverter(s))

  override def stringConverter: Option[T] => String = v => v.map(_stringConverter).orNull

  override def readFrom(conf: ConfigProvider): Option[T] = readString(conf).map(_valueConverter)

  override def defaultValue: Option[Option[T]] = None

  override def defaultValueString: String = ConfigEntry.UNDEFINED
}

private[gluten] class ConfigEntryWithDefault[T](
    _key: String,
    _doc: String,
    _version: String,
    _backend: BackendType,
    _isPublic: Boolean,
    _alternatives: List[String],
    _valueConverter: String => T,
    _stringConverter: T => String,
    _defaultVal: T)
  extends ConfigEntry[T] {
  override def key: String = _key

  override def doc: String = _doc

  override def version: String = _version

  override def backend: BackendType = _backend

  override def isPublic: Boolean = _isPublic

  override def alternatives: List[String] = _alternatives

  override def valueConverter: String => T = _valueConverter

  override def stringConverter: T => String = _stringConverter

  override def readFrom(conf: ConfigProvider): T = {
    readString(conf).map(valueConverter).getOrElse(_defaultVal)
  }

  override def defaultValue: Option[T] = Option(_defaultVal)

  override def defaultValueString: String = stringConverter(_defaultVal)
}

private class ConfigEntryWithDefaultString[T](
    _key: String,
    _doc: String,
    _version: String,
    _backend: BackendType,
    _isPublic: Boolean,
    _alternatives: List[String],
    _valueConverter: String => T,
    _stringConverter: T => String,
    _defaultVal: String)
  extends ConfigEntry[T] {
  override def key: String = _key

  override def doc: String = _doc

  override def version: String = _version

  override def backend: BackendType = _backend

  override def isPublic: Boolean = _isPublic

  override def alternatives: List[String] = _alternatives

  override def valueConverter: String => T = _valueConverter

  override def stringConverter: T => String = _stringConverter

  override def readFrom(conf: ConfigProvider): T = {
    val value = readString(conf).getOrElse(_defaultVal)
    valueConverter(value)
  }

  override def defaultValue: Option[T] = Some(valueConverter(_defaultVal))

  override def defaultValueString: String = _defaultVal
}

private[gluten] class ConfigEntryFallback[T](
    _key: String,
    _doc: String,
    _version: String,
    _backend: BackendType,
    _isPublic: Boolean,
    _alternatives: List[String],
    fallback: ConfigEntry[T])
  extends ConfigEntry[T] {
  override def key: String = _key

  override def doc: String = _doc

  override def version: String = _version

  override def backend: BackendType = _backend

  override def isPublic: Boolean = _isPublic

  override def alternatives: List[String] = _alternatives

  override def valueConverter: String => T = fallback.valueConverter

  override def stringConverter: T => String = fallback.stringConverter

  override def readFrom(conf: ConfigProvider): T = {
    readString(conf).map(valueConverter).getOrElse(fallback.readFrom(conf))
  }

  override def defaultValue: Option[T] = fallback.defaultValue

  override def defaultValueString: String = fallback.defaultValueString
}

object ConfigEntry {

  val UNDEFINED = "<undefined>"

  private val knownConfigs =
    new java.util.concurrent.ConcurrentHashMap[String, ConfigEntry[_]]()

  private def registerEntry(entry: ConfigEntry[_]): Unit = {
    val existing = knownConfigs.putIfAbsent(entry.key, entry)
    require(existing == null, s"Config entry ${entry.key} already registered!")
  }

  def containsEntry(entry: ConfigEntry[_]): Boolean = {
    Option(knownConfigs.get(entry.key)).isDefined
  }

  def findEntry(key: String): ConfigEntry[_] = knownConfigs.get(key)
}
