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
package org.apache.gluten.qt

import scala.reflect.runtime.{universe => ru}
import scala.reflect.runtime.currentMirror
import scala.util.{Failure, Success, Try}

object ReflectionUtils {

  def accessPrivateClass(className: String, args: Any*): Any = {
    val classSymbol = currentMirror.staticClass(className)
    val classMirror = currentMirror.reflectClass(classSymbol)
    val constructorSymbol = classSymbol.primaryConstructor.asMethod
    val constructorMethod = classMirror.reflectConstructor(constructorSymbol)
    constructorMethod(args: _*)
  }

  def accessPrivateMethod(instance: Any, methodName: String, args: Any*): Any = {
    val instanceMirror = currentMirror.reflect(instance)
    val methodSymbols =
      instanceMirror.symbol.typeSignature.member(ru.TermName(methodName)).asTerm.alternatives

    val exceptions = scala.collection.mutable.ArrayBuffer[Throwable]()
    methodSymbols.iterator
      .map {
        methodSymbol =>
          val method = instanceMirror.reflectMethod(methodSymbol.asMethod)
          val result = Try(method(args: _*))

          result match {
            case Failure(e) =>
              exceptions += e
              None
            case Success(value) => Some(value)
          }
      }
      .collectFirst { case Some(value) => value }
      .getOrElse {
        val newException = new IllegalArgumentException(s"Method not found for $methodName")
        exceptions.foreach(newException.addSuppressed)
        throw newException
      }
  }

}
