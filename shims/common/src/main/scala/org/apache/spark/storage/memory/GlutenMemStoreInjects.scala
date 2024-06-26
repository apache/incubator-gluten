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
package org.apache.spark.storage.memory

object GlutenMemStoreInjects {
  private var INSTANCE: GlutenExtMemStoreInjects = _
  private var memStoreSize: Long = _
  private var reservationListener: ExtMemStoreReservationListener = _

  def setInstance(instance: GlutenExtMemStoreInjects): Unit = {
    INSTANCE = instance
  }
  def getInstance(): GlutenExtMemStoreInjects = {
    if (INSTANCE == null) {
      throw new IllegalStateException("GlutenExtMemStoreInjects is not initialized")
//      INSTANCE = new GlutenExtMemStoreInjects()
    }
    INSTANCE
  }

  def setMemStoreSize(size: Long): Unit = {
    memStoreSize = size;
  }

  def getMemStoreSize(): Long = {
    memStoreSize
  }

  def setReservationListener(listener: ExtMemStoreReservationListener): Unit = {
    reservationListener = listener
  }

  def getReservationListener(): ExtMemStoreReservationListener = {
    reservationListener
  }
}
