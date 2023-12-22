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
package io.glutenproject.collections;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ReferenceMap<K, V> extends AbstractReferenceMap<K, V> {
  private final org.apache.commons.collections.map.ReferenceMap shimMap;

  private int getReferenceStrength(ReferenceStrength strength) {
    switch (strength) {
      case HARD:
        return org.apache.commons.collections.map.AbstractReferenceMap.HARD;
      case SOFT:
        return org.apache.commons.collections.map.AbstractReferenceMap.SOFT;
      case WEAK:
        return org.apache.commons.collections.map.AbstractReferenceMap.WEAK;
      default:
        throw new IllegalArgumentException();
    }
  }

  public ReferenceMap(ReferenceStrength keyStrength, ReferenceStrength valueStrength) {
    shimMap =
        new org.apache.commons.collections.map.ReferenceMap(
            getReferenceStrength(keyStrength), getReferenceStrength(valueStrength));
  }

  @NotNull
  @Override
  @SuppressWarnings("unchecked")
  public Set<Entry<K, V>> entrySet() {
    return (Set<Entry<K, V>>) shimMap.entrySet();
  }

  @Override
  @SuppressWarnings("unchecked")
  public V getOrDefault(Object key, V defaultValue) {
    return (V) shimMap.getOrDefault(key, defaultValue);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void forEach(BiConsumer<? super K, ? super V> action) {
    shimMap.forEach(action);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
    shimMap.replaceAll(function);
  }

  @Nullable
  @Override
  @SuppressWarnings("unchecked")
  public V putIfAbsent(K key, V value) {
    return (V) shimMap.putIfAbsent(key, value);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean remove(Object key, Object value) {
    return shimMap.remove(key, value);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean replace(K key, V oldValue, V newValue) {
    return shimMap.replace(key, oldValue, newValue);
  }

  @Nullable
  @Override
  @SuppressWarnings("unchecked")
  public V replace(K key, V value) {
    return (V) shimMap.replace(key, value);
  }

  @Override
  @SuppressWarnings("unchecked")
  public V computeIfAbsent(K key, @NotNull Function<? super K, ? extends V> mappingFunction) {
    return (V) shimMap.computeIfAbsent(key, mappingFunction);
  }

  @Override
  @SuppressWarnings("unchecked")
  public V computeIfPresent(
      K key, @NotNull BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return (V) shimMap.computeIfPresent(key, remappingFunction);
  }

  @Override
  @SuppressWarnings("unchecked")
  public V compute(
      K key, @NotNull BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return (V) shimMap.compute(key, remappingFunction);
  }

  @Override
  @SuppressWarnings("unchecked")
  public V merge(
      K key,
      @NotNull V value,
      @NotNull BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
    return (V) shimMap.merge(key, value, remappingFunction);
  }
}
