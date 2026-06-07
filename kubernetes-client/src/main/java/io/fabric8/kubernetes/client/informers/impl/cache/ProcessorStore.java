/*
 * Copyright (C) 2015 Red Hat, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fabric8.kubernetes.client.informers.impl.cache;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.informers.cache.Cache;
import io.fabric8.kubernetes.client.informers.impl.cache.ProcessorListener.Notification;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wraps a {@link Cache} and a {@link SharedProcessor} to distribute events related to changes and syncs
 */
public class ProcessorStore<T extends HasMetadata> {

  private final CacheImpl<T> cache;
  private final SharedProcessor<T> processor;
  private final AtomicBoolean synced = new AtomicBoolean();
  private final List<String> deferredAdd = new ArrayList<>();

  public ProcessorStore(CacheImpl<T> cache, SharedProcessor<T> processor) {
    this.cache = cache;
    this.processor = processor;
  }

  public void add(T obj) {
    update(obj);
  }

  public void update(List<T> items) {
    items.stream().map(this::updateInternal).filter(Objects::nonNull).forEach(n -> this.processor.distribute(n, false));
  }

  private Notification<T> updateInternal(T obj) {
    T oldObj = this.cache.put(obj);
    Notification<T> notification = null;
    if (oldObj != null) {
      if (!Objects.equals(oldObj.getMetadata().getResourceVersion(), obj.getMetadata().getResourceVersion())) {
        notification = new ProcessorListener.UpdateNotification<>(oldObj, obj);
      }
    } else if (synced.get() || !cache.isFullState()) {
      notification = new ProcessorListener.AddNotification<>(obj);
    } else {
      deferredAdd.add(getKey(obj));
    }
    return notification;
  }

  public void update(T obj) {
    Notification<T> notification = updateInternal(obj);
    if (notification != null) {
      this.processor.distribute(notification, false);
    }
  }

  public void delete(T obj) {
    Object oldObj = this.cache.remove(obj);
    if (oldObj != null) {
      this.processor.distribute(new ProcessorListener.DeleteNotification<>(obj, false), false);
    }
  }

  public List<T> list() {
    return cache.list();
  }

  public List<String> listKeys() {
    return cache.listKeys();
  }

  public T get(T object) {
    return cache.get(object);
  }

  public T getByKey(String key) {
    return cache.getByKey(key);
  }

  /**
   * Syncs the cache with the given set of keys from the latest list operation.
   * <p>
   * The cache is mutated immediately (deleting any cached items whose keys are not in
   * {@code nextKeys}), but the resulting notifications are <em>not</em> distributed here. Instead
   * the deferred add notifications (if this is the first sync) and the delete notifications are
   * collected into {@code notifications} so the caller can distribute them after the {@code onList}
   * event has been emitted (see {@link #distributeListNotifications(List)}).
   *
   * @param nextKeys the set of keys from the latest list result
   * @param notifications collector populated with the notifications to be distributed afterwards
   * @return {@code true} if the cache was empty before processing deletions, {@code false} otherwise
   */
  public boolean syncList(Set<String> nextKeys, List<Notification<T>> notifications) {
    if (synced.compareAndSet(false, true)) {
      deferredAdd.stream().map(cache::getByKey).filter(Objects::nonNull)
          .forEach(v -> notifications.add(new ProcessorListener.AddNotification<>(v)));
      deferredAdd.clear();
    }
    List<T> current = cache.list();
    current.forEach(v -> {
      String key = cache.getKey(v);
      if (!nextKeys.contains(key)) {
        cache.remove(v);
        notifications.add(new ProcessorListener.DeleteNotification<>(v, true));
      }
    });
    return current.isEmpty();
  }

  /**
   * Distributes the notifications collected by {@link #syncList(Set, List)}. This is kept separate
   * from the cache sync so that callers can emit the {@code onList} event before these add/delete
   * events are propagated to the handlers.
   *
   * @param notifications the notifications collected during the cache sync
   */
  public void distributeListNotifications(List<Notification<T>> notifications) {
    notifications.forEach(n -> this.processor.distribute(n, false));
  }

  /**
   * Distributes the onList event to all registered handlers and returns the serial executor
   * used for event processing. Callers can use the returned executor to schedule work that
   * must run after all handler notifications have been processed.
   *
   * @param resourceVersion the latest resource version known to the list operation
   * @param remainedEmpty {@code true} if the cache was empty both before and after the list operation
   * @return the serial executor that processes handler notifications
   */
  public Executor onList(String resourceVersion, boolean remainedEmpty) {
    this.processor.distribute(l -> l.getHandler().onList(resourceVersion, remainedEmpty), false);
    return this.processor.getSerialExecutor();
  }

  public String getKey(T obj) {
    return cache.getKey(obj);
  }

  public void resync() {
    // lock to ensure the ordering wrt other events
    synchronized (cache.getLockObject()) {
      this.cache.list()
          .forEach(i -> this.processor.distribute(new ProcessorListener.UpdateNotification<>(i, i), true));
    }
  }

}
