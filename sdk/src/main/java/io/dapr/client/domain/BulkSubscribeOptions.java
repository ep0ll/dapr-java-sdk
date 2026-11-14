/*
 * Copyright 2026 The Dapr Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dapr.client.domain;

/** Options to enable batched delivery for a streaming pub/sub subscription. */
public final class BulkSubscribeOptions {
  private final boolean enabled;
  private final Integer maxMessagesCount;
  private final Integer maxAwaitDurationMs;

  public BulkSubscribeOptions(boolean enabled, Integer maxMessagesCount, Integer maxAwaitDurationMs) {
    if (maxMessagesCount != null && maxMessagesCount <= 0) {
      throw new IllegalArgumentException("maxMessagesCount must be greater than zero");
    }
    if (maxAwaitDurationMs != null && maxAwaitDurationMs <= 0) {
      throw new IllegalArgumentException("maxAwaitDurationMs must be greater than zero");
    }
    this.enabled = enabled;
    this.maxMessagesCount = maxMessagesCount;
    this.maxAwaitDurationMs = maxAwaitDurationMs;
  }

  public boolean isEnabled() { return enabled; }
  public Integer getMaxMessagesCount() { return maxMessagesCount; }
  public Integer getMaxAwaitDurationMs() { return maxAwaitDurationMs; }
}
