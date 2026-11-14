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

package io.dapr.actors.runtime;

/** Internal actor state response containing the bytes and their read ETag. */
final class ActorStateValue {
  private final byte[] data;
  private final String etag;

  ActorStateValue(byte[] data, String etag) {
    this.data = data;
    this.etag = etag;
  }

  byte[] getData() {
    return data;
  }

  String getETag() {
    return etag;
  }
}
