/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * ObjectCacheFactory returns the appropriate cache depending on settings in
 * the hive conf.
 */
public class ObjectCacheFactory {
  private static final Logger LOG = LoggerFactory.getLogger(ObjectCacheFactory.class);

  private ObjectCacheFactory() {
    // avoid instantiation
  }

  /**
   * Returns the appropriate cache
   */
  public static ObjectCache getCache(Configuration conf, String queryId, boolean isPlanCache) {
    return new ObjectCacheWrapper(new org.apache.hadoop.hive.ql.exec.mr.ObjectCache(), queryId);
  }
}
