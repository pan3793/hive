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
package org.apache.hadoop.hive.ql.optimizer.calcite.translator;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

public class ASTBuilder {

  public static ASTBuilder construct(int tokenType, String text) {
    ASTBuilder b = new ASTBuilder();
    b.curr = createAST(tokenType, text);
    return b;
  }

  public static ASTNode createAST(int tokenType, String text) {
    return (ASTNode) ParseDriver.adaptor.create(tokenType, text);
  }

  ASTNode curr;

  public ASTNode node() {
    return curr;
  }

  public ASTBuilder add(int tokenType, String text) {
    ParseDriver.adaptor.addChild(curr, createAST(tokenType, text));
    return this;
  }

  public ASTBuilder add(ASTBuilder b) {
    ParseDriver.adaptor.addChild(curr, b.curr);
    return this;
  }

  public ASTBuilder add(ASTNode n) {
    if (n != null) {
      ParseDriver.adaptor.addChild(curr, n);
    }
    return this;
  }
}
