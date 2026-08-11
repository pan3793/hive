#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#
import sys, re
import datetime
import os

try:
  stdin = sys.stdin.buffer
  stdout = sys.stdout.buffer
  stderr = sys.stderr.buffer
except AttributeError:
  # Python 2: sys.stdin/stdout/stderr are already byte-oriented.
  stdin = sys.stdin
  stdout = sys.stdout
  stderr = sys.stderr

table_name=None
if 'hive_streaming_tablename' in os.environ:
  table_name=os.environ['hive_streaming_tablename']

for line in stdin:
  stdout.write(line)
  stderr.write(b"dummy\n")
