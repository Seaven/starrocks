// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>

#include "common/logging.h"
#include "util/byte_buffer.h"

namespace starrocks {

class ByteBufferTest : public testing::Test {
public:
    ByteBufferTest() = default;
    ~ByteBufferTest() override = default;
};

TEST_F(ByteBufferTest, normal) {
    auto buf = ByteBuffer::allocate_with_tracker(4).value();
    ASSERT_EQ(0, buf->pos);
    ASSERT_EQ(4, buf->limit);
    ASSERT_EQ(4, buf->capacity);

    char test[] = {1, 2, 3};
    buf->put_bytes(test, 3);

    ASSERT_EQ(3, buf->pos);
    ASSERT_EQ(4, buf->limit);
    ASSERT_EQ(4, buf->capacity);

    ASSERT_EQ(1, buf->remaining());
    buf->flip();
    ASSERT_EQ(0, buf->pos);
    ASSERT_EQ(3, buf->limit);
    ASSERT_EQ(4, buf->capacity);
    ASSERT_EQ(3, buf->remaining());
}

} // namespace starrocks
