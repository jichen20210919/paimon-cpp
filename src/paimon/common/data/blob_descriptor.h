/*
 * Copyright 2024-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"

namespace paimon {

class BlobDescriptor {
 public:
    static Result<std::unique_ptr<BlobDescriptor>> Create(const std::string& uri, int64_t offset,
                                                          int64_t length);

    ~BlobDescriptor() = default;

    static Result<std::unique_ptr<BlobDescriptor>> Deserialize(const char* buffer, uint64_t size);

    PAIMON_UNIQUE_PTR<Bytes> Serialize(const std::shared_ptr<MemoryPool>& pool) const;

    std::string ToString() const;

    const std::string& Uri() const {
        return uri_;
    }

    int64_t Offset() const {
        return offset_;
    }

    int64_t Length() const {
        return length_;
    }

 private:
    BlobDescriptor(const std::string& uri, int64_t offset, int64_t length)
        : uri_(uri), offset_(offset), length_(length) {}

 private:
    static constexpr int8_t CURRENT_VERSION = 1;

    const int8_t version_ = CURRENT_VERSION;
    std::string uri_;
    int64_t offset_ = 0;
    int64_t length_ = -1;
};

}  // namespace paimon
