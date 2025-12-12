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

namespace paimon {
/// Provide type specialized setters to reduce if/else and eliminate box and unbox. This is
/// mainly used on the binary format such as `BinaryRow`.
class DataSetters {
 public:
    virtual void SetNullAt(int32_t pos) = 0;

    virtual void SetBoolean(int32_t pos, bool value) = 0;

    virtual void SetByte(int32_t pos, char value) = 0;

    virtual void SetShort(int32_t pos, int16_t value) = 0;

    virtual void SetInt(int32_t pos, int32_t value) = 0;

    virtual void SetLong(int32_t pos, int64_t value) = 0;

    virtual void SetFloat(int32_t pos, float value) = 0;

    virtual void SetDouble(int32_t pos, double value) = 0;
};

}  // namespace paimon
