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
#include <string>
#include <vector>

#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {

/// MemorySize is a representation of a number of bytes, viewable in different units.
///
/// <h2>Parsing</h2>
///
/// The size can be parsed from a text expression. If the expression is a pure number, the value
/// will be interpreted as bytes.
class PAIMON_EXPORT MemorySize {
 public:
    /// class which defines memory unit, mostly used to parse value from configuration file.
    ///
    /// To make larger values more compact, the common size suffixes are supported:
    ///
    /// <ul>
    /// <li>1b or 1bytes (bytes)
    /// <li>1k or 1kb or 1kibibytes (interpreted as kibibytes = 1024 bytes)
    /// <li>1m or 1mb or 1mebibytes (interpreted as mebibytes = 1024 kibibytes)
    /// <li>1g or 1gb or 1gibibytes (interpreted as gibibytes = 1024 mebibytes)
    /// <li>1t or 1tb or 1tebibytes (interpreted as tebibytes = 1024 gibibytes)
    /// </ul>
    class MemoryUnit {
     public:
        MemoryUnit(const std::vector<std::string>& units, int64_t multiplier)
            : units_(units), multiplier_(multiplier) {}

        const std::vector<std::string>& GetUnits() const {
            return units_;
        }

        int64_t GetMultiplier() const {
            return multiplier_;
        }

     private:
        std::vector<std::string> units_;
        int64_t multiplier_;
    };

    static const MemoryUnit& Bytes();
    static const MemoryUnit& KiloBytes();
    static const MemoryUnit& MegaBytes();
    static const MemoryUnit& GigaBytes();
    static const MemoryUnit& TeraBytes();

    static Result<int64_t> ParseBytes(const std::string& text);
    static Result<MemoryUnit> ParseUnit(const std::string& unit);
    static bool MatchesAny(const std::string& str, const MemorySize::MemoryUnit& unit);

    MemorySize() = delete;
    ~MemorySize() = delete;
};

}  // namespace paimon
