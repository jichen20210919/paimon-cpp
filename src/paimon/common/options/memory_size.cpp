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

#include "paimon/common/options/memory_size.h"

#include <cstddef>
#include <limits>
#include <optional>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

const MemorySize::MemoryUnit& MemorySize::Bytes() {
    static const MemoryUnit kBytes{{"b", "bytes"}, 1L};
    return kBytes;
}

const MemorySize::MemoryUnit& MemorySize::KiloBytes() {
    static const MemoryUnit kKiloBytes{{"k", "kb", "kibibytes"}, 1024L};
    return kKiloBytes;
}

const MemorySize::MemoryUnit& MemorySize::MegaBytes() {
    static const MemoryUnit kMegaBytes{{"m", "mb", "mebibytes"}, 1024L * 1024L};
    return kMegaBytes;
}

const MemorySize::MemoryUnit& MemorySize::GigaBytes() {
    static const MemoryUnit kGigaBytes{{"g", "gb", "gibibytes"}, 1024L * 1024L * 1024L};
    return kGigaBytes;
}

const MemorySize::MemoryUnit& MemorySize::TeraBytes() {
    static const MemoryUnit kTeraBytes{{"t", "tb", "tebibytes"}, 1024L * 1024L * 1024L * 1024L};
    return kTeraBytes;
}

Result<int64_t> MemorySize::ParseBytes(const std::string& text) {
    std::string trimmed = text;
    StringUtils::Trim(&trimmed);
    if (trimmed.empty()) {
        return Status::Invalid("argument is an empty or whitespace-only string");
    }
    const size_t len = trimmed.length();
    size_t pos = 0;

    char current;
    while (pos < len) {
        current = trimmed[pos];
        if (current >= '0' && current <= '9') {
            pos++;
        } else {
            break;
        }
    }
    std::string number = trimmed.substr(0, pos);
    if (number.empty()) {
        return Status::Invalid("text does not start with a number");
    }
    std::string raw_unit = trimmed.substr(pos);
    StringUtils::Trim(&raw_unit);
    std::string unit = StringUtils::ToLowerCase(raw_unit);

    std::optional<int64_t> value = StringUtils::StringToValue<int64_t>(number);
    if (value == std::nullopt) {
        return Status::Invalid(fmt::format(
            "The value '{}' cannot be represented as 64bit number (numeric overflow).", number));
    }
    PAIMON_ASSIGN_OR_RAISE(MemoryUnit parsed_unit, ParseUnit(unit));
    int64_t multiplier = parsed_unit.GetMultiplier();

    int64_t maximum = std::numeric_limits<int64_t>::max() / multiplier;
    // check for overflow
    if (value.value() > maximum) {
        return Status::Invalid(fmt::format(
            "The value '{}' cannot be represented as 64bit number of bytes (numeric overflow).",
            text));
    }
    return value.value() * multiplier;
}

Result<MemorySize::MemoryUnit> MemorySize::ParseUnit(const std::string& unit) {
    if (MatchesAny(unit, Bytes())) {
        return Bytes();
    } else if (MatchesAny(unit, KiloBytes())) {
        return KiloBytes();
    } else if (MatchesAny(unit, MegaBytes())) {
        return MegaBytes();
    } else if (MatchesAny(unit, GigaBytes())) {
        return GigaBytes();
    } else if (MatchesAny(unit, TeraBytes())) {
        return TeraBytes();
    } else if (!unit.empty()) {
        return Status::Invalid(
            fmt::format("Memory size unit '{}' does not match any of the recognized units", unit));
    }
    return Bytes();
}

bool MemorySize::MatchesAny(const std::string& str, const MemorySize::MemoryUnit& unit) {
    for (const auto& u : unit.GetUnits()) {
        if (u == str) {
            return true;
        }
    }
    return false;
}

}  // namespace paimon
