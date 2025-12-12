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

#include "paimon/file_index/file_index_result.h"

#include <cassert>
#include <utility>

namespace paimon {
std::shared_ptr<FileIndexResult> FileIndexResult::Remain() {
    static std::shared_ptr<FileIndexResult> remain = std::make_shared<class Remain>();
    return remain;
}

std::shared_ptr<FileIndexResult> FileIndexResult::Skip() {
    static std::shared_ptr<FileIndexResult> skip = std::make_shared<class Skip>();
    return skip;
}

Result<std::shared_ptr<FileIndexResult>> FileIndexResult::And(
    const std::shared_ptr<FileIndexResult>& other) {
    assert(other);
    PAIMON_ASSIGN_OR_RAISE(bool other_remain, other->IsRemain());
    if (other_remain) {
        return shared_from_this();
    } else {
        return FileIndexResult::Skip();
    }
}

Result<std::shared_ptr<FileIndexResult>> FileIndexResult::Or(
    const std::shared_ptr<FileIndexResult>& other) {
    assert(other);
    PAIMON_ASSIGN_OR_RAISE(bool other_remain, other->IsRemain());
    if (other_remain) {
        return FileIndexResult::Remain();
    } else {
        return shared_from_this();
    }
}

Result<bool> Remain::IsRemain() const {
    return true;
}

Result<std::shared_ptr<FileIndexResult>> Remain::And(
    const std::shared_ptr<FileIndexResult>& other) {
    return other;
}

Result<std::shared_ptr<FileIndexResult>> Remain::Or(const std::shared_ptr<FileIndexResult>& other) {
    return shared_from_this();
}

std::string Remain::ToString() const {
    return "REMAIN";
}

Result<bool> Skip::IsRemain() const {
    return false;
}

Result<std::shared_ptr<FileIndexResult>> Skip::And(const std::shared_ptr<FileIndexResult>& other) {
    return shared_from_this();
}

Result<std::shared_ptr<FileIndexResult>> Skip::Or(const std::shared_ptr<FileIndexResult>& other) {
    return other;
}

std::string Skip::ToString() const {
    return "SKIP";
}

}  // namespace paimon
