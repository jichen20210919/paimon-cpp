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

#include "paimon/predicate/compound_predicate.h"

#include <cassert>
#include <cstddef>
#include <utility>

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/predicate/compound_function.h"
#include "paimon/common/predicate/compound_predicate_impl.h"
#include "paimon/predicate/function.h"

namespace paimon {
CompoundPredicate::CompoundPredicate(const CompoundFunction& compound_function,
                                     const std::vector<std::shared_ptr<Predicate>>& children)
    : compound_function_(compound_function), children_(children) {}

const Function& CompoundPredicate::GetFunction() const {
    return compound_function_;
}

std::shared_ptr<Predicate> CompoundPredicate::Negate() const {
    const auto& negate_func = compound_function_.Negate();
    std::vector<std::shared_ptr<Predicate>> negated_children;
    negated_children.reserve(children_.size());
    for (const auto& child : children_) {
        auto negated_child = child->Negate();
        if (!negated_child) {
            return nullptr;
        }
        negated_children.push_back(std::move(negated_child));
    }
    return std::make_shared<CompoundPredicateImpl>(negate_func, negated_children);
}

bool CompoundPredicate::operator==(const Predicate& other) const {
    if (this == &other) {
        return true;
    }
    auto compound_predicate = dynamic_cast<CompoundPredicate*>(const_cast<Predicate*>(&other));
    if (!compound_predicate) {
        return false;
    }
    if (GetFunction().GetType() != compound_predicate->GetFunction().GetType()) {
        return false;
    }
    const auto& children = Children();
    const auto& other_children = compound_predicate->Children();
    if (children.size() != other_children.size()) {
        return false;
    }
    for (size_t i = 0; i < children.size(); ++i) {
        if (*children[i] == *other_children[i]) {
            continue;
        } else {
            return false;
        }
    }
    return true;
}

std::string CompoundPredicate::ToString() const {
    std::vector<std::string> children_str;
    children_str.reserve(children_.size());
    for (const auto& child : children_) {
        assert(child);
        children_str.emplace_back(child->ToString());
    }
    return fmt::format("{}([{}])", compound_function_.ToString(), fmt::join(children_str, ", "));
}

}  // namespace paimon
