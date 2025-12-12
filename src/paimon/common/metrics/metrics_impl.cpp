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

#include "paimon/common/metrics/metrics_impl.h"

#include <utility>

#include "fmt/format.h"
#include "paimon/result.h"
#include "rapidjson/document.h"
#include "rapidjson/encodings.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace paimon {
void MetricsImpl::SetCounter(const std::string& metric_name, uint64_t metric_value) {
    std::lock_guard<std::mutex> guard(counter_lock_);
    counters_[metric_name] = metric_value;
}

Result<uint64_t> MetricsImpl::GetCounter(const std::string& metric_name) const {
    std::lock_guard<std::mutex> guard(counter_lock_);
    auto iter = counters_.find(metric_name);
    if (iter != counters_.end()) {
        return iter->second;
    }
    return Status::KeyError(fmt::format("metric '{}' not found", metric_name));
}

std::map<std::string, uint64_t> MetricsImpl::GetAllCounters() const {
    std::lock_guard<std::mutex> guard(counter_lock_);
    return counters_;
}

void MetricsImpl::Merge(const std::shared_ptr<Metrics>& other) {
    if (other && this != other.get()) {
        std::map<std::string, uint64_t> other_counters = other->GetAllCounters();
        for (const auto& kv : other_counters) {
            std::lock_guard<std::mutex> guard(counter_lock_);
            auto iter = counters_.find(kv.first);
            if (iter == counters_.end()) {
                counters_[kv.first] = kv.second;
            } else {
                counters_[kv.first] += kv.second;
            }
        }
    }
}

void MetricsImpl::Overwrite(const std::shared_ptr<Metrics>& other) {
    if (other && this != other.get()) {
        std::map<std::string, uint64_t> other_counters = other->GetAllCounters();
        std::lock_guard<std::mutex> guard(counter_lock_);
        counters_.swap(other_counters);
    }
}

std::string MetricsImpl::ToString() const {
    using RapidWriter =
        rapidjson::Writer<rapidjson::StringBuffer, rapidjson::UTF8<>, rapidjson::ASCII<>>;
    rapidjson::Document doc;
    doc.SetObject();
    rapidjson::Document::AllocatorType& allocator = doc.GetAllocator();

    std::map<std::string, uint64_t> counters = GetAllCounters();
    for (const auto& kv : counters) {
        doc.AddMember(rapidjson::Value(kv.first, allocator), rapidjson::Value(kv.second),
                      allocator);
    }
    rapidjson::StringBuffer s;
    RapidWriter writer(s);
    doc.Accept(writer);
    return s.GetString();
}

}  // namespace paimon
