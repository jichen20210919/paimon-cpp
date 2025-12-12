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

#include "paimon/format/column_stats.h"

#include <cstdint>
#include <memory>

#include "fmt/format.h"

namespace paimon {

template <typename T>
std::string FormatStatsToString(const T& stats) {
    auto to_str = [](auto opt) -> std::string { return opt ? fmt::format("{}", *opt) : "null"; };
    return fmt::format("min {}, max {}, null count {}", to_str(stats.Min()), to_str(stats.Max()),
                       to_str(stats.NullCount()));
}

/// A simple column statistics, supports the following stats.
///
/// <ul>
/// <li>min: the minimum value of the column
/// <li>max: the maximum value of the column
/// <li>null_count: the number of nulls
/// </ul>
template <typename T>
class InternalStatsImpl {
 public:
    InternalStatsImpl(const std::optional<T>& min, const std::optional<T>& max,
                      std::optional<int64_t> null_count)
        : min_(min), max_(max), null_count_(null_count) {}

    const std::optional<T>& Min() const {
        return min_;
    }

    const std::optional<T>& Max() const {
        return max_;
    }

    void Collect(const std::optional<T>& value) {
        if (value != std::nullopt) {
            if (max_ != std::nullopt) {
                if (max_.value() < value) {
                    max_ = value;
                }
            } else {
                max_ = value;
            }
            if (min_ != std::nullopt) {
                if (value < min_.value()) {
                    min_ = value;
                }
            } else {
                min_ = value;
            }
            if (null_count_ == std::nullopt) {
                null_count_ = 0;
            }
        } else {
            if (null_count_ != std::nullopt) {
                null_count_.value()++;
            } else {
                null_count_ = 1;
            }
        }
    }

    std::optional<int64_t> NullCount() const {
        return null_count_;
    }

 private:
    std::optional<T> min_;
    std::optional<T> max_;
    std::optional<int64_t> null_count_;
};

using InternalBooleanStats = InternalStatsImpl<bool>;
using InternalTinyIntStats = InternalStatsImpl<int8_t>;
using InternalSmallIntStats = InternalStatsImpl<int16_t>;
using InternalIntStats = InternalStatsImpl<int32_t>;
using InternalBigIntStats = InternalStatsImpl<int64_t>;
using InternalFloatStats = InternalStatsImpl<float>;
using InternalDoubleStats = InternalStatsImpl<double>;
using InternalStringStats = InternalStatsImpl<std::string>;
using InternalTimestampStats = InternalStatsImpl<Timestamp>;
using InternalDecimalStats = InternalStatsImpl<Decimal>;

class BooleanColumnStatsImpl : public BooleanColumnStats {
 public:
    explicit BooleanColumnStatsImpl(const InternalBooleanStats& stats) : stats_(stats) {}

    std::optional<bool> Min() const override {
        return stats_.Min();
    }

    std::optional<bool> Max() const override {
        return stats_.Max();
    }

    void Collect(std::optional<bool> value) override {
        stats_.Collect(value);
    }

    std::optional<int64_t> NullCount() const override {
        return stats_.NullCount();
    }

    std::string ToString() const override {
        return FormatStatsToString(*this);
    }

    FieldType GetFieldType() const override {
        return FieldType::BOOLEAN;
    }

 private:
    InternalBooleanStats stats_;
};

class TinyIntColumnStatsImpl : public TinyIntColumnStats {
 public:
    explicit TinyIntColumnStatsImpl(const InternalTinyIntStats& stats) : stats_(stats) {}

    std::optional<int8_t> Min() const override {
        return stats_.Min();
    }

    std::optional<int8_t> Max() const override {
        return stats_.Max();
    }

    void Collect(std::optional<int8_t> value) override {
        stats_.Collect(value);
    }

    std::optional<int64_t> NullCount() const override {
        return stats_.NullCount();
    }

    std::string ToString() const override {
        return FormatStatsToString(*this);
    }

    FieldType GetFieldType() const override {
        return FieldType::TINYINT;
    }

 private:
    InternalTinyIntStats stats_;
};

class SmallIntColumnStatsImpl : public SmallIntColumnStats {
 public:
    explicit SmallIntColumnStatsImpl(const InternalSmallIntStats& stats) : stats_(stats) {}

    std::optional<int16_t> Min() const override {
        return stats_.Min();
    }

    std::optional<int16_t> Max() const override {
        return stats_.Max();
    }

    void Collect(std::optional<int16_t> value) override {
        stats_.Collect(value);
    }

    std::optional<int64_t> NullCount() const override {
        return stats_.NullCount();
    }

    std::string ToString() const override {
        return FormatStatsToString(*this);
    }

    FieldType GetFieldType() const override {
        return FieldType::SMALLINT;
    }

 private:
    InternalSmallIntStats stats_;
};

class IntColumnStatsImpl : public IntColumnStats {
 public:
    explicit IntColumnStatsImpl(const InternalIntStats& stats) : stats_(stats) {}

    std::optional<int32_t> Min() const override {
        return stats_.Min();
    }

    std::optional<int32_t> Max() const override {
        return stats_.Max();
    }

    void Collect(std::optional<int32_t> value) override {
        stats_.Collect(value);
    }

    std::optional<int64_t> NullCount() const override {
        return stats_.NullCount();
    }

    std::string ToString() const override {
        return FormatStatsToString(*this);
    }

    FieldType GetFieldType() const override {
        return FieldType::INT;
    }

 private:
    InternalIntStats stats_;
};

class BigIntColumnStatsImpl : public BigIntColumnStats {
 public:
    explicit BigIntColumnStatsImpl(const InternalBigIntStats& stats) : stats_(stats) {}

    std::optional<int64_t> Min() const override {
        return stats_.Min();
    }

    std::optional<int64_t> Max() const override {
        return stats_.Max();
    }

    void Collect(std::optional<int64_t> value) override {
        stats_.Collect(value);
    }

    std::optional<int64_t> NullCount() const override {
        return stats_.NullCount();
    }

    std::string ToString() const override {
        return FormatStatsToString(*this);
    }

    FieldType GetFieldType() const override {
        return FieldType::BIGINT;
    }

 private:
    InternalBigIntStats stats_;
};

class FloatColumnStatsImpl : public FloatColumnStats {
 public:
    explicit FloatColumnStatsImpl(const InternalFloatStats& stats) : stats_(stats) {}

    std::optional<float> Min() const override {
        return stats_.Min();
    }

    std::optional<float> Max() const override {
        return stats_.Max();
    }

    void Collect(std::optional<float> value) override {
        stats_.Collect(value);
    }

    std::optional<int64_t> NullCount() const override {
        return stats_.NullCount();
    }

    std::string ToString() const override {
        return FormatStatsToString(*this);
    }

    FieldType GetFieldType() const override {
        return FieldType::FLOAT;
    }

 private:
    InternalFloatStats stats_;
};

class DoubleColumnStatsImpl : public DoubleColumnStats {
 public:
    explicit DoubleColumnStatsImpl(const InternalDoubleStats& stats) : stats_(stats) {}

    std::optional<double> Min() const override {
        return stats_.Min();
    }

    std::optional<double> Max() const override {
        return stats_.Max();
    }
    void Collect(std::optional<double> value) override {
        stats_.Collect(value);
    }

    std::optional<int64_t> NullCount() const override {
        return stats_.NullCount();
    }

    std::string ToString() const override {
        return FormatStatsToString(*this);
    }

    FieldType GetFieldType() const override {
        return FieldType::DOUBLE;
    }

 private:
    InternalDoubleStats stats_;
};

class StringColumnStatsImpl : public StringColumnStats {
 public:
    explicit StringColumnStatsImpl(const InternalStringStats& stats) : stats_(stats) {}

    const std::optional<std::string>& Min() const override {
        return stats_.Min();
    }

    const std::optional<std::string>& Max() const override {
        return stats_.Max();
    }

    void Collect(const std::optional<std::string>& value) override {
        stats_.Collect(value);
    }

    std::optional<int64_t> NullCount() const override {
        return stats_.NullCount();
    }

    std::string ToString() const override {
        return FormatStatsToString(*this);
    }

    FieldType GetFieldType() const override {
        return FieldType::STRING;
    }

 private:
    InternalStringStats stats_;
};

class TimestampColumnStatsImpl : public TimestampColumnStats {
 public:
    TimestampColumnStatsImpl(const InternalTimestampStats& stats, int32_t precision)
        : stats_(stats), precision_(precision) {}

    std::optional<Timestamp> Min() const override {
        return stats_.Min();
    }

    std::optional<Timestamp> Max() const override {
        return stats_.Max();
    }

    void Collect(const std::optional<Timestamp>& value) override {
        return stats_.Collect(value);
    }

    std::optional<int64_t> NullCount() const override {
        return stats_.NullCount();
    }

    std::string ToString() const override {
        return fmt::format("min {}, max {}, null count {}",
                           Min() ? Min().value().ToString() : "null",
                           Max() ? Max().value().ToString() : "null",
                           NullCount() ? std::to_string(NullCount().value()) : "null");
    }

    FieldType GetFieldType() const override {
        return FieldType::TIMESTAMP;
    }

    int32_t GetPrecision() const override {
        return precision_;
    }

 private:
    InternalTimestampStats stats_;
    int32_t precision_;
};

class DecimalColumnStatsImpl : public DecimalColumnStats {
 public:
    DecimalColumnStatsImpl(const InternalDecimalStats& stats, int32_t precision, int32_t scale)
        : stats_(stats), precision_(precision), scale_(scale) {}

    std::optional<Decimal> Min() const override {
        return stats_.Min();
    }

    std::optional<Decimal> Max() const override {
        return stats_.Max();
    }

    void Collect(const std::optional<Decimal>& value) override {
        return stats_.Collect(value);
    }

    std::optional<int64_t> NullCount() const override {
        return stats_.NullCount();
    }

    std::string ToString() const override {
        return fmt::format("min {}, max {}, null count {}",
                           Min() ? Min().value().ToString() : "null",
                           Max() ? Max().value().ToString() : "null",
                           NullCount() ? std::to_string(NullCount().value()) : "null");
    }

    FieldType GetFieldType() const override {
        return FieldType::DECIMAL;
    }

    int32_t GetPrecision() const override {
        return precision_;
    }

    int32_t GetScale() const override {
        return scale_;
    }

 private:
    InternalDecimalStats stats_;
    int32_t precision_;
    int32_t scale_;
};

class DateColumnStatsImpl : public DateColumnStats {
 public:
    explicit DateColumnStatsImpl(const InternalIntStats& stats) : stats_(stats) {}

    std::optional<int32_t> Min() const override {
        return stats_.Min();
    }

    std::optional<int32_t> Max() const override {
        return stats_.Max();
    }

    void Collect(std::optional<int32_t> value) override {
        stats_.Collect(value);
    }

    std::optional<int64_t> NullCount() const override {
        return stats_.NullCount();
    }

    std::string ToString() const override {
        return FormatStatsToString(*this);
    }

    FieldType GetFieldType() const override {
        return FieldType::DATE;
    }

 private:
    InternalIntStats stats_;
};

std::string NestedColumnStats::ToString() const {
    return fmt::format("min null, max null, null count {}",
                       NullCount() ? std::to_string(NullCount().value()) : "null");
}

FieldType NestedColumnStats::GetFieldType() const {
    return nested_type_;
}

std::unique_ptr<ColumnStats> ColumnStats::CreateBooleanColumnStats(
    std::optional<bool> min, std::optional<bool> max, std::optional<int64_t> null_count) {
    return std::make_unique<BooleanColumnStatsImpl>(InternalBooleanStats(min, max, null_count));
}

std::unique_ptr<ColumnStats> ColumnStats::CreateTinyIntColumnStats(
    std::optional<int8_t> min, std::optional<int8_t> max, std::optional<int64_t> null_count) {
    return std::make_unique<TinyIntColumnStatsImpl>(InternalTinyIntStats(min, max, null_count));
}

std::unique_ptr<ColumnStats> ColumnStats::CreateSmallIntColumnStats(
    std::optional<int16_t> min, std::optional<int16_t> max, std::optional<int64_t> null_count) {
    return std::make_unique<SmallIntColumnStatsImpl>(InternalSmallIntStats(min, max, null_count));
}

std::unique_ptr<ColumnStats> ColumnStats::CreateIntColumnStats(std::optional<int32_t> min,
                                                               std::optional<int32_t> max,
                                                               std::optional<int64_t> null_count) {
    return std::make_unique<IntColumnStatsImpl>(InternalIntStats(min, max, null_count));
}

std::unique_ptr<ColumnStats> ColumnStats::CreateBigIntColumnStats(
    std::optional<int64_t> min, std::optional<int64_t> max, std::optional<int64_t> null_count) {
    return std::make_unique<BigIntColumnStatsImpl>(InternalBigIntStats(min, max, null_count));
}

std::unique_ptr<ColumnStats> ColumnStats::CreateFloatColumnStats(
    std::optional<float> min, std::optional<float> max, std::optional<int64_t> null_count) {
    return std::make_unique<FloatColumnStatsImpl>(InternalFloatStats(min, max, null_count));
}

std::unique_ptr<ColumnStats> ColumnStats::CreateDoubleColumnStats(
    std::optional<double> min, std::optional<double> max, std::optional<int64_t> null_count) {
    return std::make_unique<DoubleColumnStatsImpl>(InternalDoubleStats(min, max, null_count));
}

std::unique_ptr<ColumnStats> ColumnStats::CreateStringColumnStats(
    const std::optional<std::string>& min, const std::optional<std::string>& max,
    std::optional<int64_t> null_count) {
    return std::make_unique<StringColumnStatsImpl>(InternalStringStats(min, max, null_count));
}

std::unique_ptr<ColumnStats> ColumnStats::CreateTimestampColumnStats(
    const std::optional<Timestamp>& min, const std::optional<Timestamp>& max,
    std::optional<int64_t> null_count, int32_t precision) {
    return std::make_unique<TimestampColumnStatsImpl>(InternalTimestampStats(min, max, null_count),
                                                      precision);
}

std::unique_ptr<ColumnStats> ColumnStats::CreateDecimalColumnStats(
    const std::optional<Decimal>& min, const std::optional<Decimal>& max,
    std::optional<int64_t> null_count, int32_t precision, int32_t scale) {
    return std::make_unique<DecimalColumnStatsImpl>(InternalDecimalStats(min, max, null_count),
                                                    precision, scale);
}

std::unique_ptr<ColumnStats> ColumnStats::CreateDateColumnStats(std::optional<int32_t> min,
                                                                std::optional<int32_t> max,
                                                                std::optional<int64_t> null_count) {
    return std::make_unique<DateColumnStatsImpl>(InternalIntStats(min, max, null_count));
}

std::unique_ptr<ColumnStats> ColumnStats::CreateNestedColumnStats(
    const FieldType& nested_type, std::optional<int64_t> null_count) {
    return std::make_unique<NestedColumnStats>(nested_type, null_count);
}

}  // namespace paimon
