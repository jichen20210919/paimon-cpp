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

#include "paimon/global_index/global_index_result.h"

#include "paimon/global_index/bitmap_global_index_result.h"
namespace paimon {
Result<std::shared_ptr<GlobalIndexResult>> GlobalIndexResult::And(
    const std::shared_ptr<GlobalIndexResult>& other) {
    auto supplier = [other, result = shared_from_this()]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<GlobalIndexResult::Iterator> iter1,
                               result->CreateIterator());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<GlobalIndexResult::Iterator> iter2,
                               other->CreateIterator());
        RoaringBitmap64 bitmap1;
        while (iter1->HasNext()) {
            bitmap1.Add(iter1->Next());
        }
        RoaringBitmap64 bitmap2;
        while (iter2->HasNext()) {
            bitmap2.Add(iter2->Next());
        }
        bitmap1 &= bitmap2;
        return bitmap1;
    };
    return std::make_shared<BitmapGlobalIndexResult>(supplier);
}

Result<std::shared_ptr<GlobalIndexResult>> GlobalIndexResult::Or(
    const std::shared_ptr<GlobalIndexResult>& other) {
    auto supplier = [other, result = shared_from_this()]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<GlobalIndexResult::Iterator> iter1,
                               result->CreateIterator());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<GlobalIndexResult::Iterator> iter2,
                               other->CreateIterator());
        RoaringBitmap64 bitmap;
        while (iter1->HasNext()) {
            bitmap.Add(iter1->Next());
        }
        while (iter2->HasNext()) {
            bitmap.Add(iter2->Next());
        }
        return bitmap;
    };
    return std::make_shared<BitmapGlobalIndexResult>(supplier);
}
}  // namespace paimon
