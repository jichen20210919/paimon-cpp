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

#include "paimon/reader/batch_reader.h"

#include "arrow/c/abi.h"
#include "paimon/common/reader/reader_utils.h"

namespace paimon {
BatchReader::ReadBatch BatchReader::MakeEofBatch() {
    return std::make_pair(std::unique_ptr<ArrowArray>(), std::unique_ptr<ArrowSchema>());
}
BatchReader::ReadBatchWithBitmap BatchReader::MakeEofBatchWithBitmap() {
    return std::make_pair(MakeEofBatch(), RoaringBitmap32());
}

bool BatchReader::IsEofBatch(const ReadBatch& batch) {
    return batch.first == nullptr;
}

bool BatchReader::IsEofBatch(const ReadBatchWithBitmap& batch_with_bitmap) {
    return batch_with_bitmap.first.first == nullptr;
}

Result<BatchReader::ReadBatchWithBitmap> BatchReader::NextBatchWithBitmap() {
    PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, NextBatch());
    return ReaderUtils::AddAllValidBitmap(std::move(batch));
}

}  // namespace paimon
