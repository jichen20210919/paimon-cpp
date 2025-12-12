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

#include "paimon/common/utils/delta_varint_compressor.h"

#include <cstdio>
#include <fstream>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <utility>

#include "gtest/gtest.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(DeltaVarintCompressorTest, TestCompatibleWithJava) {
    std::shared_ptr<FileSystem> fs = std::make_shared<LocalFileSystem>();

    for (int32_t i = 0; i < 5; ++i) {
        std::string file_prefix = paimon::test::GetDataDir() +
                                  "/delta_varint_compressor.data/case-000" + std::to_string(i);

        // Read original data from text file
        std::string original_file = file_prefix + ".txt";
        std::ifstream in(original_file);
        std::vector<int64_t> original;
        int64_t value;
        while (in >> value) {
            original.push_back(value);
        }
        auto compressed = DeltaVarintCompressor::Compress(original);

        // Read expected compressed bytes from binary file
        std::string binary_file = file_prefix + ".bin";
        std::string expected_bytes;
        ASSERT_OK(fs->ReadFile(binary_file, &expected_bytes));
        ASSERT_EQ(expected_bytes, std::string(compressed.data(), compressed.size()));

        // Decompress and verify
        ASSERT_OK_AND_ASSIGN(auto decompressed, DeltaVarintCompressor::Decompress(compressed));
        ASSERT_EQ(original, decompressed);
    }
}

/// Test cases from Java Paimon
// Test case for normal compression and decompression
TEST(DeltaVarintCompressorTest, TestNormalCase1) {
    std::vector<int64_t> original = {80, 50, 90, 80, 70};
    auto compressed = DeltaVarintCompressor::Compress(original);
    ASSERT_OK_AND_ASSIGN(auto decompressed, DeltaVarintCompressor::Decompress(compressed));

    ASSERT_EQ(original, decompressed);
    ASSERT_EQ(6u, compressed.size());
}

TEST(DeltaVarintCompressorTest, TestNormalCase2) {
    std::vector<int64_t> original = {100, 50, 150, 100, 200};
    auto compressed = DeltaVarintCompressor::Compress(original);
    ASSERT_OK_AND_ASSIGN(auto decompressed, DeltaVarintCompressor::Decompress(compressed));

    ASSERT_EQ(original, decompressed);
    ASSERT_EQ(8u, compressed.size());
}

TEST(DeltaVarintCompressorTest, TestRandomRoundTrip) {
    std::mt19937 gen(123456789);
    std::uniform_int_distribution<uint64_t> dist;

    for (int32_t iter = 0; iter < 10000; ++iter) {
        std::vector<int64_t> original;
        for (int32_t i = 0; i < 100; ++i) {
            original.push_back(static_cast<int64_t>(dist(gen)));
        }

        auto compressed = DeltaVarintCompressor::Compress(original);
        ASSERT_OK_AND_ASSIGN(auto decompressed, DeltaVarintCompressor::Decompress(compressed));

        ASSERT_EQ(original, decompressed);
    }
}

// Test case for empty array
TEST(DeltaVarintCompressorTest, TestEmptyArray) {
    std::vector<int64_t> original;
    auto compressed = DeltaVarintCompressor::Compress(original);
    ASSERT_OK_AND_ASSIGN(auto decompressed, DeltaVarintCompressor::Decompress(compressed));

    ASSERT_EQ(original, decompressed);
    ASSERT_EQ(0u, compressed.size());
}

// Test case for single-element array
TEST(DeltaVarintCompressorTest, TestSingleElement) {
    std::vector<int64_t> original = {42};
    auto compressed = DeltaVarintCompressor::Compress(original);
    ASSERT_OK_AND_ASSIGN(auto decompressed, DeltaVarintCompressor::Decompress(compressed));

    ASSERT_EQ(original, decompressed);
    // Calculate expected size: Varint encoding for 42 (0x2A -> 1 byte)
    ASSERT_EQ(1u, compressed.size());
}

// Test case for extreme values (INT64.MIN_VALUE and MAX_VALUE)
TEST(DeltaVarintCompressorTest, TestExtremeValues) {
    std::vector<int64_t> original(
        {std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max()});
    auto compressed = DeltaVarintCompressor::Compress(original);
    ASSERT_OK_AND_ASSIGN(auto decompressed, DeltaVarintCompressor::Decompress(compressed));

    ASSERT_EQ(original, decompressed);
    // Expected size: 10 bytes (MIN_VALUE) + 1 bytes (delta overflow) = 11 bytes
    ASSERT_EQ(11u, compressed.size());
}

// Test case for negative deltas with ZigZag optimization
TEST(DeltaVarintCompressorTest, TestNegativeDeltas) {
    std::vector<int64_t> original = {100, -50, -150, -100};
    auto compressed = DeltaVarintCompressor::Compress(original);
    ASSERT_OK_AND_ASSIGN(auto decompressed, DeltaVarintCompressor::Decompress(compressed));

    ASSERT_EQ(original, decompressed);
    // Verify ZigZag optimization: -1 → 1 (1 byte)
    // Delta sequence: [100, -150, -100, 50] → ZigZag → Each encoded in 1-2 bytes
    ASSERT_LE(compressed.size(), 8u);
}

// Test case for unsorted data (worse compression ratio)
TEST(DeltaVarintCompressorTest, TestUnsortedData) {
    std::vector<int64_t> original = {1000, 5, 9999, 12345, 6789};
    auto compressed = DeltaVarintCompressor::Compress(original);
    ASSERT_OK_AND_ASSIGN(auto decompressed, DeltaVarintCompressor::Decompress(compressed));

    ASSERT_EQ(original, decompressed);
    // Larger deltas → more bytes (e.g., 9994 → 3 bytes)
    ASSERT_GT(compressed.size(), 5u);  // Worse than sorted case
}

// Test case for corrupted input (invalid Varint)
TEST(DeltaVarintCompressorTest, TestCorruptedInput) {
    std::vector<char> corrupted = {static_cast<char>(0x80), static_cast<char>(0x80),
                                   static_cast<char>(0x80)};
    ASSERT_NOK(DeltaVarintCompressor::Decompress(corrupted));
}

/// Test cases from Python Paimon
// Test case for arrays with zero values
TEST(DeltaVarintCompressorTest, TestZeroValues) {
    std::vector<int64_t> original = {0, 0, 0, 0, 0};
    auto compressed = DeltaVarintCompressor::Compress(original);
    ASSERT_OK_AND_ASSIGN(auto decompressed, DeltaVarintCompressor::Decompress(compressed));

    ASSERT_EQ(original, decompressed);
    // All deltas are 0, so should compress very well
    ASSERT_LE(compressed.size(), 5u);
}

// Test case for ascending sequence (optimal for delta compression)
TEST(DeltaVarintCompressorTest, TestAscendingSequence) {
    std::vector<int64_t> original = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    auto compressed = DeltaVarintCompressor::Compress(original);
    ASSERT_OK_AND_ASSIGN(auto decompressed, DeltaVarintCompressor::Decompress(compressed));

    ASSERT_EQ(original, decompressed);
    // All deltas are 1, so should compress very well
    ASSERT_LE(compressed.size(), 15u);  // Much smaller than original
}

// Test case for descending sequence
TEST(DeltaVarintCompressorTest, TestDescendingSequence) {
    std::vector<int64_t> original = {10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
    auto compressed = DeltaVarintCompressor::Compress(original);
    ASSERT_OK_AND_ASSIGN(auto decompressed, DeltaVarintCompressor::Decompress(compressed));

    ASSERT_EQ(original, decompressed);
    // All deltas are -1, should still compress well with ZigZag
    ASSERT_LE(compressed.size(), 15u);
}

// Test case for large positive values
TEST(DeltaVarintCompressorTest, TestLargePositiveValues) {
    std::vector<int64_t> original = {1000000, 2000000, 3000000, 4000000};
    auto compressed = DeltaVarintCompressor::Compress(original);
    ASSERT_OK_AND_ASSIGN(auto decompressed, DeltaVarintCompressor::Decompress(compressed));

    ASSERT_EQ(original, decompressed);
    // Large values but consistent deltas should still compress reasonably
    ASSERT_GT(compressed.size(), 4u);  // Will be larger due to big numbers
}

// Test case for mixed positive and negative values
TEST(DeltaVarintCompressorTest, TestMixedPositiveNegative) {
    std::vector<int64_t> original = {100, -200, 300, -400, 500};
    auto compressed = DeltaVarintCompressor::Compress(original);
    ASSERT_OK_AND_ASSIGN(auto decompressed, DeltaVarintCompressor::Decompress(compressed));

    ASSERT_EQ(original, decompressed);
    // Mixed signs create larger deltas
    ASSERT_GT(compressed.size(), 5u);
}

// Test that compression actually reduces size for suitable data
TEST(DeltaVarintCompressorTest, TestCompressionEfficiency) {
    // Create a sequence with small deltas
    std::vector<int64_t> original;
    std::mt19937 gen(42);  // Fixed seed for reproducibility
    std::uniform_int_distribution<int32_t> delta_dist(-10, 10);

    int64_t base = 1000;
    for (int32_t i = 0; i < 100; ++i) {
        base += delta_dist(gen);  // Small deltas
        original.push_back(base);
    }

    auto compressed = DeltaVarintCompressor::Compress(original);
    ASSERT_OK_AND_ASSIGN(auto decompressed, DeltaVarintCompressor::Decompress(compressed));

    ASSERT_EQ(original, decompressed);
    // For small deltas, compression should be effective
    // Original would need 8 bytes per int64_t (800 bytes), compressed should be much smaller
    ASSERT_LT(compressed.size(), original.size() * 4);  // At least 50% compression
}

// Test that multiple compress/decompress cycles are consistent
TEST(DeltaVarintCompressorTest, TestRoundTripConsistency) {
    std::vector<int64_t> original = {1, 10, 100, 1000, 10000};

    // First round trip
    auto compressed1 = DeltaVarintCompressor::Compress(original);
    ASSERT_OK_AND_ASSIGN(auto decompressed1, DeltaVarintCompressor::Decompress(compressed1));

    // Second round trip
    auto compressed2 = DeltaVarintCompressor::Compress(decompressed1);
    ASSERT_OK_AND_ASSIGN(auto decompressed2, DeltaVarintCompressor::Decompress(compressed2));

    // All should be identical
    ASSERT_EQ(original, decompressed1);
    ASSERT_EQ(original, decompressed2);
    ASSERT_EQ(compressed1, compressed2);
}

// Test boundary values for varint encoding
TEST(DeltaVarintCompressorTest, TestBoundaryValues) {
    // Test values around varint boundaries (127, 16383, etc.)
    std::vector<int64_t> boundary_values = {0,     1,      127,    128,    255,   256,  16383,
                                            16384, 32767,  32768,  -1,     -127,  -128, -255,
                                            -256,  -16383, -16384, -32767, -32768};

    auto compressed = DeltaVarintCompressor::Compress(boundary_values);
    ASSERT_OK_AND_ASSIGN(auto decompressed, DeltaVarintCompressor::Decompress(compressed));
    ASSERT_EQ(boundary_values, decompressed);
}

// Test ZigZag encoding compatibility with Java implementation
TEST(DeltaVarintCompressorTest, TestJavaCompatibilityZigZagEncoding) {
    // Test cases that verify ZigZag encoding matches Java's implementation
    // ZigZag mapping: 0->0, -1->1, 1->2, -2->3, 2->4, -3->5, 3->6, etc.
    std::vector<std::pair<int64_t, uint64_t>> zigzag_test_cases = {
        {0, 0},      // 0 -> 0
        {-1, 1},     // -1 -> 1
        {1, 2},      // 1 -> 2
        {-2, 3},     // -2 -> 3
        {2, 4},      // 2 -> 4
        {-3, 5},     // -3 -> 5
        {3, 6},      // 3 -> 6
        {-64, 127},  // -64 -> 127
        {64, 128},   // 64 -> 128
        {-65, 129},  // -65 -> 129
    };

    for (const auto& [original_value, expected_zigzag] : zigzag_test_cases) {
        // Test single value compression to verify ZigZag encoding
        std::vector<int64_t> single_value = {original_value};
        auto compressed = DeltaVarintCompressor::Compress(single_value);
        ASSERT_OK_AND_ASSIGN(auto decompressed, DeltaVarintCompressor::Decompress(compressed));

        ASSERT_EQ(single_value, decompressed)
            << "ZigZag encoding failed for value " << original_value;
        ASSERT_EQ(expected_zigzag, DeltaVarintCompressor::ZigZag(original_value))
            << "ZigZag encoding failed for value " << original_value;
    }
}

// Test with known test vectors that should match Java implementation
TEST(DeltaVarintCompressorTest, TestJavaCompatibilityKnownVectors) {
    // Test vectors with expected compressed output (hexadecimal)
    std::vector<std::pair<std::vector<int64_t>, std::string>> test_vectors = {
        // Simple cases
        {{0}, "00"},   // 0 -> ZigZag(0) = 0 -> Varint(0) = 0x00
        {{1}, "02"},   // 1 -> ZigZag(1) = 2 -> Varint(2) = 0x02
        {{-1}, "01"},  // -1 -> ZigZag(-1) = 1 -> Varint(1) = 0x01
        {{2}, "04"},   // 2 -> ZigZag(2) = 4 -> Varint(4) = 0x04
        {{-2}, "03"},  // -2 -> ZigZag(-2) = 3 -> Varint(3) = 0x03

        // Delta encoding cases
        {{0, 1}, "0002"},   // [0, 1] -> [0, delta=1] -> [0x00, 0x02]
        {{1, 2}, "0202"},   // [1, 2] -> [1, delta=1] -> [0x02, 0x02]
        {{0, -1}, "0001"},  // [0, -1] -> [0, delta=-1] -> [0x00, 0x01]
        {{1, 0}, "0201"},   // [1, 0] -> [1, delta=-1] -> [0x02, 0x01]

        // Larger values
        {{127}, "fe01"},   // 127 -> ZigZag(127) = 254 -> Varint(254) = 0xfe01
        {{-127}, "fd01"},  // -127 -> ZigZag(-127) = 253 -> Varint(253) = 0xfd01
        {{128}, "8002"},   // 128 -> ZigZag(128) = 256 -> Varint(256) = 0x8002
        {{-128}, "ff01"},  // -128 -> ZigZag(-128) = 255 -> Varint(255) = 0xff01
    };

    auto bytes_to_hex = [](const std::vector<char>& bytes) -> std::string {
        std::string hex;
        for (char byte : bytes) {
            char buf[3];
            snprintf(buf, sizeof(buf), "%02x", static_cast<unsigned char>(byte));
            hex += buf;
        }
        return hex;
    };

    for (const auto& [original, expected_hex] : test_vectors) {
        auto compressed = DeltaVarintCompressor::Compress(original);
        std::string actual_hex = bytes_to_hex(compressed);

        ASSERT_EQ(expected_hex, actual_hex)
            << "Binary compatibility failed for original data. "
            << "Expected: " << expected_hex << ", Got: " << actual_hex;

        // Also verify round-trip
        ASSERT_OK_AND_ASSIGN(auto decompressed, DeltaVarintCompressor::Decompress(compressed));
        ASSERT_EQ(original, decompressed) << "Round-trip failed for original data";
    }
}

// Test compatibility with Java for large numbers (64-bit range)
TEST(DeltaVarintCompressorTest, TestJavaCompatibilityLargeNumbers) {
    // Test cases covering the full 64-bit signed integer range
    std::vector<int64_t> large_number_cases = {
        2147483647LL,            // Integer.MAX_VALUE
        -2147483648LL,           // Integer.MIN_VALUE
        9223372036854775807LL,   // Long.MAX_VALUE
        -9223372036854775807LL,  // Long.MIN_VALUE + 1 (avoid overflow)
        4294967295LL,            // 2^32 - 1
        -4294967296LL,           // -2^32
    };

    for (int64_t value : large_number_cases) {
        // Test individual values
        std::vector<int64_t> single_value = {value};
        auto compressed = DeltaVarintCompressor::Compress(single_value);
        ASSERT_OK_AND_ASSIGN(auto decompressed, DeltaVarintCompressor::Decompress(compressed));
        ASSERT_EQ(single_value, decompressed) << "Large number compatibility failed for " << value;
    }

    // Test as a sequence to verify delta encoding with large numbers
    auto compressed_seq = DeltaVarintCompressor::Compress(large_number_cases);
    ASSERT_OK_AND_ASSIGN(auto decompressed_seq, DeltaVarintCompressor::Decompress(compressed_seq));
    ASSERT_EQ(large_number_cases, decompressed_seq) << "Large number sequence compatibility failed";
}

// Test Varint encoding boundaries that match Java implementation
TEST(DeltaVarintCompressorTest, TestJavaCompatibilityVarintBoundaries) {
    // Test values at Varint encoding boundaries
    std::vector<int64_t> varint_boundary_cases = {
        // 1-byte Varint boundary
        63,   // ZigZag(63) = 126, fits in 1 byte
        64,   // ZigZag(64) = 128, needs 2 bytes
        -64,  // ZigZag(-64) = 127, fits in 1 byte
        -65,  // ZigZag(-65) = 129, needs 2 bytes

        // 2-byte Varint boundary
        8191,   // ZigZag(8191) = 16382, fits in 2 bytes
        8192,   // ZigZag(8192) = 16384, needs 3 bytes
        -8192,  // ZigZag(-8192) = 16383, fits in 2 bytes
        -8193,  // ZigZag(-8193) = 16385, needs 3 bytes

        // 3-byte Varint boundary
        1048575,  // ZigZag(1048575) = 2097150, fits in 3 bytes
        1048576,  // ZigZag(1048576) = 2097152, needs 4 bytes
    };

    for (int64_t value : varint_boundary_cases) {
        std::vector<int64_t> single_value = {value};
        auto compressed = DeltaVarintCompressor::Compress(single_value);
        ASSERT_OK_AND_ASSIGN(auto decompressed, DeltaVarintCompressor::Decompress(compressed));
        ASSERT_EQ(single_value, decompressed)
            << "Varint boundary compatibility failed for " << value;
    }
}

// Test delta encoding edge cases for Java compatibility
TEST(DeltaVarintCompressorTest, TestJavaCompatibilityDeltaEdgeCases) {
    // Edge cases that test delta encoding behavior
    std::vector<std::vector<int64_t>> delta_edge_cases = {
        // Maximum positive delta
        {0, std::numeric_limits<int64_t>::max()},
        // Maximum negative delta
        {std::numeric_limits<int64_t>::max(), 0},
        // Alternating large deltas
        {0, 1000000, -1000000, 2000000, -2000000},
        // Sequence with zero deltas
        {42, 42, 42, 42},
        // Mixed small and large deltas
        {0, 1, 1000000, 1000001, 0},
    };

    for (const auto& test_case : delta_edge_cases) {
        auto compressed = DeltaVarintCompressor::Compress(test_case);
        ASSERT_OK_AND_ASSIGN(auto decompressed, DeltaVarintCompressor::Decompress(compressed));
        ASSERT_EQ(test_case, decompressed) << "Delta edge case compatibility failed";
    }
}

// Test error conditions that should match Java behavior
TEST(DeltaVarintCompressorTest, TestJavaCompatibilityErrorConditions) {
    // Test cases for error handling - our implementation gracefully handles
    // truncated data by returning errors, which is acceptable behavior

    // Test with various truncated/invalid byte sequences
    std::vector<std::vector<char>> invalid_cases = {
        {static_cast<char>(0x80)},                           // Single incomplete byte
        {static_cast<char>(0x80), static_cast<char>(0x80)},  // Incomplete 3-byte varint
        {static_cast<char>(0xFF), static_cast<char>(0xFF), static_cast<char>(0xFF),
         static_cast<char>(0xFF), static_cast<char>(0xFF), static_cast<char>(0xFF),
         static_cast<char>(0xFF), static_cast<char>(0xFF), static_cast<char>(0xFF),
         static_cast<char>(0x80)},  // Long sequence
    };

    for (const auto& invalid_data : invalid_cases) {
        // Our implementation handles invalid data by returning an error
        // This is acceptable behavior for robustness
        auto result = DeltaVarintCompressor::Decompress(invalid_data);
        ASSERT_NOK(result) << "Should return error for invalid data";
    }

    // Test that valid empty input returns empty list
    std::vector<char> empty_input;
    ASSERT_OK_AND_ASSIGN(auto empty_result, DeltaVarintCompressor::Decompress(empty_input));
    ASSERT_TRUE(empty_result.empty()) << "Empty input should return empty list";
}

}  // namespace paimon::test
