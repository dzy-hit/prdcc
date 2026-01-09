#pragma once

#include "benchmark/tpcc/Schema.h"
#include "benchmark/ycsb/Schema.h"
#include <cstdint>

namespace aria {

class KeyConverter {
public:
    // For YCSB, the key is already a single integer.
    static uint64_t to_uint64(const ycsb::ycsb::key& k) {
        return static_cast<uint64_t>(k.Y_KEY);
    }

    // For TPC-C, we pack multi-column keys into a single uint64_t.
    // We need to ensure the packing does not cause collisions for TPC-C's key ranges.
    // W_ID: up to partition_num (e.g., 64) -> 6 bits
    // D_ID: up to 10 -> 4 bits
    // C_ID: up to 3000 -> 12 bits
    // O_ID: up to 3001 -> 12 bits
    // I_ID: up to 100000 -> 17 bits
    // Total bits are well within 64.

    // warehouse::key (W_ID)
    static uint64_t to_uint64(const tpcc::warehouse::key& k) {
        return static_cast<uint64_t>(k.W_ID);
    }

    // district::key (D_W_ID, D_ID)
    static uint64_t to_uint64(const tpcc::district::key& k) {
        return (static_cast<uint64_t>(k.D_W_ID) << 32) | k.D_ID;
    }

    // customer::key (C_W_ID, C_D_ID, C_ID)
    static uint64_t to_uint64(const tpcc::customer::key& k) {
        return (static_cast<uint64_t>(k.C_W_ID) << 48) | (static_cast<uint64_t>(k.C_D_ID) << 32) | k.C_ID;
    }
    
    // stock::key (S_W_ID, S_I_ID)
    static uint64_t to_uint64(const tpcc::stock::key& k) {
        return (static_cast<uint64_t>(k.S_W_ID) << 32) | k.S_I_ID;
    }

    // item::key (I_ID) - Note: Item table is not partitioned.
    static uint64_t to_uint64(const tpcc::item::key& k) {
        return static_cast<uint64_t>(k.I_ID);
    }

    // customer_name_idx::key (C_W_ID, C_D_ID, C_LAST)
    // C_LAST is a string, direct conversion is non-trivial. For now, we'll hash it.
    // For a fully deterministic system, a more robust string-to-int mapping would be needed.
    static uint64_t to_uint64(const tpcc::customer_name_idx::key& k) {
        std::size_t c_last_hash = std::hash<aria::FixedString<16>>{}(k.C_LAST);
        return (static_cast<uint64_t>(k.C_W_ID) << 48) | (static_cast<uint64_t>(k.C_D_ID) << 32) | (c_last_hash & 0xFFFFFFFF);
    }
};

} // namespace aria