#pragma once

#include "protocol/Prodcc/DataSummary.h"
#include "protocol/Prodcc/AriaTransaction.h"
#include <algorithm>
#include <vector>
#include <utility> // For std::pair

namespace aria {

struct TxnQueryRanges {
    std::vector<std::pair<uint64_t, uint64_t>> read;
    std::vector<std::pair<uint64_t, uint64_t>> write;
};

class ConflictPredictor {
public:
    ConflictPredictor() = default;

    void build_summary(const std::vector<uint64_t>& keys) {
        data_summary_.build(keys);
    }

    void fill_query_ranges(const ProdccTransaction& txn, TxnQueryRanges& out) const {
        out.read.clear();
        out.write.clear();
        txn.get_query_ranges(out.read, out.write);
    }

    /**
     * @brief Predicts the conflict type between two transactions (t1, t2) assuming t1 executes before t2.
     * 
     * @param t1 The transaction that is logically ordered first.
     * @param t2 The transaction that is logically ordered second.
     * @return The highest priority conflict type found (WAW > RAW > WAR > NONE).
     */
    ConflictType predict_conflict(const ProdccTransaction& t1, const ProdccTransaction& t2) const {
        DCHECK(t1.get_id() < t2.get_id());

        // Avoid per-call allocations in the hot path (called O(n^2) during partitioning).
        thread_local TxnQueryRanges t1_ranges;
        thread_local TxnQueryRanges t2_ranges;

        fill_query_ranges(t1, t1_ranges);
        fill_query_ranges(t2, t2_ranges);

        return predict_conflict(t1_ranges, t2_ranges);
    }

    ConflictType predict_conflict(const TxnQueryRanges& t1, const TxnQueryRanges& t2) const {
        // 1. Check for WAW (Write-After-Write): W(t1) ∩ W(t2)
        if (check_range_intersection(t1.write, t2.write)) {
            return ConflictType::WAW;
        }

        // 2. Check for RAW (Read-After-Write): W(t1) ∩ R(t2)
        if (check_range_intersection(t1.write, t2.read)) {
            return ConflictType::RAW;
        }

        // 3. Check for WAR (Write-After-Read): R(t1) ∩ W(t2)
        if (check_range_intersection(t1.read, t2.write)) {
            return ConflictType::WAR;
        }

        // 4. No conflict detected
        return ConflictType::NONE;
    }

private:
    /**
     * @brief 检查两个已排序且不相交的范围集之间是否存在交集。
     *        此实现采用双指针线性扫描算法，复杂度为 O(K1 + K2)。
     */
    bool check_range_intersection(const std::vector<std::pair<uint64_t, uint64_t>>& ranges1,
                                  const std::vector<std::pair<uint64_t, uint64_t>>& ranges2) const {
        if (ranges1.empty() || ranges2.empty()) {
            return false;
        }

        auto it1 = ranges1.begin();
        auto it2 = ranges2.begin();

        while (it1 != ranges1.end() && it2 != ranges2.end()) {
            uint64_t intersect_start = std::max(it1->first, it2->first);
            uint64_t intersect_end = std::min(it1->second, it2->second);

            if (intersect_start <= intersect_end) {
                if (data_summary_.query_range_exists(intersect_start, intersect_end)) {
                    return true;
                }
            }
            
            if (it1->second < it2->second) {
                ++it1;
            } else {
                ++it2;
            }
        }

        return false;
    }

private:
    DataSummary data_summary_;
};

} // namespace aria
