#pragma once

#include "protocol/Prodcc/L1Filter.h"
#include "protocol/Prodcc/L2Verifier.h"

namespace aria {

class DataSummary {
public:
    DataSummary() = default;

    /**
     * @brief 从 uint64_t 键集合构建两级数据概要。
     */
    void build(const std::vector<uint64_t>& keys) {
        l1_filter_.build(keys);
        l2_verifier_.build(keys);
    }

    /**
     * @brief 对一个范围进行存在性查询。
     * 
     * @param start 范围起始。
     * @param end 范围结束。
     * @return true 如果范围内可能存在数据, false 如果确定不存在。
     */
    bool query_range_exists(uint64_t start, uint64_t end) const {
        // 第一级过滤
        if (l1_filter_.query(start, end) == L1Result::NO_CONFLICT) {
            return false;
        }

        // 第二级验证
        // 注意：对于点查询 [v,v]，这里能精确判断。
        // 对于真正的范围查询，L2 仍可能有假阳性，但这符合零假阴性的要求。
        return l2_verifier_.query(start, end) == L2Result::CONFLICT;
    }

private:
    L1Filter l1_filter_;
    L2Verifier l2_verifier_;
};

} // namespace aria