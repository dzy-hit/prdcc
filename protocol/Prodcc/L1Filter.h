#pragma once

#include <vector>
#include <glog/logging.h>

namespace aria {

// L1 过滤器的查询结果
enum class L1Result {
    NO_CONFLICT,    // 100% 确定范围内无数据
    MAYBE_CONFLICT  // 范围内可能存在数据
};

class L1Filter {
public:
    static constexpr uint64_t kKeySpace = 1 << 20; // 假设键空间为 2^20
    static constexpr uint64_t kBlockSize = 1 << 8; // 每个块包含 256 个键
    static constexpr uint64_t kNumBlocks = kKeySpace / kBlockSize;

    L1Filter() : presence_bitmap_(kNumBlocks, false) {}


    /**
     * @brief 从 uint64_t 键集合构建位图。
     */
    void build(const std::vector<uint64_t>& keys) {
        LOG(INFO) << "Building L1Filter with " << keys.size() << " keys...";
        presence_bitmap_.assign(kNumBlocks, false); // Reset bitmap
        for (const auto& key_val : keys) {
            if (key_val >= kKeySpace) continue;

            uint64_t block_idx = key_val / kBlockSize;
            if (!presence_bitmap_[block_idx]) {
                presence_bitmap_[block_idx] = true;
            }
        }
        LOG(INFO) << "L1Filter build complete.";
    }

    /**
     * @brief 查询一个范围是否可能包含数据。
     * 
     * @param start 范围查询的起始键。
     * @param end 范围查询的结束键。
     * @return L1Result 枚举值。
     */
    L1Result query(uint64_t start, uint64_t end) const {
        // 保证查询范围在键空间内
        start = std::max(0UL, start);
        end = std::min(kKeySpace - 1, end);

        if (start > end) return L1Result::NO_CONFLICT;

        uint64_t start_block = start / kBlockSize;
        uint64_t end_block = end / kBlockSize;

        for (uint64_t i = start_block; i <= end_block; ++i) {
            if (presence_bitmap_[i]) {
                // 只要有一个块被标记，就可能存在冲突
                return L1Result::MAYBE_CONFLICT;
            }
        }

        // 所有覆盖的块都为空，确定无冲突
        return L1Result::NO_CONFLICT;
    }

private:
    std::vector<bool> presence_bitmap_;
};

} // namespace aria