#pragma once

#include <vector>
#include <set> // 使用 std::set 作为平衡二叉搜索树的简单替代
#include <random>
#include <glog/logging.h>

namespace aria {

// L2 验证器的查询结果
enum class L2Result {
    NO_CONFLICT,
    CONFLICT
};

class L2Verifier {
public:
    // 哈希参数，应仔细选择以平衡性能和冲突率
    static constexpr uint64_t R = 1 << 24; // 哈希空间大小
    static constexpr uint64_t P = (1ULL << 61) - 1; // 一个大素数 > R

    L2Verifier() {
        // 在构造时随机选择哈希函数参数，保证 pairwise 独立性
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<uint64_t> distrib_alpha(1, P - 1);
        std::uniform_int_distribution<uint64_t> distrib_beta(0, P - 1);
        alpha_ = distrib_alpha(gen);
        beta_ = distrib_beta(gen);
    }
    
    /**
     * @brief 从 uint64_t 键集合构建哈希树。
     */
    void build(const std::vector<uint64_t>& keys) {
        LOG(INFO) << "Building L2Verifier with " << keys.size() << " keys...";
        hashed_summary_tree_.clear(); // Reset tree
        for (const auto& key_val : keys) {
            hashed_summary_tree_.insert(hash_func(key_val));
        }
        LOG(INFO) << "L2Verifier build complete.";
    }

    L2Result query(uint64_t start, uint64_t end) const {
        uint64_t h_start = hash_func(start);
        uint64_t h_end = hash_func(end);

        // 情况一: 非回环 (Non-wrapping)
        if (h_start <= h_end) {
            // 查找 >= h_start 的第一个元素
            auto it = hashed_summary_tree_.lower_bound(h_start);
            if (it != hashed_summary_tree_.end() && *it <= h_end) {
                return L2Result::CONFLICT;
            }
        } 
        // 情况二: 回环 (Wrapping)
        else {
            // 检查 [0, h_end]
            auto it = hashed_summary_tree_.lower_bound(0);
            if (it != hashed_summary_tree_.end() && *it <= h_end) {
                return L2Result::CONFLICT;
            }
            // 检查 [h_start, R-1]
            it = hashed_summary_tree_.lower_bound(h_start);
            if (it != hashed_summary_tree_.end()) {
                // 因为集合是有序的，所有后续元素都 >= h_start
                return L2Result::CONFLICT;
            }
        }

        return L2Result::NO_CONFLICT;
    }

private:
    uint64_t q_func(uint64_t y) const {
        // Pairwise independent hash function
        return ((alpha_ * y + beta_) % P) % R;
    }

    uint64_t hash_func(uint64_t x) const {
        // Locality-preserving double hashing
        return (q_func(x / R) + x) % R;
    }

private:
    std::set<uint64_t> hashed_summary_tree_;
    uint64_t alpha_, beta_;
};

} // namespace aria