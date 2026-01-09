// protocol/Prodcc/AriaManager.h

#pragma once

#include "core/Manager.h"
#include "core/Partitioner.h"
#include "protocol/Prodcc/Aria.h"
#include "protocol/Prodcc/AriaExecutor.h"
#include "protocol/Aria/AriaHelper.h"
#include "protocol/Prodcc/AriaTransaction.h"
#include "protocol/Prodcc/ConflictPredictor.h"
#include "protocol/Prodcc/KeyConverter.h"
#include "benchmark/tpcc/Schema.h"
#include "benchmark/ycsb/Schema.h"
#include "common/Random.h"

#include <boost/dynamic_bitset.hpp>
#include <algorithm> // For std::sort
#include <array>
#include <atomic>
#include <chrono>
#include <iterator>
#include <mutex>
#include <thread> // For std::thread
#include <unordered_map>
#include <vector>

namespace aria {

// Forward declaration
template <class Workload> class ProdccManager;

using MicroBlock = std::vector<ProdccTransaction*>;

template <class Workload> class ProdccManager : public aria::Manager {
public:
  using base_type = aria::Manager;

  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TransactionType = ProdccTransaction;
  static_assert(std::is_same<typename WorkloadType::TransactionType,
                             TransactionType>::value,
                "Transaction types do not match.");
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  ProdccManager(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
              const ContextType &context, std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag), db(db), epoch(0),
        current_micro_block(nullptr),
        partitioner(PartitionerFactory::create_partitioner(
            context.partitioner, coordinator_id, context.coordinator_num))  {

    // Prodcc: Define the aggregation factor. Here, we aggregate 5 micro-blocks.
    const std::size_t AGGREGATION_FACTOR = 5;

    // The batch_size from context is now treated as the size of a "Micro Block".
    const std::size_t micro_block_size = context.batch_size;
    
    // The "Macro Block" is the aggregation of multiple micro-blocks.
    const std::size_t macro_block_size = micro_block_size * AGGREGATION_FACTOR;

    LOG(INFO) << "ProdccManager: Aggregating " << AGGREGATION_FACTOR 
              << " micro-blocks of size " << micro_block_size 
              << " into a macro-block of size " << macro_block_size;

    // Initialize storage for the entire macro-block.
    storages.resize(macro_block_size);
    transactions.resize(macro_block_size);

    LOG(INFO) << "Coordinator " << coordinator_id << ": Starting to build DataSummary...";

    const std::vector<uint64_t>& all_db_keys = db.get_all_keys();
    predictor_.build_summary(all_db_keys);

    LOG(INFO) << "Conflict predictor data summary built successfully.";
  }

 private:
  void build_txns_to_schedule() {
    txns_to_schedule_.clear();
    txns_to_schedule_.reserve(transactions.size());
    for (auto &txn_ptr : transactions) {
      if (txn_ptr && !txn_ptr->abort_no_retry) {
        txns_to_schedule_.push_back(txn_ptr.get());
      }
    }

    std::sort(txns_to_schedule_.begin(), txns_to_schedule_.end(),
              [](const TransactionType *a, const TransactionType *b) {
                return a->get_id() < b->get_id();
              });
  }

  void prepare_dependency_graph(std::size_t graph_size) {
    if (dependency_graph.size() != graph_size) {
      dependency_graph.resize(graph_size);
    }
    for (auto &edges : dependency_graph) {
      edges.clear();
    }
  }

  void build_query_ranges_cache_if_needed(bool enable_prediction) {
    if (!enable_prediction) {
      txn_ranges_.clear();
      return;
    }

    txn_ranges_.resize(txns_to_schedule_.size());
    for (std::size_t i = 0; i < txns_to_schedule_.size(); ++i) {
      txn_ranges_[i].read.reserve(8);
      txn_ranges_[i].write.reserve(8);
      predictor_.fill_query_ranges(*txns_to_schedule_[i], txn_ranges_[i]);
    }
  }

  double last_macro_abort_rate() const {
    const auto last_total_valid =
        last_macro_total_valid.load(std::memory_order_acquire);
    if (last_total_valid == 0) {
      return 0.0;
    }
    return 1.0 * last_macro_total_delay.load(std::memory_order_acquire) / last_total_valid;
  }

  bool enable_conflict_prediction() const {
    if (prediction_bypass_cooldown_.load(std::memory_order_acquire) > 0) {
      return false;
    }
    const double threshold =
        std::clamp(context.prodcc_abort_rate_threshold, 0.0, 1.0);
    return last_macro_abort_rate() >= threshold;
  }

  std::vector<MicroBlock> partition_into_fixed_micro_blocks(
      const std::vector<TransactionType*>& transactions_to_schedule,
      std::size_t micro_block_size) {
    std::vector<MicroBlock> result;
    if (transactions_to_schedule.empty() || micro_block_size == 0) {
      return result;
    }

    result.reserve((transactions_to_schedule.size() + micro_block_size - 1) /
                   micro_block_size);

    for (std::size_t i = 0; i < transactions_to_schedule.size();
         i += micro_block_size) {
      MicroBlock mb;
      const std::size_t end =
          std::min(i + micro_block_size, transactions_to_schedule.size());
      mb.reserve(end - i);
      for (std::size_t j = i; j < end; ++j) {
        mb.push_back(transactions_to_schedule[j]);
      }
      result.push_back(std::move(mb));
    }
    return result;
  }

 public:
  // 新增：此函数将由所有 Executor 并行调用
  void partition_task(std::size_t worker_id) {
      const bool enable_prediction = enable_conflict_prediction();
      if (!enable_prediction) {
        return;
      }

      const double toxic_query_rate =
          std::clamp(context.prodcc_toxic_query_rate, 0.0, 1.0);
      static thread_local aria::Random toxic_rng;
      static thread_local bool toxic_rng_seeded = false;
      if (!toxic_rng_seeded) {
        toxic_rng.init_seed(static_cast<uint64_t>(worker_id) ^
                            (reinterpret_cast<uint64_t>(this) >> 4));
        toxic_rng_seeded = true;
      }

      const auto &txns_to_schedule = txns_to_schedule_;
      const auto &txn_ranges = txn_ranges_;

      std::size_t total_workers = context.worker_num;
      if (worker_id >= total_workers) {
        return;
      }
      std::size_t n = txns_to_schedule.size();
      if (n <= 1) return;
      DCHECK(!dependency_graph.empty());

      // Build edges locally to avoid locking on each conflict and avoid allocating
      // a giant vector-of-vectors per worker.
      std::unordered_map<std::size_t,
                         std::vector<std::pair<std::size_t, ConflictType>>>
          local_edges;
      local_edges.reserve(256);

      for (std::size_t i = worker_id; i < n; i += total_workers) {
          for (std::size_t j = i + 1; j < n; ++j) {
              TransactionType* t1 = txns_to_schedule[i];
              TransactionType* t2 = txns_to_schedule[j];

              // Transactions are pre-sorted by ID, so (i < j) implies t1 executes before t2.
              ConflictType conflict = ConflictType::NONE;
              if (toxic_query_rate > 0.0 &&
                  toxic_rng.next_double() < toxic_query_rate) {
                // Experiment: simulate predictor failure by forcing a worst-case conflict.
                conflict = ConflictType::NONE;
              } else {
                conflict =
                    predictor_.predict_conflict(txn_ranges[i], txn_ranges[j]);
              }
                
              if (conflict != ConflictType::NONE) {
                  local_edges[t1->get_id()].push_back({t2->get_id(), conflict});
              }
          }
      }

      for (auto &kv : local_edges) {
        const std::size_t src_id = kv.first;
        auto &src = kv.second;
        if (src.empty()) {
          continue;
        }

        auto &mtx = graph_build_locks_[src_id % kGraphLockShards];
        std::lock_guard<std::mutex> lock(mtx);
        auto &dst = dependency_graph[src_id];
        dst.insert(dst.end(), std::make_move_iterator(src.begin()),
                   std::make_move_iterator(src.end()));
      }
  }

private:
  /**
   * @brief 实现确定性的主动推迟策略，将宏块划分为微块。
   * 
   * @param transactions_to_schedule 待调度的宏块中所有有效事务的 vector，
   *                                 必须已按事务ID升序排列。
   * @return 一个微块的序列 (std::vector<MicroBlock>)。
   */
  std::vector<MicroBlock> partition_into_micro_blocks(
      const std::vector<TransactionType*>& transactions_to_schedule) {
      
      std::vector<MicroBlock> final_schedule;
      if (transactions_to_schedule.empty()) {
          return final_schedule;
      }

      std::vector<TransactionType*> remaining_txns = transactions_to_schedule;
      std::vector<TransactionType*> postponed_txns;

       while (!remaining_txns.empty()) {
           MicroBlock current_mb;
           postponed_txns.clear();

           for (auto* t_i : remaining_txns) {
               bool should_postpone = false;
               for (const auto* t_j : current_mb) {
                   // 在预计算的图中查找冲突
                  const auto& neighbors = dependency_graph[t_j->get_id()];
                  for (const auto& edge : neighbors) {
                      if (edge.first == t_i->get_id()) {
                          ConflictType conflict = edge.second;
                          if (conflict == ConflictType::WAW || conflict == ConflictType::RAW) {
                              should_postpone = true;
                              break;
                          }
                          if (!context.aria_reordering_optmization && conflict == ConflictType::WAR) {
                              should_postpone = true;
                              break;
                          }
                      }
                  }
                  if (should_postpone) break;
              }

              if (should_postpone) {
                  postponed_txns.push_back(t_i);
              } else {
                  current_mb.push_back(t_i);
              }
          }

          if (current_mb.empty() && !remaining_txns.empty()) {
              current_mb.push_back(remaining_txns.front());
              postponed_txns.erase(std::remove(postponed_txns.begin(), postponed_txns.end(), remaining_txns.front()), postponed_txns.end());
          }

          if (!current_mb.empty()) {
              final_schedule.push_back(std::move(current_mb));
          }
          
          remaining_txns = std::move(postponed_txns);
      }
      return final_schedule;
  }


public:
  const MicroBlock& get_current_micro_block() const {
      DCHECK(current_micro_block != nullptr);
      return *current_micro_block;
  }

  void coordinator_start() override {
    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    while (!stopFlag.load()) {
        epoch.fetch_add(1);

        // 阶段 1: 聚合与事务生成 (与Aria相同)
        cleanup_batch(); 
        
        n_started_workers.store(0);
        n_completed_workers.store(0);
        signal_worker(ExecutorStatus::Aria_READ);
        wait_all_workers_start();
        wait_all_workers_finish();
        
        // 全局同步点 1: 确保所有节点都生成了相同的 Macro-Block
        broadcast_stop(); 
        wait4_stop(n_coordinators - 1);
        n_completed_workers.store(0);
        set_worker_status(ExecutorStatus::STOP);
        wait_all_workers_finish();
        wait4_ack();

        // // timer_flag
        // const auto prediction_phase_start =
        //     context.enable_timer ? std::chrono::steady_clock::now()
        //                          : std::chrono::steady_clock::time_point{};

        // 阶段 2: 并行冲突预测与图构建 (PRODCC_PARTITION)
        const bool enable_prediction = enable_conflict_prediction();
        last_macro_prediction_enabled_.store(enable_prediction,
                                            std::memory_order_release);
        if (enable_prediction) {
          prepare_dependency_graph(transactions.size() * n_coordinators + 1);
        }
        build_txns_to_schedule();
        build_query_ranges_cache_if_needed(enable_prediction);

        n_started_workers.store(0);
        n_completed_workers.store(0);
        signal_worker(ExecutorStatus::PRODCC_PARTITION);
        wait_all_workers_start();
        wait_all_workers_finish();
        
        // 全局同步点 2
        broadcast_stop();
        wait4_stop(n_coordinators - 1);
        n_completed_workers.store(0);
        set_worker_status(ExecutorStatus::STOP);
        wait_all_workers_finish();
        wait4_ack();

        // // timer_flag
        // if (context.enable_timer) {
        //   const auto prediction_phase_end = std::chrono::steady_clock::now();
        //   const auto ns =
        //       std::chrono::duration_cast<std::chrono::nanoseconds>(
        //           prediction_phase_end - prediction_phase_start)
        //           .count();
        //   timer_phase1_ns.fetch_add(static_cast<uint64_t>(ns),
        //                             std::memory_order_relaxed);
        // }

        // const auto schedule_exec_phase_start =
        //     context.enable_timer ? std::chrono::steady_clock::now()
        //                          : std::chrono::steady_clock::time_point{};

        // 阶段 3: 基于图的快速串行调度
        if (enable_prediction) {
          schedule = partition_into_micro_blocks(txns_to_schedule_);
        } else {
          schedule =
              partition_into_fixed_micro_blocks(txns_to_schedule_, context.batch_size);
        }

        // 阶段 3: 分块串行执行
        for (std::size_t mb_idx = 0; mb_idx < schedule.size(); ++mb_idx) {
            auto& mb = schedule[mb_idx];
            current_micro_block = &mb;

            n_started_workers.store(0);
            n_completed_workers.store(0);
            signal_worker(ExecutorStatus::Aria_COMMIT); // 指令 Executor 执行 MB
            wait_all_workers_start();
            wait_all_workers_finish();
            
            // 全局同步点 2: 确保当前 MB 在所有节点上都已提交
            broadcast_stop();
            wait4_stop(n_coordinators - 1);
            n_completed_workers.store(0);
            set_worker_status(ExecutorStatus::STOP);
            wait_all_workers_finish();
            cleanup_metadata(mb); // 在同步提交之后，清理刚刚提交的微块所接触到的元数据
            wait4_ack();
        }
        current_micro_block = nullptr; // 重置指针

        // // timer_flag
        // if (context.enable_timer) {
        //   const auto schedule_exec_phase_end = std::chrono::steady_clock::now();
        //   const auto ns =
        //       std::chrono::duration_cast<std::chrono::nanoseconds>(
        //           schedule_exec_phase_end - schedule_exec_phase_start)
        //           .count();
        //   timer_phase2_ns.fetch_add(static_cast<uint64_t>(ns),
        //                             std::memory_order_relaxed);
        // }
    }
    signal_worker(ExecutorStatus::EXIT);
  }

  void non_coordinator_start() override {
    std::size_t n_coordinators = context.coordinator_num;

    for (;;) {
        ExecutorStatus status = wait4_signal();
        if (status == ExecutorStatus::EXIT) {
            set_worker_status(ExecutorStatus::EXIT);
            break;
        }

        // 阶段 1: 聚合与生成
        DCHECK(status == ExecutorStatus::Aria_READ);
        epoch.fetch_add(1);
        cleanup_batch();
        
        n_started_workers.store(0);
        n_completed_workers.store(0);
        set_worker_status(ExecutorStatus::Aria_READ);
        wait_all_workers_start();
        wait_all_workers_finish();
        broadcast_stop();
        wait4_stop(n_coordinators - 1);
        n_completed_workers.store(0);
        set_worker_status(ExecutorStatus::STOP);
        wait_all_workers_finish();
        send_ack();

        // // timer_flag
        // const auto prediction_phase_start =
        //     context.enable_timer ? std::chrono::steady_clock::now()
        //                          : std::chrono::steady_clock::time_point{};

        // 阶段 2: 并行冲突预测
        status = wait4_signal();
        DCHECK(status == ExecutorStatus::PRODCC_PARTITION); // <-- 等待新状态
        const bool enable_prediction = enable_conflict_prediction();
        last_macro_prediction_enabled_.store(enable_prediction,
                                            std::memory_order_release);
        if (enable_prediction) {
          prepare_dependency_graph(transactions.size() * n_coordinators + 1);
        }
        build_txns_to_schedule();
        build_query_ranges_cache_if_needed(enable_prediction);

        n_started_workers.store(0);
        n_completed_workers.store(0);
        set_worker_status(ExecutorStatus::PRODCC_PARTITION);
        wait_all_workers_start();
        wait_all_workers_finish();
        broadcast_stop();
        wait4_stop(n_coordinators - 1);
        n_completed_workers.store(0);
        set_worker_status(ExecutorStatus::STOP);
        wait_all_workers_finish();
        send_ack();

        // // timer_flag
        // if (context.enable_timer) {
        //   const auto prediction_phase_end = std::chrono::steady_clock::now();
        //   const auto ns =
        //       std::chrono::duration_cast<std::chrono::nanoseconds>(
        //           prediction_phase_end - prediction_phase_start)
        //           .count();
        //   timer_phase1_ns.fetch_add(static_cast<uint64_t>(ns),
        //                             std::memory_order_relaxed);
        // }

        // const auto schedule_exec_phase_start =
        //     context.enable_timer ? std::chrono::steady_clock::now()
        //                          : std::chrono::steady_clock::time_point{};

        // 阶段 3: 快速串行调度
        if (enable_prediction) {
          schedule = partition_into_micro_blocks(txns_to_schedule_);
        } else {
          schedule =
              partition_into_fixed_micro_blocks(txns_to_schedule_, context.batch_size);
        }

        // 阶段 3: 分块串行执行
        for (std::size_t mb_idx = 0; mb_idx < schedule.size(); ++mb_idx) {
            auto& mb = schedule[mb_idx];
            current_micro_block = &mb;
            status = wait4_signal();
            DCHECK(status == ExecutorStatus::Aria_COMMIT);

            n_started_workers.store(0);
            n_completed_workers.store(0);
            set_worker_status(ExecutorStatus::Aria_COMMIT);
            wait_all_workers_start();
            wait_all_workers_finish();
            broadcast_stop();
            wait4_stop(n_coordinators - 1);
            n_completed_workers.store(0);
            set_worker_status(ExecutorStatus::STOP);
            wait_all_workers_finish();
            cleanup_metadata(mb);
            send_ack(); 
        }
        current_micro_block = nullptr; // 重置指针

        // // timer_flag
        // if (context.enable_timer) {
        //   const auto schedule_exec_phase_end = std::chrono::steady_clock::now();
        //   const auto ns =
        //       std::chrono::duration_cast<std::chrono::nanoseconds>(
        //           schedule_exec_phase_end - schedule_exec_phase_start)
        //           .count();
        //   timer_phase2_ns.fetch_add(static_cast<uint64_t>(ns),
        //                             std::memory_order_relaxed);
        // }
    }
  }

  void cleanup_batch() {
    std::size_t it = 0;
    std::size_t it2 = 0;
    std::size_t total_valid = 0;
    for (auto i = 0u; i < transactions.size(); i++) {
      if (transactions[i] == nullptr) {
        break;
      }
      if (!transactions[i]->abort_no_retry) {
        total_valid++;
      }
      if (transactions[i]->abort_lock) {
        transactions[it++].swap(transactions[i]);
      }
      if (transactions[i]->delay_lock) {
        it2++;
      }
    }
    total_abort.store(it);
    last_macro_total_delay.store(it2);
    last_macro_total_valid.store(total_valid);

    // Adaptive failsafe: if prediction was enabled in the last macro-block but
    // observed delay/abort rate regressed compared to the last bypass baseline,
    // force bypass for a few epochs to "fallback" safely.
    //
    // Note: delay_lock is used as Prodcc's "abort/delay" signal.
    const uint32_t abort_ppm =
        total_valid == 0 ? 0u
                         : static_cast<uint32_t>((1000000.0 * it2) / total_valid);
    last_macro_abort_ppm_.store(abort_ppm, std::memory_order_release);

    uint32_t cooldown =
        prediction_bypass_cooldown_.load(std::memory_order_acquire);
    if (cooldown > 0) {
      prediction_bypass_cooldown_.store(cooldown - 1, std::memory_order_release);
    }

    const bool last_used_prediction =
        last_macro_prediction_enabled_.load(std::memory_order_acquire);
    if (!last_used_prediction) {
      bypass_baseline_abort_ppm_.store(abort_ppm, std::memory_order_release);
      return;
    }

    const uint32_t baseline_ppm =
        bypass_baseline_abort_ppm_.load(std::memory_order_acquire);
    // 1% absolute regression triggers fallback.
    static constexpr uint32_t kRegressionMarginPpm = 10000;
    static constexpr uint32_t kBypassCooldownEpochs = 5;
    if (abort_ppm > baseline_ppm + kRegressionMarginPpm) {
      prediction_bypass_cooldown_.store(kBypassCooldownEpochs,
                                       std::memory_order_release);
    }
  }

private:
  void cleanup_metadata(const MicroBlock& mb) {
    // 遍历刚刚提交的微块中的所有事务
    for (const auto* txn : mb) {
      // 只需要为成功提交的事务清理元数据
      if (txn->abort_lock || txn->abort_no_retry) {
        continue;
      }

      auto cleanup_key_metadata = [this](const AriaRWKey& key) {
        if (key.get_local_index_read_bit()) {
          return; // 本地索引读取不涉及共享元数据
        }
        auto partitionId = key.get_partition_id();
        if (partitioner->has_master_partition(partitionId)) {
          auto table = db.find_table(key.get_table_id(), partitionId);
          // 获取元数据的原子引用并将其重置为0
          // 这会清除 epoch, wts, 和 rts
          std::atomic<uint64_t>& metadata = table->search_metadata(key.get_key());
          metadata.store(0);
        }
      };
      
      // 为读集和写集中的所有键清理元数据
      // 注意：Aria 中，写操作也会出现在读集中（read-modify-write）
      for (const auto& key : txn->readSet) {
        cleanup_key_metadata(key);
      }
      for (const auto& key : txn->writeSet) {
        cleanup_key_metadata(key);
      }
    }
  }

private:
  std::vector<MicroBlock> schedule;
  MicroBlock* current_micro_block;
  ConflictPredictor predictor_;
  std::unique_ptr<Partitioner> partitioner;

  // 依赖图，将在每个宏块开始时构建
  std::vector<std::vector<std::pair<std::size_t, ConflictType>>> dependency_graph;
  static constexpr std::size_t kGraphLockShards = 64;
  std::array<std::mutex, kGraphLockShards> graph_build_locks_{};

  // Pre-sorted per-macro-block inputs shared by all worker threads.
  std::vector<TransactionType*> txns_to_schedule_;
  std::vector<TxnQueryRanges> txn_ranges_;

public:
  RandomType random;
  DatabaseType &db;
  std::atomic<uint32_t> epoch;
  std::vector<StorageType> storages;
  std::vector<std::unique_ptr<TransactionType>> transactions;
  std::atomic<uint32_t> total_abort;
  std::atomic<uint32_t> last_macro_total_delay{0};
  std::atomic<uint32_t> last_macro_total_valid{0};
  std::atomic<uint32_t> last_macro_abort_ppm_{0};

  // Adaptive prediction control (failsafe fallback to bypass).
  std::atomic<bool> last_macro_prediction_enabled_{false};
  std::atomic<uint32_t> bypass_baseline_abort_ppm_{0};
  std::atomic<uint32_t> prediction_bypass_cooldown_{0};
};
} // namespace aria
