//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include "benchmark/tpcc/Context.h"
#include "benchmark/tpcc/Random.h"
#include "common/FixedString.h"
#include <string>
#include <vector>      // ADDED
#include <utility>     // ADDED
#include "benchmark/tpcc/Schema.h" // ADDED
#include "protocol/Prodcc/KeyConverter.h" // ADDED

namespace aria {
namespace tpcc {

// 【新增】一个私有辅助函数，用于将排好序的、可能重复的键列表转换为有序、不相交的[key, key]范围列表
namespace internal {
inline void normalize_point_ranges(const std::vector<uint64_t>& sorted_keys, 
                                   std::vector<std::pair<uint64_t, uint64_t>>& out_ranges) {
    out_ranges.clear();
    if (sorted_keys.empty()) {
        return;
    }
    
    // 预分配内存
    out_ranges.reserve(sorted_keys.size());
    
    // 添加第一个元素
    out_ranges.push_back({sorted_keys[0], sorted_keys[0]});
    
    // 遍历剩余元素，只添加不重复的
    for (size_t i = 1; i < sorted_keys.size(); ++i) {
        // 因为已排序，只需与前一个比较即可去重
        if (sorted_keys[i] > sorted_keys[i-1]) {
            out_ranges.push_back({sorted_keys[i], sorted_keys[i]});
        }
    }
}
} // namespace internal

struct NewOrderQuery {
  bool isRemote() const {
    for (auto i = 0; i < O_OL_CNT; i++) {
      if (INFO[i].OL_SUPPLY_W_ID != W_ID) {
        return true;
      }
    }
    return false;
  }

  int32_t W_ID;
  int32_t D_ID;
  int32_t C_ID;
  int8_t O_OL_CNT;

  struct NewOrderQueryInfo {
    int32_t OL_I_ID;
    int32_t OL_SUPPLY_W_ID;
    int8_t OL_QUANTITY;
  };

  NewOrderQueryInfo INFO[15];

  // ADDED: Caches for pre-calculated ranges
  std::vector<std::pair<uint64_t, uint64_t>> read_ranges;
  std::vector<std::pair<uint64_t, uint64_t>> write_ranges;

  // ADDED: Method to populate the caches
  void populate_ranges() {
      std::vector<uint64_t> temp_read_keys;
      std::vector<uint64_t> temp_write_keys;
      
      // 预估大小以减少内存重分配
      temp_read_keys.reserve(3 + O_OL_CNT * 2);
      temp_write_keys.reserve(1 + O_OL_CNT);

      // 1. 仓库，区域，客户相关的键 (Warehouse, District, Customer)
      temp_read_keys.push_back(KeyConverter::to_uint64(warehouse::key(W_ID)));
      temp_read_keys.push_back(KeyConverter::to_uint64(district::key(W_ID, D_ID)));
      temp_read_keys.push_back(KeyConverter::to_uint64(customer::key(W_ID, D_ID, C_ID)));
      
      // District is Read-Modify-Write
      temp_write_keys.push_back(KeyConverter::to_uint64(district::key(W_ID, D_ID)));

      // 2. 物品和库存相关的键 (Item and Stock)
      for (int i = 0; i < O_OL_CNT; i++) {
          // 如果是无效物品ID（用于触发回滚的场景），则跳过
          if (INFO[i].OL_I_ID == 0) {
              continue;
          }
          temp_read_keys.push_back(KeyConverter::to_uint64(item::key(INFO[i].OL_I_ID)));
          temp_read_keys.push_back(KeyConverter::to_uint64(stock::key(INFO[i].OL_SUPPLY_W_ID, INFO[i].OL_I_ID)));
          
          // Stock is Read-Modify-Write
          temp_write_keys.push_back(KeyConverter::to_uint64(stock::key(INFO[i].OL_SUPPLY_W_ID, INFO[i].OL_I_ID)));
      }
      
      // 3. 排序和去重
      std::sort(temp_read_keys.begin(), temp_read_keys.end());
      std::sort(temp_write_keys.begin(), temp_write_keys.end());

      // 4. 生成最终的、有序且不相交的范围列表
      internal::normalize_point_ranges(temp_read_keys, read_ranges);
      internal::normalize_point_ranges(temp_write_keys, write_ranges);
      
      // 注意: new_order, order, order_line 表的写入键依赖于从 district 表读出的 D_NEXT_O_ID,
      // 因此无法在静态分析阶段确定。我们依赖于对 district 表的冲突检测来序列化事务，
      // 这足以保证正确性，因此这里不包含这些键。
  }
};

class makeNewOrderQuery {
public:
  NewOrderQuery operator()(const Context &context, int32_t W_ID,
                           Random &random) const {
    NewOrderQuery query;
    // W_ID is constant over the whole measurement interval
    query.W_ID = W_ID;
    // The district number (D_ID) is randomly selected within [1 ..
    // context.n_district] from the home warehouse (D_W_ID = W_ID).
    query.D_ID = random.uniform_dist(1, context.n_district);

    // The non-uniform random customer number (C_ID) is selected using the
    // NURand(1023,1,3000) function from the selected district number (C_D_ID =
    // D_ID) and the home warehouse number (C_W_ID = W_ID).

    query.C_ID = random.non_uniform_distribution(1023, 1, 3000);

    // The number of items in the order (ol_cnt) is randomly selected within [5
    // .. 15] (an average of 10).

    query.O_OL_CNT = random.uniform_dist(5, 15);

    int rbk = random.uniform_dist(1, 100);

    for (auto i = 0; i < query.O_OL_CNT; i++) {

      // A non-uniform random item number (OL_I_ID) is selected using the
      // NURand(8191,1,100000) function. If this is the last item on the order
      // and rbk = 1 (see Clause 2.4.1.4), then the item number is set to an
      // unused value.

      bool retry;
      do {
        retry = false;
        query.INFO[i].OL_I_ID =
            random.non_uniform_distribution(8191, 1, 100000);
        for (int k = 0; k < i; k++) {
          if (query.INFO[k].OL_I_ID == query.INFO[i].OL_I_ID) {
            retry = true;
            break;
          }
        }
      } while (retry);

      if (i == query.O_OL_CNT - 1 && rbk == 1) {
        query.INFO[i].OL_I_ID = 0;
      }

      // The first supplying warehouse number (OL_SUPPLY_W_ID) is selected as
      // the home warehouse 90% of the time and as a remote warehouse 10% of the
      // time.

      if (i == 0) {
        int x = random.uniform_dist(1, 100);
        if (x <= context.newOrderCrossPartitionProbability &&
            context.partition_num > 1) {
          int32_t OL_SUPPLY_W_ID = W_ID;
          while (OL_SUPPLY_W_ID == W_ID) {
            OL_SUPPLY_W_ID = random.uniform_dist(1, context.partition_num);
          }
          query.INFO[i].OL_SUPPLY_W_ID = OL_SUPPLY_W_ID;
        } else {
          query.INFO[i].OL_SUPPLY_W_ID = W_ID;
        }
      } else {
        query.INFO[i].OL_SUPPLY_W_ID = W_ID;
      }
      query.INFO[i].OL_QUANTITY = random.uniform_dist(1, 10);
    }

    return query;
  }
};

struct PaymentQuery {
  int32_t W_ID;
  int32_t D_ID;
  int32_t C_ID;
  FixedString<16> C_LAST;
  int32_t C_D_ID;
  int32_t C_W_ID;
  float H_AMOUNT;

  
  // ADDED: Caches for pre-calculated ranges
  std::vector<std::pair<uint64_t, uint64_t>> read_ranges;
  std::vector<std::pair<uint64_t, uint64_t>> write_ranges;

  // ADDED: Method to populate the caches
  void populate_ranges(const Context& context) {
      std::vector<uint64_t> temp_read_keys;
      std::vector<uint64_t> temp_write_keys;

      temp_read_keys.reserve(3);
      temp_write_keys.reserve(3);
      
      // 1. 仓库和区域相关的键 (Warehouse and District)
      if (context.write_to_w_ytd) {
          uint64_t w_key = KeyConverter::to_uint64(warehouse::key(W_ID));
          temp_read_keys.push_back(w_key);
          temp_write_keys.push_back(w_key);
      }
      uint64_t d_key = KeyConverter::to_uint64(district::key(W_ID, D_ID));
      temp_read_keys.push_back(d_key);
      temp_write_keys.push_back(d_key);

      // 2. 客户相关的键 (Customer)
      if (C_ID == 0) { // 按名字查找
          temp_read_keys.push_back(KeyConverter::to_uint64(customer_name_idx::key(C_W_ID, C_D_ID, C_LAST)));
          // 注意：按名字查找时，customer表的写入键依赖于查找结果，无法静态确定。
          // 我们依赖于对customer_name_idx的读冲突来处理，这已足够。
      } else { // 按ID查找
          uint64_t c_key = KeyConverter::to_uint64(customer::key(C_W_ID, C_D_ID, C_ID));
          temp_read_keys.push_back(c_key);
      }

      // 3. 排序和去重
      std::sort(temp_read_keys.begin(), temp_read_keys.end());
      std::sort(temp_write_keys.begin(), temp_write_keys.end());

      // 4. 生成最终的范围列表
      internal::normalize_point_ranges(temp_read_keys, read_ranges);
      internal::normalize_point_ranges(temp_write_keys, write_ranges);
  }
};

class makePaymentQuery {
public:
  PaymentQuery operator()(const Context &context, int32_t W_ID,
                          Random &random) const {
    PaymentQuery query;

    // W_ID is constant over the whole measurement interval

    query.W_ID = W_ID;

    // The district number (D_ID) is randomly selected within [1
    // ..context.n_district] from the home warehouse (D_W_ID) = W_ID).

    query.D_ID = random.uniform_dist(1, context.n_district);

    // the customer resident warehouse is the home warehouse 85% of the time
    // and is a randomly selected remote warehouse 15% of the time.

    // If the system is configured for a single warehouse,
    // then all customers are selected from that single home warehouse.

    int x = random.uniform_dist(1, 100);

    if (x <= context.paymentCrossPartitionProbability &&
        context.partition_num > 1) {
      // If x <= 15 a customer is selected from a random district number (C_D_ID
      // is randomly selected within [1 .. context.n_district]), and a random
      // remote warehouse number (C_W_ID is randomly selected within the range
      // of active warehouses (see Clause 4.2.2), and C_W_ID ≠ W_ID).

      int32_t C_W_ID = W_ID;

      while (C_W_ID == W_ID) {
        C_W_ID = random.uniform_dist(1, context.partition_num);
      }

      query.C_W_ID = C_W_ID;
      query.C_D_ID = random.uniform_dist(1, context.n_district);
    } else {
      // If x > 15 a customer is selected from the selected district number
      // (C_D_ID = D_ID) and the home warehouse number (C_W_ID = W_ID).

      query.C_D_ID = query.D_ID;
      query.C_W_ID = W_ID;
    }

    // a CID is always used.
    int y = random.uniform_dist(1, 100);
    // The customer is randomly selected 60% of the time by last name (C_W_ID ,
    // C_D_ID, C_LAST) and 40% of the time by number (C_W_ID , C_D_ID , C_ID).

    if (y <= 60 && context.payment_look_up) {
      // If y <= 60 a customer last name (C_LAST) is generated according to
      // Clause 4.3.2.3 from a non-uniform random value using the
      // NURand(255,0,999) function.

      std::string last_name =
          random.rand_last_name(random.non_uniform_distribution(255, 0, 999));
      query.C_LAST.assign(last_name);
      query.C_ID = 0;
    } else {
      // If y > 60 a non-uniform random customer number (C_ID) is selected using
      // the NURand(1023,1,3000) function.
      query.C_ID = random.non_uniform_distribution(1023, 1, 3000);
    }

    // The payment amount (H_AMOUNT) is randomly selected within [1.00 ..
    // 5,000.00].

    query.H_AMOUNT = random.uniform_dist(1, 5000);
    return query;
  }
};
} // namespace tpcc
} // namespace aria
