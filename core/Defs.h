//
// Created by Yi Lu on 9/10/18.
//

#pragma once

namespace aria {

enum class ExecutorStatus {
  START,
  CLEANUP,
  C_PHASE,
  S_PHASE,
  Analysis,
  Execute,
  Aria_READ,
  Aria_COMMIT,
  AriaFB_READ,
  AriaFB_COMMIT,
  AriaFB_Fallback_Prepare,
  AriaFB_Fallback,
    // Prodcc Specific States
    PRODCC_GENERATE_MACRO_BLOCK, // 替代 Aria_READ，生成宏块并填充读写集
    PRODCC_PARTITION,            // Manager进行划分，Executor等待
    PRODCC_EXECUTE_MICRO_BLOCK,  // 替代 Aria_COMMIT，执行并提交一个微块
  Bohm_Analysis,
  Bohm_Insert,
  Bohm_Execute,
  Bohm_GC,
  Pwv_Analysis,
  Pwv_Execute,
    // DOCC Specific States
    DOCC_EXECUTE,
    DOCC_COMMIT,
  STOP,
  EXIT
};

enum class TransactionResult { COMMIT, READY_TO_COMMIT, ABORT, ABORT_NORETRY };

} // namespace aria
