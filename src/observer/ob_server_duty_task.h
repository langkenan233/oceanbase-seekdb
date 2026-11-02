/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef OB_SERVER_DUTY_TASK_H
#define OB_SERVER_DUTY_TASK_H
#include <stdint.h>
#include <pthread.h>
#include "lib/task/ob_timer.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase {
namespace observer {

class ObServerDutyTask
    : private common::ObTimerTask
{
  static constexpr int64_t SCHEDULE_PERIOD = 10 * 1000L * 1000L;
public:
  int schedule(common::ObTimer &timer);
  ObServerDutyTask();
private:
  void runTimerTask() override;
  void update_runtime_settings();

private:
  // Apply context-memory limits from the runtime configuration.
  int update_ctx_memory_throttle();
  common::ObArenaAllocator allocator_;
};

class ObSqlMemoryTimerTask : private common::ObTimerTask
{
public:
  int schedule(common::ObTimer &timer);
private:
  void runTimerTask() override;
private:
  static constexpr int64_t SCHEDULE_PERIOD = 3 * 1000L * 1000L;
};

// Fills in the AI columns that were left empty by a write. The DML itself only
// queues the work, because a model call can take longer than the statement that
// caused it is allowed to run.
//
// This runs on its own thread instead of a timer task: the timer service in this
// build does not carry a worker pool, so queued timer tasks never run.
class ObAiColumnWorkerTask
{
  static constexpr int64_t SLEEP_BETWEEN_ROUNDS = 20 * 1000L;
  static constexpr int64_t MAX_TASKS_PER_ROUND = 32;
public:
  ObAiColumnWorkerTask();
  ~ObAiColumnWorkerTask();
  int start();
  void stop();
private:
  // One queued column of one row. The rows that share a request id and a row
  // predicate are computed together, so their results become visible at once.
  struct AiTaskRow
  {
    uint64_t table_id_;
    uint64_t column_id_;
    int64_t retry_count_;
    int64_t state_;
    common::ObString task_id_;
    common::ObString request_id_;
    common::ObString predicate_;
    common::ObString db_name_;
    common::ObString table_name_;
  };
  static void *loop_entry(void *arg);
  void loop();
  int do_work();
  int exec_write(const common::ObString &sql, int64_t &affected_rows);
  int fetch_tasks(common::ObIAllocator &alloc, common::ObIArray<AiTaskRow> &rows);
  int process_group(common::ObIAllocator &alloc, const common::ObIArray<AiTaskRow> &group);
  int build_update_sql(common::ObIAllocator &alloc,
                       const common::ObIArray<AiTaskRow> &group,
                       common::ObSqlString &sql);
  int finish_group(const common::ObIArray<AiTaskRow> &group);
private:
  pthread_t thread_;
  bool started_;
  volatile bool stop_;
  int64_t tick_count_;
};

}  // observer
}  // oceanbase

#endif /* OB_SERVER_DUTY_TASK_H */
