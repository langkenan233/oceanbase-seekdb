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

#define USING_LOG_PREFIX SERVER
#include <pthread.h>
#include <unistd.h>
#include "config_bridge.h"
#include "share/config/ob_config_helper.h"
#include "ob_server_duty_task.h"
#include "share/rc/ob_server_runtime.h"
#include "sql/engine/ob_sql_memory_manager.h"
#include "observer/omt/ob_server_runtime.h"
#include "common/mysqlclient/ob_mysql_transaction.h"
#include "common/mysqlclient/ob_isql_connection.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "lib/string/ob_sql_string.h"

using namespace oceanbase::common;

namespace oceanbase {
using namespace share;
using namespace share::schema;
using namespace sql;
namespace observer {

ObServerDutyTask::ObServerDutyTask()
  : allocator_(ObModIds::OB_DUTY_TASK)
{
}

void ObServerDutyTask::runTimerTask()
{
  allocator_.reset_remain_one_page();
  update_runtime_settings();
}

int ObServerDutyTask::schedule(common::ObTimer &timer)
{
  return timer.schedule(*this, SCHEDULE_PERIOD, true);
}

void ObServerDutyTask::update_runtime_settings()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(update_ctx_memory_throttle())) {
  }
}

int ObServerDutyTask::update_ctx_memory_throttle()
{
  int ret = OB_SUCCESS;
  {
    ObCtxMemoryLimitChecker checker;
    uint64_t ctx_id = 0;
    int64_t limit = 0;
    ObMallocAllocator *alloc = ObMallocAllocator::get_instance();
    rust::String limits = config::_ctx_memory_limit();
    if (!checker.check(limits.c_str(), ctx_id, limit)) {
      // do nothing
    } else {
      if (limits.empty()) {
        ctx_id = ObCtxIds::MAX_CTX_ID;
        limit = INT64_MAX; // empty str means no limit, and not care ctx_id.
      }
      for (int i = 0; i < ObCtxIds::MAX_CTX_ID; i++) {
        if (ObCtxIds::WORK_AREA == i ||
            ObCtxIds::META_OBJ_CTX_ID == i) {
          continue;
        }
        auto allocator = alloc->get_ctx_allocator(i);
        if (OB_NOT_NULL(allocator)) {
          if (OB_FAIL(allocator->set_limit(ctx_id == i ? limit : INT64_MAX))) {
          }
        }
      }
    }
  }
  return ret;
}

//////////////////////////////////////////////////////////////////////////////////////////

int ObSqlMemoryTimerTask::schedule(common::ObTimer &timer)
{
  return timer.schedule(*this, SCHEDULE_PERIOD, true);
}

void ObSqlMemoryTimerTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  {
    SERVER_MODULE_SCOPE {
      ObSqlMemoryManager *sql_mem_mgr = ::oceanbase::share::server_service<::oceanbase::sql::ObSqlMemoryManager>();
      if (OB_UNLIKELY(nullptr == sql_mem_mgr)) {
      } else if (OB_FAIL(sql_mem_mgr->calculate_global_bound_size())) {
      }
    }
  }
}

// ---------------------------------------------------------------------------
// AI column worker
// ---------------------------------------------------------------------------

namespace {

// The stored definition is a flat record: function kind, model name, retry
// limit, sync mode and the expression the user wrote.
const char AI_TASK_DEF_SEP = '\x01';

int split_ai_def(const ObString &def, ObIArray<ObString> &parts)
{
  int ret = OB_SUCCESS;
  int64_t start = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i <= def.length(); ++i) {
    if (i == def.length() || AI_TASK_DEF_SEP == def.ptr()[i]) {
      if (OB_FAIL(parts.push_back(ObString(static_cast<int32_t>(i - start), def.ptr() + start)))) {
        LOG_WARN("split ai column definition failed", K(ret));
      } else {
        start = i + 1;
      }
    }
  }
  return ret;
}

// The model is asked for the shape the column needs, so a json column gets a
// json value rather than whatever prose the model would otherwise write.
ObString ai_column_instruction(const ObColumnSchemaV2 &column)
{
  ObString instruction;
  if (ObJsonType == column.get_data_type()) {
    instruction = ObString::make_string("Return exactly one valid JSON value.");
  } else if (ob_is_integer_type(column.get_data_type())) {
    instruction = ObString::make_string("Return a base-10 integer.");
  }
  return instruction;
}

// Adds the instruction as the config argument of the AI call. AI_COMPLETE takes
// it as its third argument; AI_EMBED has no such argument and is left alone.
int ai_column_call_text(ObIAllocator &allocator,
                        const ObString &expr,
                        const ObString &instruction,
                        ObSqlString &text)
{
  int ret = OB_SUCCESS;
  int64_t close_pos = -1;
  if (instruction.empty()) {
    ret = text.assign(expr);
  } else {
    for (int64_t i = expr.length() - 1; i >= 0 && close_pos < 0; --i) {
      if (')' == expr.ptr()[i]) {
        close_pos = i;
      }
    }
    if (close_pos < 0) {
      ret = text.assign(expr);
    } else if (OB_FAIL(text.assign(expr.ptr(), close_pos))) {
    } else if (OB_FAIL(text.append(", '{\"system_prompt\":\""))) {
    } else if (OB_FAIL(text.append(instruction))) {
    } else if (OB_FAIL(text.append("\"}'"))) {
    } else if (OB_FAIL(text.append(expr.ptr() + close_pos, expr.length() - close_pos))) {
    }
  }
  UNUSED(allocator);
  return ret;
}

int64_t ai_def_int(const ObString &text)
{
  int64_t value = 0;
  bool negative = false;
  int64_t i = 0;
  if (text.length() > 0 && '-' == text.ptr()[0]) {
    negative = true;
    i = 1;
  }
  for (; i < text.length() && text.ptr()[i] >= '0' && text.ptr()[i] <= '9'; ++i) {
    value = value * 10 + (text.ptr()[i] - '0');
  }
  return negative ? -value : value;
}

int ai_request_column_name(const ObString &column_name, ObSqlString &name)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(name.assign("__ai_request_"))) {
  } else if (OB_FAIL(name.append(column_name))) {
  }
  return ret;
}

}  // namespace

ObAiColumnWorkerTask::ObAiColumnWorkerTask()
  : thread_(0),
    started_(false),
    stop_(false),
    tick_count_(0)
{
}

ObAiColumnWorkerTask::~ObAiColumnWorkerTask()
{
  stop();
}

int ObAiColumnWorkerTask::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(started_)) {
    ret = OB_INIT_TWICE;
  } else {
    stop_ = false;
    const int err = pthread_create(&thread_, NULL, loop_entry, this);
    if (0 != err) {
      ret = OB_ERR_SYS;
      LOG_WARN("create ai column worker thread failed", K(ret), K(err));
    } else {
      pthread_detach(thread_);
      started_ = true;
      LOG_INFO("ai column worker thread started");
    }
  }
  return ret;
}

void ObAiColumnWorkerTask::stop()
{
  if (started_) {
    stop_ = true;
    // The loop sleeps at most one round between checks.
    ::usleep(SLEEP_BETWEEN_ROUNDS * 2);
    started_ = false;
  }
}

void *ObAiColumnWorkerTask::loop_entry(void *arg)
{
  ObAiColumnWorkerTask *self = static_cast<ObAiColumnWorkerTask *>(arg);
#ifdef __linux__
  prctl(PR_SET_NAME, "AiColWorker", 0, 0, 0);
#endif
  if (NULL != self) {
    self->loop();
  }
  return NULL;
}

void ObAiColumnWorkerTask::loop()
{
  // The SQL layer is not serving requests yet when this thread is created.
  ::usleep(3 * 1000 * 1000L);
  LOG_INFO("ai column worker loop begin");
  while (!stop_) {
    ++tick_count_;
    int ret = OB_SUCCESS;
    if (OB_FAIL(do_work())) {
      LOG_WARN("ai column worker round failed", K(ret));
    }
    ::usleep(SLEEP_BETWEEN_ROUNDS);
  }
  LOG_INFO("ai column worker loop exit");
}

int ObAiColumnWorkerTask::exec_write(const ObString &sql, int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  sqlclient::ObISQLConnectionGuard conn_guard;
  sqlclient::ObISQLConnection *conn = NULL;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(GCTX.sql_proxy_->acquire_connection(conn_guard, 0))) {
    LOG_WARN("acquire ai column write connection failed", K(ret));
  } else if (OB_ISNULL(conn = conn_guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(conn->execute_write(sql, affected_rows))) {
    int tmp_ret = conn->rollback();
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("rollback ai column write failed", K(tmp_ret));
    }
  }
  return ret;
}

int ObAiColumnWorkerTask::do_work(){
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("AiColumnWorker");
  ObSEArray<AiTaskRow, 32> rows;
  ObSEArray<int64_t, 32> used;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(fetch_tasks(allocator, rows))) {
    LOG_WARN("fetch ai column tasks failed", K(ret));
  } else if (rows.empty()) {
    if (tick_count_ <= 400) {
      LOG_INFO("ai column worker idle", K(tick_count_));
    }
  } else if (OB_FAIL(used.prepare_allocate(rows.count()))) {
    LOG_WARN("prepare ai column group marks failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); ++i) {
      ObSEArray<AiTaskRow, 4> group;
      if (0 != used.at(i)) {
        continue;
      }
      used.at(i) = 1;
      if (OB_FAIL(group.push_back(rows.at(i)))) {
      }
      for (int64_t j = i + 1; OB_SUCC(ret) && j < rows.count(); ++j) {
        const AiTaskRow &left = rows.at(i);
        const AiTaskRow &right = rows.at(j);
        if (0 == used.at(j) && left.table_id_ == right.table_id_
            && 0 == left.request_id_.compare(right.request_id_)
            && 0 == left.predicate_.compare(right.predicate_)) {
          used.at(j) = 1;
          if (OB_FAIL(group.push_back(right))) {
          }
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(process_group(allocator, group))) {
        LOG_WARN("process ai column group failed", K(ret));
      }
    }
  }
  return ret;
}

int ObAiColumnWorkerTask::fetch_tasks(ObIAllocator &alloc, ObIArray<AiTaskRow> &rows)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  sqlclient::ObISQLConnectionGuard conn_guard;
  sqlclient::ObISQLConnection *conn = NULL;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(sql.append_fmt(
                 "SELECT task.table_id, task.column_id, task.task_id, task.request_id,"
                 " task.rowkey_predicate, task.state, task.retry_count,"
                 " t.table_name, d.database_name"
                 " FROM oceanbase.__all_ai_column_task task"
                 " JOIN oceanbase.__all_table t ON t.table_id = task.table_id"
                 " JOIN oceanbase.__all_database d ON d.database_id = t.database_id"
                 " WHERE task.eligible_ts <= NOW(6) LIMIT %ld",
                 MAX_TASKS_PER_ROUND))) {
    LOG_WARN("build ai column task query failed", K(ret));
  } else if (OB_FAIL(GCTX.sql_proxy_->acquire_connection(conn_guard, 0))) {
    LOG_WARN("acquire ai column connection failed", K(ret));
  } else if (OB_ISNULL(conn = conn_guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(conn->execute_read(
                      ObString(static_cast<int32_t>(sql.length()), sql.ptr()), res))) {
        LOG_WARN("read ai column tasks failed", K(ret), K(sql));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        while (OB_SUCC(ret)) {
          int tmp_ret = result->next();
          if (OB_ITER_END == tmp_ret) {
            break;
          } else if (OB_SUCCESS != tmp_ret) {
            ret = tmp_ret;
            LOG_WARN("iterate ai column tasks failed", K(ret));
          } else {
            AiTaskRow row;
            ObString task_id;
            ObString request_id;
            ObString predicate;
            ObString table_name;
            ObString db_name;
            int64_t table_id = 0;
            int64_t column_id = 0;
            MEMSET(&row, 0, sizeof(row));
            EXTRACT_INT_FIELD_MYSQL(*result, "table_id", table_id, int64_t);
            EXTRACT_INT_FIELD_MYSQL(*result, "column_id", column_id, int64_t);
            row.table_id_ = static_cast<uint64_t>(table_id);
            row.column_id_ = static_cast<uint64_t>(column_id);
            EXTRACT_INT_FIELD_MYSQL(*result, "state", row.state_, int64_t);
            EXTRACT_INT_FIELD_MYSQL(*result, "retry_count", row.retry_count_, int64_t);
            EXTRACT_VARCHAR_FIELD_MYSQL(*result, "task_id", task_id);
            EXTRACT_VARCHAR_FIELD_MYSQL(*result, "request_id", request_id);
            EXTRACT_VARCHAR_FIELD_MYSQL(*result, "rowkey_predicate", predicate);
            EXTRACT_VARCHAR_FIELD_MYSQL(*result, "table_name", table_name);
            EXTRACT_VARCHAR_FIELD_MYSQL(*result, "database_name", db_name);
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(ob_write_string(alloc, task_id, row.task_id_))) {
            } else if (OB_FAIL(ob_write_string(alloc, request_id, row.request_id_))) {
            } else if (OB_FAIL(ob_write_string(alloc, predicate, row.predicate_))) {
            } else if (OB_FAIL(ob_write_string(alloc, table_name, row.table_name_))) {
            } else if (OB_FAIL(ob_write_string(alloc, db_name, row.db_name_))) {
            } else if (OB_FAIL(rows.push_back(row))) {
              LOG_WARN("collect ai column task failed", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObAiColumnWorkerTask::build_update_sql(ObIAllocator &alloc,
                                           const ObIArray<AiTaskRow> &group,
                                           ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  UNUSED(alloc);
  ObSchemaGetterGuard guard;
  const ObTableSchema *table_schema = NULL;
  const AiTaskRow *head = group.empty() ? NULL : &group.at(0);
  if (group.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_runtime_schema_guard(guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(guard.get_table_schema(head->table_id_, table_schema)) || OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
  } else if (OB_FAIL(sql.append_fmt("UPDATE `%.*s`.`%.*s` SET ",
                                    head->db_name_.length(), head->db_name_.ptr(),
                                    head->table_name_.length(), head->table_name_.ptr()))) {
    LOG_WARN("build ai column update failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < group.count(); ++i) {
      const ObColumnSchemaV2 *column =
          table_schema->get_column_schema(group.at(i).column_id_);
      ObString ai_def;
      ObSEArray<ObString, 8> parts;
      ObSqlString call_text;
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ai column schema is missing", K(ret), K(group.at(i).column_id_));
      } else if (!column->get_ai_column_def(ai_def)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ai column definition is missing", K(ret),
                 K(column->get_column_name_str()));
      } else if (OB_FAIL(split_ai_def(ai_def, parts)) || parts.count() < 5) {
        ret = COVER_SUCC(OB_ERR_UNEXPECTED);
        LOG_WARN("ai column definition is malformed", K(ret), K(ai_def));
      } else if (OB_FAIL(ai_column_call_text(alloc,
                                             parts.at(4),
                                             ai_column_instruction(*column),
                                             call_text))) {
        LOG_WARN("build ai column call failed", K(ret));
      } else if (OB_FAIL(sql.append_fmt("%s`%.*s` = %.*s",
                                       0 == i ? "" : ", ",
                                       column->get_column_name_str().length(),
                                       column->get_column_name_str().ptr(),
                                       static_cast<int>(call_text.length()),
                                       call_text.ptr()))) {
        LOG_WARN("append ai column assignment failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql.append_fmt(" WHERE (%.*s)",
                               head->predicate_.length(), head->predicate_.ptr()))) {
      LOG_WARN("append ai column row predicate failed", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < group.count(); ++i) {
    const ObColumnSchemaV2 *column = NULL;
    ObSqlString request_column;
    if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_ISNULL(column = table_schema->get_column_schema(group.at(i).column_id_))) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(ai_request_column_name(column->get_column_name_str(), request_column))) {
    } else if (OB_FAIL(sql.append_fmt(" AND `%.*s` = '%.*s'",
                                      static_cast<int>(request_column.length()),
                                      request_column.ptr(),
                                      group.at(i).request_id_.length(),
                                      group.at(i).request_id_.ptr()))) {
      LOG_WARN("append ai column request guard failed", K(ret));
    }
  }
  return ret;
}

int ObAiColumnWorkerTask::process_group(ObIAllocator &alloc,
                                        const ObIArray<AiTaskRow> &group)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  bool computed = false;
  int64_t affected_rows = 0;
  sqlclient::ObISQLConnectionGuard conn_guard;
  sqlclient::ObISQLConnection *conn = NULL;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(GCTX.sql_proxy_->acquire_connection(conn_guard, 0))) {
    LOG_WARN("acquire ai column write connection failed", K(ret));
  } else if (OB_ISNULL(conn = conn_guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(build_update_sql(alloc, group, sql))) {
  } else if (OB_FAIL(conn->start_transaction(false))) {
    LOG_WARN("start ai column write transaction failed", K(ret));
  } else if (OB_FAIL(conn->execute_write(
                 ObString(static_cast<int32_t>(sql.length()), sql.ptr()),
                 affected_rows))) {
    // A request that did not finish leaves the column empty; the task is
    // retried or dropped below.
    LOG_INFO("ai column request did not finish", K(ret), K(sql));
    IGNORE_RETURN conn->rollback();
    ret = OB_SUCCESS;
  } else {
    computed = true;
    // The results and the removal of their tasks become visible together.
    for (int64_t i = 0; OB_SUCC(ret) && i < group.count(); ++i) {
      if (OB_FAIL(sql.assign_fmt("DELETE FROM oceanbase.__all_ai_column_task"
                                 " WHERE table_id = %lu AND column_id = %lu"
                                 " AND task_id = '%.*s' AND request_id = '%.*s'",
                                 group.at(i).table_id_,
                                 group.at(i).column_id_,
                                 group.at(i).task_id_.length(),
                                 group.at(i).task_id_.ptr(),
                                 group.at(i).request_id_.length(),
                                 group.at(i).request_id_.ptr()))) {
      } else if (OB_FAIL(conn->execute_write(
                     ObString(static_cast<int32_t>(sql.length()), sql.ptr()),
                     affected_rows))) {
        LOG_WARN("delete finished ai column task failed", K(ret), K(sql));
      }
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = conn->commit())) {
      LOG_WARN("commit ai column write failed", K(tmp_ret));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  }
  if (OB_SUCC(ret) && !computed) {
    if (OB_FAIL(finish_group(group))) {
      LOG_WARN("record ai column failure failed", K(ret));
    }
  }
  return ret;
}

int ObAiColumnWorkerTask::finish_group(const ObIArray<AiTaskRow> &group)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSchemaGetterGuard guard;
  const ObTableSchema *table_schema = NULL;
  bool guard_ok = false;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_NOT_INIT;
  } else {
    if (OB_SUCC(ObMultiVersionSchemaService::get_instance().get_runtime_schema_guard(guard))
        && OB_SUCC(guard.get_table_schema(group.at(0).table_id_, table_schema))
        && OB_NOT_NULL(table_schema)) {
      guard_ok = true;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < group.count(); ++i) {
      const AiTaskRow &row = group.at(i);
      int64_t retry_limit = 0;
      bool terminal = true;
      if (guard_ok) {
        const ObColumnSchemaV2 *column = table_schema->get_column_schema(row.column_id_);
        ObString ai_def;
        ObSEArray<ObString, 8> parts;
        if (NULL != column && column->get_ai_column_def(ai_def)
            && OB_SUCC(split_ai_def(ai_def, parts)) && parts.count() >= 5) {
          retry_limit = ai_def_int(parts.at(2));
          terminal = row.retry_count_ + 1 > retry_limit;
        }
      }
      if (terminal) {
        // Nothing more will be tried for this row, so the queue forgets it.
        if (OB_FAIL(sql.assign_fmt("DELETE FROM oceanbase.__all_ai_column_task"
                                   " WHERE table_id = %lu AND column_id = %lu"
                                   " AND task_id = '%.*s' AND request_id = '%.*s'",
                                   row.table_id_,
                                   row.column_id_,
                                   row.task_id_.length(), row.task_id_.ptr(),
                                   row.request_id_.length(), row.request_id_.ptr()))) {
          LOG_WARN("build ai column task drop failed", K(ret));
        }
      } else {
        if (OB_FAIL(sql.assign_fmt("UPDATE oceanbase.__all_ai_column_task"
                                   " SET state = 3, retry_count = retry_count + 1,"
                                   " eligible_ts = DATE_ADD(NOW(6), INTERVAL 500000 MICROSECOND)"
                                   " WHERE table_id = %lu AND column_id = %lu"
                                   " AND task_id = '%.*s' AND request_id = '%.*s'",
                                   row.table_id_,
                                   row.column_id_,
                                   row.task_id_.length(), row.task_id_.ptr(),
                                   row.request_id_.length(), row.request_id_.ptr()))) {
          LOG_WARN("build ai column task retry failed", K(ret));
        }
      }
      int64_t affected_rows = 0;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(exec_write(
                     ObString(static_cast<int32_t>(sql.length()), sql.ptr()),
                     affected_rows))) {
        LOG_WARN("write ai column task state failed", K(ret), K(sql));
      }
    }
  }
  return ret;
}

}  // observer
}  // oceanbase
