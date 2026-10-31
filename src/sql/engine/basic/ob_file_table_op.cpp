/**
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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/basic/ob_file_table_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

OB_DEF_SERIALIZE(ObFileTableSpec)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObFileTableSpec, ObOpSpec));
  OB_UNIS_ENCODE(path_);
  OB_UNIS_ENCODE(format_);
  OB_UNIS_ENCODE(has_header_);
  OB_UNIS_ENCODE(null_string_);
  OB_UNIS_ENCODE(delimiter_);
  OB_UNIS_ENCODE(quote_);
  OB_UNIS_ENCODE(file_size_);
  OB_UNIS_ENCODE(file_mtime_);
  int64_t column_count = col_names_.count();
  OB_UNIS_ENCODE(column_count);
  for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
    OB_UNIS_ENCODE(col_names_.at(i));
    OB_UNIS_ENCODE(col_types_.at(i));
    OB_UNIS_ENCODE(col_nullable_.at(i));
    OB_UNIS_ENCODE(col_output_idx_.at(i));
  }
  OB_UNIS_ENCODE(column_exprs_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObFileTableSpec)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObFileTableSpec, ObOpSpec));
  OB_UNIS_ADD_LEN(path_);
  OB_UNIS_ADD_LEN(format_);
  OB_UNIS_ADD_LEN(has_header_);
  OB_UNIS_ADD_LEN(null_string_);
  OB_UNIS_ADD_LEN(delimiter_);
  OB_UNIS_ADD_LEN(quote_);
  OB_UNIS_ADD_LEN(file_size_);
  OB_UNIS_ADD_LEN(file_mtime_);
  int64_t column_count = col_names_.count();
  OB_UNIS_ADD_LEN(column_count);
  for (int64_t i = 0; i < column_count; ++i) {
    OB_UNIS_ADD_LEN(col_names_.at(i));
    OB_UNIS_ADD_LEN(col_types_.at(i));
    OB_UNIS_ADD_LEN(col_nullable_.at(i));
    OB_UNIS_ADD_LEN(col_output_idx_.at(i));
  }
  OB_UNIS_ADD_LEN(column_exprs_);
  return len;
}

OB_DEF_DESERIALIZE(ObFileTableSpec)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObFileTableSpec, ObOpSpec));
  OB_UNIS_DECODE(path_);
  OB_UNIS_DECODE(format_);
  OB_UNIS_DECODE(has_header_);
  OB_UNIS_DECODE(null_string_);
  OB_UNIS_DECODE(delimiter_);
  OB_UNIS_DECODE(quote_);
  OB_UNIS_DECODE(file_size_);
  OB_UNIS_DECODE(file_mtime_);
  int64_t column_count = 0;
  OB_UNIS_DECODE(column_count);
  if (OB_SUCC(ret) && OB_FAIL(col_names_.init(column_count))) {
  } else if (OB_SUCC(ret) && OB_FAIL(col_types_.init(column_count))) {
  } else if (OB_SUCC(ret) && OB_FAIL(col_nullable_.init(column_count))) {
  } else if (OB_SUCC(ret) && OB_FAIL(col_output_idx_.init(column_count))) {
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
    ObString name;
    int64_t type = 0;
    int64_t nullable = 0;
    int64_t output_idx = -1;
    OB_UNIS_DECODE(name);
    OB_UNIS_DECODE(type);
    OB_UNIS_DECODE(nullable);
    OB_UNIS_DECODE(output_idx);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(col_names_.push_back(name))) {
    } else if (OB_FAIL(col_types_.push_back(type))) {
    } else if (OB_FAIL(col_nullable_.push_back(nullable))) {
    } else if (OB_FAIL(col_output_idx_.push_back(output_idx))) {
    }
  }
  OB_UNIS_DECODE(column_exprs_);
  return ret;
}

int ObFileTableOp::init_reader()
{
  int ret = OB_SUCCESS;
  ObFileScanOptions options;
  options.has_header_ = MY_SPEC.has_header_;
  options.null_string_ = MY_SPEC.null_string_;
  options.delimiter_ = static_cast<char>(MY_SPEC.delimiter_);
  options.quote_ = static_cast<char>(MY_SPEC.quote_);
  options.escape_ = options.quote_;
  ObSQLSessionInfo *session = ctx_.get_my_session();
  if (OB_NOT_NULL(session)) {
    options.tz_info_ = session->get_timezone_info();
  }
  if (OB_FAIL(ObFileScanReader::create(meta_alloc_, MY_SPEC.path_,
                                       static_cast<ObFileScanFormat>(MY_SPEC.format_),
                                       options, reader_))) {
    LOG_WARN("create file scan reader failed", K(ret), K(MY_SPEC.path_), K(MY_SPEC.format_));
  } else if (FILE_SCAN_GLOB != MY_SPEC.format_) {
    if (MY_SPEC.file_size_ > 0 && reader_->file_size() != MY_SPEC.file_size_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file size changed", K(ret), K(MY_SPEC.path_),
               K(MY_SPEC.file_size_), K(reader_->file_size()));
    }
  }
  if (OB_FAIL(ret) && NULL != reader_) {
    reader_->close();
    reader_ = NULL;
  }
  return ret;
}

int ObFileTableOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_reader())) {
  } else {
    eof_ = false;
    opened_ = true;
  }
  return ret;
}

int ObFileTableOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
  } else {
    if (NULL != reader_) {
      reader_->close();
      reader_ = NULL;
    }
    row_alloc_.reuse();
    meta_alloc_.reset();
    cells_.reuse();
    if (OB_FAIL(init_reader())) {
    } else {
      eof_ = false;
    }
  }
  return ret;
}

int ObFileTableOp::build_row()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.col_output_idx_.count(); ++i) {
    const int64_t output_idx = MY_SPEC.col_output_idx_.at(i);
    if (output_idx < 0 || output_idx >= MY_SPEC.column_exprs_.count()) {
      continue;
    }
    ObExpr *expr = MY_SPEC.column_exprs_.at(output_idx);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null column expr", K(ret));
    } else {
      ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
      const ObFileCell &cell = cells_.at(i);
      if (cell.is_null_) {
        datum.set_null();
      } else {
        switch (MY_SPEC.col_types_.at(i)) {
          case FILE_SCAN_TYPE_DOUBLE:
            datum.set_double(cell.double_value_);
            break;
          case FILE_SCAN_TYPE_DATE:
            datum.set_date(static_cast<int32_t>(cell.int_value_));
            break;
          case FILE_SCAN_TYPE_DATETIME:
            datum.set_datetime(cell.int_value_);
            break;
          case FILE_SCAN_TYPE_VARCHAR: {
            ObString value = cell.string_value_;
            if (0 == value.length()) {
              datum.set_string(ObString(0, ""));
            } else {
              datum.set_string(value);
            }
            break;
          }
          default:
            datum.set_int(cell.int_value_);
            break;
        }
      }
      expr->get_eval_info(eval_ctx_).evaluated_ = true;
    }
  }
  return ret;
}

int ObFileTableOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(reader_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("file reader is not inited", K(ret));
  } else if (eof_) {
    ret = OB_ITER_END;
  } else {
    row_alloc_.reuse();
    cells_.reuse();
    if (OB_FAIL(reader_->next_row(row_alloc_, cells_, eof_))) {
      LOG_WARN("read next row failed", K(ret));
    } else if (eof_) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(build_row())) {
    }
  }
  return ret;
}

int ObFileTableOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (NULL != reader_) {
    reader_->close();
    reader_ = NULL;
  }
  opened_ = false;
  return ret;
}

void ObFileTableOp::destroy()
{
  if (NULL != reader_) {
    reader_->close();
    reader_ = NULL;
  }
  row_alloc_.reset();
  meta_alloc_.reset();
  cells_.reset();
  ObOperator::destroy();
}

} // namespace sql
} // namespace oceanbase
