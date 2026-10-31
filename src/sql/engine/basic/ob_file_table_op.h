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

#ifndef SRC_SQL_ENGINE_BASIC_OB_FILE_TABLE_OP_H_
#define SRC_SQL_ENGINE_BASIC_OB_FILE_TABLE_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/basic/ob_file_scan_utils.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace sql
{

class ObFileTableSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObFileTableSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      path_(),
      format_(0),
      has_header_(true),
      null_string_(),
      delimiter_(','),
      quote_('"'),
      col_names_(alloc),
      col_types_(alloc),
      col_nullable_(alloc),
      col_output_idx_(alloc),
      column_exprs_(alloc),
      file_size_(0),
      file_mtime_(0) {}
  // File location and format.
  ObString path_;
  int64_t format_;
  bool has_header_;
  ObString null_string_;
  int64_t delimiter_;
  int64_t quote_;
  // Inferred file columns, in file order.
  common::ObFixedArray<ObString, common::ObIAllocator> col_names_;
  common::ObFixedArray<int64_t, common::ObIAllocator> col_types_;
  common::ObFixedArray<int64_t, common::ObIAllocator> col_nullable_;
  // For each file column, the index in column_exprs_ or -1 when unused.
  common::ObFixedArray<int64_t, common::ObIAllocator> col_output_idx_;
  common::ObFixedArray<ObExpr *, common::ObIAllocator> column_exprs_;
  // Fingerprint of the file at resolve time, used to detect changes.
  int64_t file_size_;
  int64_t file_mtime_;
};

class ObFileTableOp : public ObOperator
{
public:
  ObFileTableOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input),
      reader_(NULL),
      row_alloc_(),
      meta_alloc_(),
      cells_(),
      eof_(false),
      opened_(false) {}
  virtual ~ObFileTableOp() { reader_ = NULL; }
  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_close() override;
  virtual void destroy() override;
private:
  int init_reader();
  int build_row();
  ObFileScanReader *reader_;
  // Row values point into row_alloc_ and only live for one row.
  common::ObArenaAllocator row_alloc_;
  // Reader metadata (column names, file content) must outlive every row.
  common::ObArenaAllocator meta_alloc_;
  common::ObSEArray<ObFileCell, 8> cells_;
  bool eof_;
  bool opened_;
};

} // namespace sql
} // namespace oceanbase

#endif // SRC_SQL_ENGINE_BASIC_OB_FILE_TABLE_OP_H_
