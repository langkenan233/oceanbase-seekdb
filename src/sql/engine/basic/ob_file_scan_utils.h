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

#ifndef SRC_SQL_ENGINE_BASIC_OB_FILE_SCAN_UTILS_H_
#define SRC_SQL_ENGINE_BASIC_OB_FILE_SCAN_UTILS_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_array.h"
#include "share/schema/ob_schema_struct.h"
#include "common/object/ob_object.h"
#include "common/timezone/ob_time_convert.h"

namespace oceanbase
{
namespace sql
{

// Physical format of a file table.
enum ObFileScanFormat
{
  FILE_SCAN_CSV = 0,
  FILE_SCAN_JSON = 1,
  FILE_SCAN_PARQUET = 2,
  FILE_SCAN_GLOB = 3,
};

// Logical type inferred for a file column. It is kept apart from ObObjType
// because the display name (see DESCRIBE) and the physical column semantics
// are not always the same, e.g. BOOLEAN is stored as a numeric column.
enum ObFileScanType
{
  FILE_SCAN_TYPE_INVALID = 0,
  FILE_SCAN_TYPE_BIGINT = 1,
  FILE_SCAN_TYPE_DOUBLE = 2,
  FILE_SCAN_TYPE_BOOLEAN = 3,
  FILE_SCAN_TYPE_VARCHAR = 4,
  FILE_SCAN_TYPE_DATE = 5,
  FILE_SCAN_TYPE_DATETIME = 6,
};

const char *ob_file_scan_type_name(const ObFileScanType type);
ObObjType ob_file_scan_obj_type(const ObFileScanType type);

struct ObFileColumnMeta
{
  ObFileColumnMeta() : name_(), type_(FILE_SCAN_TYPE_INVALID), nullable_(true) {}
  ObString name_;
  ObFileScanType type_;
  bool nullable_;
};

struct ObFileScanOptions
{
  ObFileScanOptions()
    : has_header_(true),
      null_string_(),
      delimiter_(','),
      quote_('"'),
      escape_('"'),
      allow_nested_(false),
      tz_info_(NULL) {}
  bool has_header_;
  ObString null_string_;
  char delimiter_;
  char quote_;
  char escape_;
  bool allow_nested_;
  const common::ObTimeZoneInfo *tz_info_;
};

struct ObFileCell
{
  ObFileCell() : is_null_(true), int_value_(0), double_value_(0), string_value_() {}
  bool is_null_;
  int64_t int_value_;
  double double_value_;
  ObString string_value_;
};

// Reads one file and produces rows in a streaming fashion. The schema is
// inferred when the reader is created, rows are produced on demand.
class ObFileScanReader
{
public:
  ObFileScanReader() {}
  virtual ~ObFileScanReader() {}
  // Opens the file and infers the column layout. The file is rewound
  // afterwards so that next_row() starts from the first data row.
  static int create(ObIAllocator &alloc,
                    const ObString &path,
                    const ObFileScanFormat format,
                    const ObFileScanOptions &options,
                    ObFileScanReader *&reader);
  virtual int get_columns(ObIArray<ObFileColumnMeta> &columns) = 0;
  virtual int next_row(ObIAllocator &alloc,
                       ObIArray<ObFileCell> &cells,
                       bool &eof) = 0;
  virtual int64_t file_size() const = 0;
  virtual int64_t file_mtime() const = 0;
  virtual void close() {}

private:
  DISALLOW_COPY_AND_ASSIGN(ObFileScanReader);
};

// Lexical helpers shared by CSV/JSONL inference and by the scan itself.
class ObFileScanUtils
{
public:
  static bool is_integer_text(const ObString &text, int64_t &value);
  static bool is_double_text(const ObString &text, double &value);
  static bool is_boolean_text(const ObString &text, bool &value);
  static bool is_date_text(const ObString &text, int32_t &value);
  static bool is_datetime_text(const ObString &text,
                               const common::ObTimeZoneInfo *tz_info,
                               int64_t &value);
  // Folds a newly observed value into the column type.
  static ObFileScanType widen_type(const ObFileScanType current, const ObFileScanType incoming);
  // Maps a CSV scalar into the physical type it suggests.
  static ObFileScanType probe_text_type(const ObString &text,
                                        const common::ObTimeZoneInfo *tz_info);
  // Maps a JSON string into the physical type it suggests.
  static ObFileScanType probe_string_type(const ObString &text,
                                          const common::ObTimeZoneInfo *tz_info);
  // Appends a de-duplicated column name, id -> id__2, id__3 ...
  static int make_unique_column_name(ObIAllocator &alloc,
                                     const ObString &name,
                                     ObIArray<ObFileColumnMeta> &columns,
                                     ObString &unique_name);
  // Resolves the path against realpath and verifies it stays within root.
  static int check_file_path(const ObString &path,
                             const ObString &secure_root,
                             bool &allowed);
  static int expand_glob(ObIAllocator &alloc,
                         const ObString &pattern,
                         ObIArray<ObString> &files);
};

// Implemented in ob_file_parquet_reader.cpp.
int ob_create_parquet_reader(ObIAllocator &alloc,
                             const ObString &path,
                             const ObFileScanOptions &options,
                             ObFileScanReader *&reader);

} // namespace sql
} // namespace oceanbase

#endif // SRC_SQL_ENGINE_BASIC_OB_FILE_SCAN_UTILS_H_
