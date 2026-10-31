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
#include "sql/engine/basic/ob_file_scan_utils.h"
#include "lib/allocator/page_arena.h"
#include <sys/stat.h>


namespace oceanbase
{
namespace sql
{
using namespace common;

namespace
{

// ---------------------------------------------------------------------------
// Snappy block decompression. Parquet stores pages with the raw block format,
// so only the literal and copy tags are needed.
// ---------------------------------------------------------------------------
int snappy_uncompress(const char *input, const int64_t input_len, char *output, const int64_t output_len)
{
  int ret = OB_SUCCESS;
  int64_t in_pos = 0;
  int64_t out_pos = 0;
  uint64_t uncompressed_len = 0;
  int shift = 0;
  bool length_done = false;
  while (OB_SUCC(ret) && in_pos < input_len && !length_done) {
    const uint8_t byte = static_cast<uint8_t>(input[in_pos++]);
    uncompressed_len |= static_cast<uint64_t>(byte & 0x7f) << shift;
    length_done = (0 == (byte & 0x80));
    shift += 7;
    if (shift > 35 && !length_done) {
      ret = OB_INVALID_DATA;
      LOG_WARN("snappy length is too large", K(ret));
    }
  }
  if (OB_SUCC(ret) && (!length_done || static_cast<int64_t>(uncompressed_len) != output_len)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("snappy length mismatch", K(ret), K(uncompressed_len), K(output_len));
  }
  while (OB_SUCC(ret) && in_pos < input_len && out_pos < output_len) {
    const uint8_t tag = static_cast<uint8_t>(input[in_pos++]);
    const int32_t kind = tag & 0x03;
    if (0 == kind) {
      // Literal run, the length is stored in the upper 6 bits.
      int64_t literal_len = (tag >> 2) + 1;
      if (literal_len > 60) {
        const int32_t extra = static_cast<int32_t>(literal_len - 60);
        int64_t value = 0;
        for (int32_t i = 0; i < extra && in_pos < input_len; ++i) {
          value |= static_cast<int64_t>(static_cast<uint8_t>(input[in_pos++])) << (8 * i);
        }
        literal_len = value + 1;
      }
      if (in_pos + literal_len > input_len || out_pos + literal_len > output_len) {
        ret = OB_INVALID_DATA;
        LOG_WARN("snappy literal out of range", K(ret), K(literal_len));
      } else {
        MEMCPY(output + out_pos, input + in_pos, literal_len);
        in_pos += literal_len;
        out_pos += literal_len;
      }
    } else {
      int64_t copy_len = 0;
      int64_t offset = 0;
      if (1 == kind) {
        copy_len = 4 + ((tag >> 2) & 0x07);
        if (in_pos >= input_len) {
          ret = OB_INVALID_DATA;
        } else {
          offset = (static_cast<int64_t>(tag >> 5) << 8) | static_cast<uint8_t>(input[in_pos++]);
        }
      } else if (2 == kind) {
        copy_len = (tag >> 2) + 1;
        if (in_pos + 2 > input_len) {
          ret = OB_INVALID_DATA;
        } else {
          offset = static_cast<uint8_t>(input[in_pos])
                   | (static_cast<int64_t>(static_cast<uint8_t>(input[in_pos + 1])) << 8);
          in_pos += 2;
        }
      } else {
        copy_len = (tag >> 2) + 1;
        if (in_pos + 4 > input_len) {
          ret = OB_INVALID_DATA;
        } else {
          offset = static_cast<uint8_t>(input[in_pos])
                   | (static_cast<int64_t>(static_cast<uint8_t>(input[in_pos + 1])) << 8)
                   | (static_cast<int64_t>(static_cast<uint8_t>(input[in_pos + 2])) << 16)
                   | (static_cast<int64_t>(static_cast<uint8_t>(input[in_pos + 3])) << 24);
          in_pos += 4;
        }
      }
      if (OB_SUCC(ret)) {
        if (offset <= 0 || offset > out_pos || out_pos + copy_len > output_len) {
          ret = OB_INVALID_DATA;
          LOG_WARN("snappy copy out of range", K(ret), K(offset), K(copy_len), K(out_pos));
        } else {
          for (int64_t i = 0; i < copy_len; ++i) {
            output[out_pos + i] = output[out_pos - offset + i];
          }
          out_pos += copy_len;
        }
      }
    }
  }
  if (OB_SUCC(ret) && out_pos != output_len) {
    ret = OB_INVALID_DATA;
    LOG_WARN("snappy output size mismatch", K(ret), K(out_pos), K(output_len));
  }
  return ret;
}

// ---------------------------------------------------------------------------
// Thrift compact protocol reader. Parquet metadata is thrift encoded, the
// reader only needs field ids and the primitive types.
// ---------------------------------------------------------------------------
enum ObPqThriftType
{
  PQ_TT_STOP = 0,
  PQ_TT_TRUE = 1,
  PQ_TT_FALSE = 2,
  PQ_TT_BYTE = 3,
  PQ_TT_I16 = 4,
  PQ_TT_I32 = 5,
  PQ_TT_I64 = 6,
  PQ_TT_DOUBLE = 7,
  PQ_TT_BINARY = 8,
  PQ_TT_LIST = 9,
  PQ_TT_SET = 10,
  PQ_TT_MAP = 11,
  PQ_TT_STRUCT = 12,
};

class ObPqInput
{
public:
  ObPqInput(const char *buf, const int64_t len, const int64_t pos)
    : buf_(buf), len_(len), pos_(pos), last_field_id_(0), field_stack_(), depth_(0)
  {
    MEMSET(field_stack_, 0, sizeof(field_stack_));
  }
  int64_t pos() const { return pos_; }
  int read_byte(uint8_t &value)
  {
    int ret = OB_SUCCESS;
    if (pos_ >= len_) {
      ret = OB_INVALID_DATA;
    } else {
      value = static_cast<uint8_t>(buf_[pos_++]);
    }
    return ret;
  }
  int read_varint(uint64_t &value)
  {
    int ret = OB_SUCCESS;
    value = 0;
    int shift = 0;
    bool done = false;
    while (OB_SUCC(ret) && !done) {
      uint8_t byte = 0;
      if (OB_FAIL(read_byte(byte))) {
      } else {
        value |= static_cast<uint64_t>(byte & 0x7f) << shift;
        done = (0 == (byte & 0x80));
        shift += 7;
        if (shift > 63 && !done) {
          ret = OB_INVALID_DATA;
        }
      }
    }
    return ret;
  }
  int read_zigzag(int64_t &value)
  {
    int ret = OB_SUCCESS;
    uint64_t raw = 0;
    if (OB_FAIL(read_varint(raw))) {
    } else {
      value = static_cast<int64_t>(raw >> 1) ^ -static_cast<int64_t>(raw & 1);
    }
    return ret;
  }
  int read_i32(int32_t &value)
  {
    int ret = OB_SUCCESS;
    int64_t raw = 0;
    if (OB_FAIL(read_zigzag(raw))) {
    } else {
      value = static_cast<int32_t>(raw);
    }
    return ret;
  }
  int read_i64(int64_t &value) { return read_zigzag(value); }
  int read_binary(ObString &value)
  {
    int ret = OB_SUCCESS;
    uint64_t size = 0;
    if (OB_FAIL(read_varint(size))) {
    } else if (pos_ + static_cast<int64_t>(size) > len_) {
      ret = OB_INVALID_DATA;
    } else {
      value.assign_ptr(buf_ + pos_, static_cast<int32_t>(size));
      pos_ += static_cast<int64_t>(size);
    }
    return ret;
  }
  int read_list_header(int32_t &size, int32_t &elem_type)
  {
    int ret = OB_SUCCESS;
    uint8_t header = 0;
    if (OB_FAIL(read_byte(header))) {
    } else {
      size = (header >> 4) & 0x0f;
      elem_type = header & 0x0f;
      if (15 == size) {
        uint64_t raw = 0;
        if (OB_FAIL(read_varint(raw))) {
        } else {
          size = static_cast<int32_t>(raw);
        }
      }
    }
    return ret;
  }
  // Field ids are delta encoded, so every struct starts a new baseline. A struct
  // that appears as a field pushes in next_field, a struct that appears as a list
  // element is pushed by its caller; the terminating STOP pops either way.
  void push_struct()
  {
    if (depth_ < MAX_DEPTH) {
      field_stack_[depth_++] = last_field_id_;
    }
    last_field_id_ = 0;
  }
  void pop_struct()
  {
    last_field_id_ = (depth_ > 0) ? field_stack_[--depth_] : 0;
  }
  // Returns PQ_TT_STOP at the end of a struct.
  int next_field(int32_t &type, int32_t &field_id)
  {
    int ret = OB_SUCCESS;
    uint8_t header = 0;
    if (OB_FAIL(read_byte(header))) {
    } else if (PQ_TT_STOP == (header & 0x0f)) {
      type = PQ_TT_STOP;
      pop_struct();
    } else {
      const int32_t delta = (header >> 4) & 0x0f;
      type = header & 0x0f;
      if (0 == delta) {
        int64_t raw_id = 0;
        if (OB_FAIL(read_zigzag(raw_id))) {
        } else {
          field_id = static_cast<int32_t>(raw_id);
        }
      } else {
        field_id = last_field_id_ + delta;
      }
      last_field_id_ = field_id;
      if (PQ_TT_STRUCT == type) {
        push_struct();
      }
    }
    return ret;
  }
  int skip(const int32_t type)
  {
    int ret = OB_SUCCESS;
    int64_t int_value = 0;
    ObString binary;
    switch (type) {
      case PQ_TT_TRUE:
      case PQ_TT_FALSE:
        break;
      case PQ_TT_BYTE: {
        uint8_t byte = 0;
        ret = read_byte(byte);
        break;
      }
      case PQ_TT_I16:
      case PQ_TT_I32:
      case PQ_TT_I64:
        ret = read_zigzag(int_value);
        break;
      case PQ_TT_DOUBLE:
        if (pos_ + 8 > len_) {
          ret = OB_INVALID_DATA;
        } else {
          pos_ += 8;
        }
        break;
      case PQ_TT_BINARY:
        ret = read_binary(binary);
        break;
      case PQ_TT_LIST:
      case PQ_TT_SET: {
        int32_t size = 0;
        int32_t elem_type = 0;
        if (OB_FAIL(read_list_header(size, elem_type))) {
        }
        for (int32_t i = 0; OB_SUCC(ret) && i < size; ++i) {
          if (PQ_TT_TRUE == elem_type || PQ_TT_FALSE == elem_type) {
            // a boolean element is a single byte carrying its value
            uint8_t value = 0;
            ret = read_byte(value);
          } else {
            if (PQ_TT_STRUCT == elem_type) {
              push_struct();
            }
            ret = skip(elem_type);
          }
        }
        break;
      }
      case PQ_TT_MAP: {
        uint64_t size = 0;
        uint8_t types = 0;
        if (OB_FAIL(read_varint(size))) {
        } else if (OB_FAIL(read_byte(types))) {
        } else {
          const int32_t key_type = (types >> 4) & 0x0f;
          const int32_t value_type = types & 0x0f;
          for (uint64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
            if (OB_FAIL(skip(key_type))) {
            } else {
              ret = skip(value_type);
            }
          }
        }
        break;
      }
      case PQ_TT_STRUCT: {
        bool done = false;
        while (OB_SUCC(ret) && !done) {
          int32_t field_type = 0;
          int32_t field_id = 0;
          if (OB_FAIL(next_field(field_type, field_id))) {
          } else if (PQ_TT_STOP == field_type) {
            done = true;
          } else {
            ret = skip(field_type);
          }
        }
        break;
      }
      default:
        ret = OB_INVALID_DATA;
        break;
    }
    return ret;
  }
private:
  static const int32_t MAX_DEPTH = 32;
  const char *buf_;
  int64_t len_;
  int64_t pos_;
  int32_t last_field_id_;
  int32_t field_stack_[MAX_DEPTH];
  int32_t depth_;
};

// ---------------------------------------------------------------------------
// Parquet metadata
// ---------------------------------------------------------------------------
struct ObPqColumnChunk
{
  ObPqColumnChunk()
    : type_(0), codec_(0), num_values_(0), data_page_offset_(0), dict_page_offset_(-1),
      total_compressed_size_(0), path_() {}
  int32_t type_;
  int32_t codec_;
  int64_t num_values_;
  int64_t data_page_offset_;
  int64_t dict_page_offset_;
  int64_t total_compressed_size_;
  ObString path_;
};

struct ObPqSchemaLeaf
{
  ObPqSchemaLeaf() : name_(), type_(0), repetition_(0), max_def_level_(0) {}
  ObString name_;
  int32_t type_;
  int32_t repetition_;
  int32_t max_def_level_;
};

// One column of a row group in row order. Null rows get a placeholder entry so
// that all arrays stay aligned with the row index.
struct ObPqColumnData
{
  ObPqColumnData() : ints_(), doubles_(), strings_(), nulls_() {}
  common::ObSEArray<int64_t, 32> ints_;
  common::ObSEArray<double, 32> doubles_;
  common::ObSEArray<ObString, 32> strings_;
  common::ObSEArray<int8_t, 32> nulls_;
};

class ObParquetScanReader : public ObFileScanReader
{
public:
  ObParquetScanReader()
    : buf_(NULL), buf_len_(0), size_(0), mtime_(0), leaves_(), columns_(), row_count_(0),
      row_index_(0), row_group_count_(0), current_row_group_(0), row_group_rows_(0),
      page_is_compressed_(true) {}
  ~ObParquetScanReader() override { close(); }
  int open(ObIAllocator &alloc, const ObString &path, const ObFileScanOptions &options);
  int get_columns(ObIArray<ObFileColumnMeta> &columns) override;
  int next_row(ObIAllocator &alloc, ObIArray<ObFileCell> &cells, bool &eof) override;
  int64_t file_size() const override { return size_; }
  int64_t file_mtime() const override { return mtime_; }
  void close() override;
private:
  int read_file(const ObString &path);
  int parse_footer(bool &ok);
  int parse_metadata(ObPqInput &input, bool &ok);
  int parse_schema_element(ObPqInput &input, bool &is_leaf, int32_t &num_children);
  int parse_row_groups(ObPqInput &input);
  int parse_column_metadata(ObPqInput &input, ObPqColumnChunk &chunk);
  int parse_page_header(ObPqInput &input, int32_t &page_type, int32_t &uncompressed_size,
                        int32_t &compressed_size, int64_t &num_values, int32_t &encoding,
                        int32_t &def_level_encoding, int32_t &def_levels_size,
                        int32_t &rep_levels_size);
  int load_column(const ObPqColumnChunk &chunk, const ObPqSchemaLeaf &leaf, ObPqColumnData &data);
  int load_row_group(const int64_t group_index);
  int decode_data_page(const char *payload, const int64_t payload_len, const int64_t num_values,
                       const int32_t encoding, const int32_t def_level_encoding,
                       const int32_t levels_len, const ObPqSchemaLeaf &leaf,
                       const ObPqColumnData &dict, ObPqColumnData &data);
  int decode_plain_values(const char *payload, const int64_t payload_len, const int64_t value_count,
                          const int32_t physical_type, ObPqColumnData &out, int64_t &consumed);
  int decode_rle_hybrid(const char *payload, const int64_t payload_len, const int32_t bit_width,
                        const int64_t value_count, common::ObIArray<int64_t> &values,
                        int64_t &consumed);
  int decompress(const char *payload, const int64_t payload_len, const int32_t codec,
                 char *out, const int64_t out_len);
private:
  char *buf_;
  int64_t buf_len_;
  int64_t size_;
  int64_t mtime_;
  common::ObSEArray<ObPqSchemaLeaf, 16> leaves_;
  common::ObSEArray<ObPqColumnData, 4> columns_;
  common::ObSEArray<ObPqColumnChunk, 16> chunks_;
  int64_t row_count_;
  int64_t row_index_;
  // Row groups are decoded one at a time; row_index_ restarts at every group.
  int64_t row_group_count_;
  int64_t current_row_group_;
  int64_t row_group_rows_;
  // Holds decoded string values for the row group that is loaded right now.
  common::ObArenaAllocator value_alloc_;
  // A v2 page advertises whether only the values are compressed.
  bool page_is_compressed_;
};

int ObParquetScanReader::read_file(const ObString &path)
{
  int ret = OB_SUCCESS;
  char name[PATH_MAX];
  FILE *fp = NULL;
  if (path.length() <= 0 || path.length() >= PATH_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid file path", K(ret), K(path));
  } else {
    MEMCPY(name, path.ptr(), path.length());
    name[path.length()] = '\0';
    struct stat st;
    if (OB_ISNULL(fp = fopen(name, "rb"))) {
      ret = OB_FILE_NOT_EXIST;
      LOG_WARN("open parquet file failed", K(ret), K(path), K(errno));
    } else if (0 != stat(name, &st)) {
      ret = OB_IO_ERROR;
      LOG_WARN("stat parquet file failed", K(ret), K(path));
    } else {
      size_ = static_cast<int64_t>(st.st_size);
      mtime_ = static_cast<int64_t>(st.st_mtime);
      if (size_ < 12) {
        ret = OB_INVALID_DATA;
        LOG_WARN("parquet file is too small", K(ret), K(size_));
      } else if (OB_ISNULL(buf_ = static_cast<char *>(ob_malloc(size_, "PqScan")))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate parquet buffer failed", K(ret), K(size_));
      } else {
        buf_len_ = size_;
        const int64_t read_len = static_cast<int64_t>(fread(buf_, 1, size_, fp));
        if (read_len != size_) {
          ret = OB_IO_ERROR;
          LOG_WARN("read parquet file failed", K(ret), K(read_len), K(size_));
        }
      }
    }
    if (NULL != fp) {
      fclose(fp);
      fp = NULL;
    }
  }
  return ret;
}

void ObParquetScanReader::close()
{
  if (NULL != buf_) {
    ob_free(buf_);
    buf_ = NULL;
  }
  buf_len_ = 0;
  row_index_ = 0;
  row_group_rows_ = 0;
  value_alloc_.reset();
}

int ObParquetScanReader::parse_schema_element(ObPqInput &input, bool &is_leaf, int32_t &num_children)
{
  int ret = OB_SUCCESS;
  bool done = false;
  ObPqSchemaLeaf leaf;
  int32_t repetition = 0;
  num_children = 0;
  is_leaf = false;
  while (OB_SUCC(ret) && !done) {
    int32_t type = 0;
    int32_t field_id = 0;
    if (OB_FAIL(input.next_field(type, field_id))) {
    } else if (PQ_TT_STOP == type) {
      done = true;
    } else if (1 == field_id) {
      int32_t raw = 0;
      if (OB_FAIL(input.read_i32(raw))) {
      } else {
        leaf.type_ = raw;
        is_leaf = true;
      }
    } else if (3 == field_id) {
      ret = input.read_i32(repetition);
    } else if (4 == field_id) {
      ret = input.read_binary(leaf.name_);
    } else if (5 == field_id) {
      ret = input.read_i32(num_children);
    } else {
      ret = input.skip(type);
    }
  }
  if (OB_SUCC(ret) && is_leaf) {
    leaf.repetition_ = repetition;
    leaf.max_def_level_ = (1 == repetition) ? 1 : 0;
    ret = leaves_.push_back(leaf);
  }
  return ret;
}

int ObParquetScanReader::parse_column_metadata(ObPqInput &input, ObPqColumnChunk &chunk)
{
  int ret = OB_SUCCESS;
  bool done = false;
  while (OB_SUCC(ret) && !done) {
    int32_t type = 0;
    int32_t field_id = 0;
    int64_t int_value = 0;
    int32_t int32_value = 0;
    if (OB_FAIL(input.next_field(type, field_id))) {
    } else if (PQ_TT_STOP == type) {
      done = true;
    } else if (1 == field_id) {
      if (OB_FAIL(input.read_i32(int32_value))) {
      } else {
        chunk.type_ = int32_value;
      }
    } else if (3 == field_id) {
      // path_in_schema is a list of strings, only the last one matters.
      int32_t size = 0;
      int32_t elem_type = 0;
      if (OB_FAIL(input.read_list_header(size, elem_type))) {
      } else {
        for (int32_t i = 0; OB_SUCC(ret) && i < size; ++i) {
          if (OB_FAIL(input.read_binary(chunk.path_))) {
          }
        }
      }
    } else if (4 == field_id) {
      if (OB_FAIL(input.read_i32(int32_value))) {
      } else {
        chunk.codec_ = int32_value;
      }
    } else if (5 == field_id) {
      ret = input.read_i64(chunk.num_values_);
    } else if (7 == field_id) {
      ret = input.read_i64(chunk.total_compressed_size_);
    } else if (9 == field_id) {
      ret = input.read_i64(chunk.data_page_offset_);
    } else if (11 == field_id) {
      ret = input.read_i64(chunk.dict_page_offset_);
    } else {
      ret = input.skip(type);
    }
  }
  return ret;
}

int ObParquetScanReader::parse_row_groups(ObPqInput &input)
{
  int ret = OB_SUCCESS;
  int32_t group_count = 0;
  int32_t elem_type = 0;
  if (OB_FAIL(input.read_list_header(group_count, elem_type))) {
  }
  for (int32_t i = 0; OB_SUCC(ret) && i < group_count; ++i) {
    bool group_done = false;
    input.push_struct();
    while (OB_SUCC(ret) && !group_done) {
      int32_t group_type = 0;
      int32_t group_field = 0;
      if (OB_FAIL(input.next_field(group_type, group_field))) {
      } else if (PQ_TT_STOP == group_type) {
        group_done = true;
      } else if (1 == group_field) {
        int32_t chunk_count = 0;
        int32_t chunk_type = 0;
        if (OB_FAIL(input.read_list_header(chunk_count, chunk_type))) {
        }
        for (int32_t j = 0; OB_SUCC(ret) && j < chunk_count; ++j) {
          ObPqColumnChunk chunk;
          bool column_done = false;
          input.push_struct();
          while (OB_SUCC(ret) && !column_done) {
            int32_t column_type = 0;
            int32_t column_field = 0;
            if (OB_FAIL(input.next_field(column_type, column_field))) {
            } else if (PQ_TT_STOP == column_type) {
              column_done = true;
            } else if (3 == column_field) {
              ret = parse_column_metadata(input, chunk);
            } else {
              ret = input.skip(column_type);
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(chunks_.push_back(chunk))) {
          }
        }
      } else {
        ret = input.skip(group_type);
      }
    }
  }
  return ret;
}

int ObParquetScanReader::parse_metadata(ObPqInput &input, bool &ok)
{
  int ret = OB_SUCCESS;
  bool done = false;
  ok = false;
  while (OB_SUCC(ret) && !done) {
    int32_t type = 0;
    int32_t field_id = 0;
    if (OB_FAIL(input.next_field(type, field_id))) {
    } else if (PQ_TT_STOP == type) {
      done = true;
    } else if (2 == field_id) {
      int32_t size = 0;
      int32_t elem_type = 0;
      if (OB_FAIL(input.read_list_header(size, elem_type))) {
      }
      for (int32_t i = 0; OB_SUCC(ret) && i < size; ++i) {
        bool is_leaf = false;
        int32_t num_children = 0;
        input.push_struct();
        ret = parse_schema_element(input, is_leaf, num_children);
      }
    } else if (3 == field_id) {
      ret = input.read_i64(row_count_);
    } else if (4 == field_id) {
      ret = parse_row_groups(input);
    } else {
      ret = input.skip(type);
    }
  }
  if (OB_SUCC(ret)) {
    ok = true;
  }
  return ret;
}

int ObParquetScanReader::parse_footer(bool &ok)
{
  int ret = OB_SUCCESS;
  ok = false;
  if (OB_ISNULL(buf_) || buf_len_ < 12) {
    ret = OB_NOT_INIT;
    LOG_WARN("parquet file is not opened", K(ret));
  } else if (0 != MEMCMP(buf_, "PAR1", 4) || 0 != MEMCMP(buf_ + buf_len_ - 4, "PAR1", 4)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid parquet magic", K(ret));
  } else {
    int32_t footer_len = 0;
    MEMCPY(&footer_len, buf_ + buf_len_ - 8, 4);
    if (footer_len <= 0 || footer_len > buf_len_ - 12) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid parquet footer length", K(ret), K(footer_len));
    } else {
      ObPqInput input(buf_, buf_len_, buf_len_ - 8 - footer_len);
      ret = parse_metadata(input, ok);
    }
  }
  return ret;
}

int ObParquetScanReader::decompress(const char *payload, const int64_t payload_len, const int32_t codec,
                                    char *out, const int64_t out_len)
{
  int ret = OB_SUCCESS;
  if (0 == codec) {
    if (payload_len != out_len) {
      ret = OB_INVALID_DATA;
      LOG_WARN("uncompressed page size mismatch", K(ret), K(payload_len), K(out_len));
    } else {
      MEMCPY(out, payload, out_len);
    }
  } else if (1 == codec) {
    ret = snappy_uncompress(payload, payload_len, out, out_len);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported parquet compression codec", K(ret), K(codec));
  }
  return ret;
}

int ObParquetScanReader::decode_rle_hybrid(const char *payload, const int64_t payload_len,
                                           const int32_t bit_width, const int64_t value_count,
                                           common::ObIArray<int64_t> &values, int64_t &consumed)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  consumed = 0;
  while (OB_SUCC(ret) && pos < payload_len && values.count() < value_count) {
    uint64_t header = 0;
    int shift = 0;
    bool header_done = false;
    while (pos < payload_len && !header_done) {
      const uint8_t byte = static_cast<uint8_t>(payload[pos++]);
      header |= static_cast<uint64_t>(byte & 0x7f) << shift;
      header_done = (0 == (byte & 0x80));
      shift += 7;
    }
    if (!header_done) {
      ret = OB_INVALID_DATA;
      LOG_WARN("truncated rle header", K(ret));
    } else if (0 == (header & 1)) {
      // RLE run: one value of ceil(bit_width / 8) bytes repeated.
      const int64_t value_bytes = (bit_width + 7) / 8;
      const int64_t repeat = static_cast<int64_t>(header >> 1);
      if (pos + value_bytes > payload_len) {
        ret = OB_INVALID_DATA;
      } else {
        int64_t value = 0;
        for (int64_t i = 0; i < value_bytes; ++i) {
          value |= static_cast<int64_t>(static_cast<uint8_t>(payload[pos + i])) << (8 * i);
        }
        pos += value_bytes;
        for (int64_t i = 0; OB_SUCC(ret) && i < repeat && values.count() < value_count; ++i) {
          ret = values.push_back(value);
        }
      }
    } else {
      // Bit packed runs, always in groups of eight values.
      const int64_t groups = static_cast<int64_t>(header >> 1);
      const int64_t byte_count = groups * bit_width;
      if (pos + byte_count > payload_len) {
        ret = OB_INVALID_DATA;
      } else {
        int64_t bit_pos = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < groups * 8 && values.count() < value_count; ++i) {
          int64_t value = 0;
          for (int32_t b = 0; b < bit_width; ++b) {
            const int64_t byte_index = pos + (bit_pos / 8);
            const int32_t bit_index = static_cast<int32_t>(bit_pos % 8);
            if (byte_index < payload_len) {
              const uint8_t byte = static_cast<uint8_t>(payload[byte_index]);
              value |= static_cast<int64_t>((byte >> bit_index) & 1) << b;
            }
            ++bit_pos;
          }
          ret = values.push_back(value);
        }
        pos += byte_count;
      }
    }
  }
  consumed = pos;
  return ret;
}

int ObParquetScanReader::decode_plain_values(const char *payload, const int64_t payload_len,
                                             const int64_t value_count, const int32_t physical_type,
                                             ObPqColumnData &out, int64_t &consumed)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < value_count; ++i) {
    if (0 == physical_type) {
      // BOOLEAN values are bit packed, LSB first.
      const int64_t byte_index = i / 8;
      const int32_t bit_index = static_cast<int32_t>(i % 8);
      if (byte_index >= payload_len) {
        ret = OB_INVALID_DATA;
      } else {
        ret = out.ints_.push_back((static_cast<uint8_t>(payload[byte_index]) >> bit_index) & 1);
      }
    } else if (1 == physical_type) {
      if (pos + 4 > payload_len) {
        ret = OB_INVALID_DATA;
      } else {
        int32_t value = 0;
        MEMCPY(&value, payload + pos, 4);
        pos += 4;
        ret = out.ints_.push_back(static_cast<int64_t>(value));
      }
    } else if (2 == physical_type || 3 == physical_type) {
      const int32_t width = (2 == physical_type) ? 8 : 12;
      if (pos + width > payload_len) {
        ret = OB_INVALID_DATA;
      } else {
        int64_t value = 0;
        MEMCPY(&value, payload + pos, 8);
        pos += width;
        ret = out.ints_.push_back(value);
      }
    } else if (4 == physical_type) {
      if (pos + 4 > payload_len) {
        ret = OB_INVALID_DATA;
      } else {
        float value = 0;
        MEMCPY(&value, payload + pos, 4);
        pos += 4;
        ret = out.doubles_.push_back(static_cast<double>(value));
      }
    } else if (5 == physical_type) {
      if (pos + 8 > payload_len) {
        ret = OB_INVALID_DATA;
      } else {
        double value = 0;
        MEMCPY(&value, payload + pos, 8);
        pos += 8;
        ret = out.doubles_.push_back(value);
      }
    } else if (6 == physical_type) {
      int32_t str_len = 0;
      if (pos + 4 > payload_len) {
        ret = OB_INVALID_DATA;
      } else {
        MEMCPY(&str_len, payload + pos, 4);
        pos += 4;
        if (str_len < 0 || pos + str_len > payload_len) {
          ret = OB_INVALID_DATA;
        } else {
          // The page buffer is released at the end of this call, so the value
          // text has to be copied out of it.
          ObString copy;
          ObString value;
          value.assign_ptr(payload + pos, str_len);
          pos += str_len;
          if (OB_FAIL(ob_write_string(value_alloc_, value, copy))) {
          } else {
            ret = out.strings_.push_back(copy);
          }
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported parquet physical type", K(ret), K(physical_type));
    }
  }
  consumed = pos;
  return ret;
}

int ObParquetScanReader::decode_data_page(const char *payload, const int64_t payload_len,
                                          const int64_t num_values, const int32_t encoding,
                                          const int32_t def_level_encoding, const int32_t levels_len,
                                          const ObPqSchemaLeaf &leaf, const ObPqColumnData &dict,
                                          ObPqColumnData &data)
{
  int ret = OB_SUCCESS;
  int64_t value_pos = 0;
  ObSEArray<int8_t, 32> nulls;
  if (levels_len >= 0) {
    // A v2 page always reserves the level section even when the column is
    // required and no level has to be decoded.
    value_pos = levels_len;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < num_values; ++i) {
    ret = nulls.push_back(leaf.max_def_level_ > 0 ? 1 : 0);
  }
  if (OB_SUCC(ret) && leaf.max_def_level_ > 0) {
    ObSEArray<int64_t, 32> def_levels;
    int64_t consumed = 0;
    int32_t bit_width = 1;
    while ((1LL << bit_width) <= leaf.max_def_level_) {
      ++bit_width;
    }
    const char *levels = payload;
    int64_t levels_size = 0;
    if (levels_len >= 0) {
      // Data page v2 keeps the levels uncompressed and their size in the header.
      levels_size = levels_len;
      value_pos = levels_len;
    } else if (payload_len >= 4) {
      // Data page v1 prefixes an rle encoded level section with its length.
      int32_t prefix = 0;
      MEMCPY(&prefix, payload, 4);
      levels = payload + 4;
      levels_size = (prefix >= 0 && 4 + prefix <= payload_len) ? prefix : payload_len - 4;
      value_pos = 4 + levels_size;
    } else {
      ret = OB_INVALID_DATA;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(decode_rle_hybrid(levels, levels_size, bit_width, num_values, def_levels,
                                         consumed))) {
      LOG_WARN("decode definition levels failed", K(ret), K(def_level_encoding));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < num_values; ++i) {
        const int64_t level = i < def_levels.count() ? def_levels.at(i) : 0;
        nulls.at(i) = (level >= leaf.max_def_level_) ? 0 : 1;
      }
    }
  }
  int64_t valid_count = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < nulls.count(); ++i) {
    if (0 == nulls.at(i)) {
      ++valid_count;
    }
  }
  ObPqColumnData values;
  if (OB_SUCC(ret) && valid_count > 0) {
    if (0 == encoding) {
      int64_t consumed = 0;
      if (OB_FAIL(decode_plain_values(payload + value_pos, payload_len - value_pos, valid_count,
                                      leaf.type_, values, consumed))) {
      }
    } else if (2 == encoding || 8 == encoding) {
      if (value_pos >= payload_len) {
        ret = OB_INVALID_DATA;
      } else {
        const int32_t bit_width = static_cast<int32_t>(static_cast<uint8_t>(payload[value_pos]));
        ObSEArray<int64_t, 32> indices;
        int64_t consumed = 0;
        if (OB_FAIL(decode_rle_hybrid(payload + value_pos + 1, payload_len - value_pos - 1, bit_width,
                                      valid_count, indices, consumed))) {
        }
        const int64_t dict_size = dict.strings_.count() + dict.doubles_.count() + dict.ints_.count();
        for (int64_t i = 0; OB_SUCC(ret) && i < indices.count(); ++i) {
          const int64_t index = indices.at(i);
          if (index < 0 || index >= dict_size) {
            ret = OB_INVALID_DATA;
            LOG_WARN("dictionary index out of range", K(ret), K(index), K(dict_size));
          } else if (dict.strings_.count() > 0) {
            ret = values.strings_.push_back(dict.strings_.at(index));
          } else if (dict.doubles_.count() > 0) {
            ret = values.doubles_.push_back(dict.doubles_.at(index));
          } else {
            ret = values.ints_.push_back(dict.ints_.at(index));
          }
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported parquet encoding", K(ret), K(encoding));
    }
  }
  // Interleave the decoded values with placeholders, so that the arrays of the
  // column stay aligned with the row index.
  if (OB_SUCC(ret)) {
    int64_t valid_index = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < num_values; ++i) {
      const bool is_null = (0 != nulls.at(i));
      if (OB_FAIL(data.nulls_.push_back(is_null ? 1 : 0))) {
      } else if (values.strings_.count() > 0) {
        ret = data.strings_.push_back(is_null ? ObString() : values.strings_.at(valid_index));
        valid_index += is_null ? 0 : 1;
      } else if (values.doubles_.count() > 0) {
        ret = data.doubles_.push_back(is_null ? 0 : values.doubles_.at(valid_index));
        valid_index += is_null ? 0 : 1;
      } else {
        ret = data.ints_.push_back(is_null ? 0 : values.ints_.at(valid_index));
        valid_index += is_null ? 0 : 1;
      }
    }
  }
  return ret;
}

int ObParquetScanReader::parse_page_header(ObPqInput &input, int32_t &page_type,
                                           int32_t &uncompressed_size, int32_t &compressed_size,
                                           int64_t &num_values, int32_t &encoding,
                                           int32_t &def_level_encoding, int32_t &def_levels_size,
                                           int32_t &rep_levels_size)
{
  int ret = OB_SUCCESS;
  bool done = false;
  num_values = 0;
  encoding = 0;
  def_level_encoding = 3;
  def_levels_size = 0;
  rep_levels_size = 0;
  page_is_compressed_ = true;
  while (OB_SUCC(ret) && !done) {
    int32_t type = 0;
    int32_t field_id = 0;
    if (OB_FAIL(input.next_field(type, field_id))) {
    } else if (PQ_TT_STOP == type) {
      done = true;
    } else if (1 == field_id) {
      ret = input.read_i32(page_type);
    } else if (2 == field_id) {
      ret = input.read_i32(uncompressed_size);
    } else if (3 == field_id) {
      ret = input.read_i32(compressed_size);
    } else if (5 == field_id) {
      // DataPageHeader of a v1 page.
      bool sub_done = false;
      while (OB_SUCC(ret) && !sub_done) {
        int32_t sub_type = 0;
        int32_t sub_field = 0;
        if (OB_FAIL(input.next_field(sub_type, sub_field))) {
        } else if (PQ_TT_STOP == sub_type) {
          sub_done = true;
        } else if (1 == sub_field) {
          int32_t value = 0;
          if (OB_FAIL(input.read_i32(value))) {
          } else {
            num_values = value;
          }
        } else if (2 == sub_field) {
          ret = input.read_i32(encoding);
        } else if (3 == sub_field) {
          ret = input.read_i32(def_level_encoding);
        } else {
          ret = input.skip(sub_type);
        }
      }
    } else if (8 == field_id) {
      // DataPageHeaderV2 of a v2 page.
      bool sub_done = false;
      while (OB_SUCC(ret) && !sub_done) {
        int32_t sub_type = 0;
        int32_t sub_field = 0;
        if (OB_FAIL(input.next_field(sub_type, sub_field))) {
        } else if (PQ_TT_STOP == sub_type) {
          sub_done = true;
        } else if (1 == sub_field) {
          ret = input.read_i64(num_values);
        } else if (4 == sub_field) {
          ret = input.read_i32(encoding);
        } else if (5 == sub_field) {
          ret = input.read_i32(def_levels_size);
        } else if (6 == sub_field) {
          ret = input.read_i32(rep_levels_size);
        } else if (7 == sub_field) {
          // a thrift bool field carries its value in the field type
          page_is_compressed_ = (PQ_TT_TRUE == sub_type);
        } else {
          ret = input.skip(sub_type);
        }
      }
    } else if (7 == field_id) {
      // DictionaryPageHeader.
      bool sub_done = false;
      while (OB_SUCC(ret) && !sub_done) {
        int32_t sub_type = 0;
        int32_t sub_field = 0;
        if (OB_FAIL(input.next_field(sub_type, sub_field))) {
        } else if (PQ_TT_STOP == sub_type) {
          sub_done = true;
        } else if (1 == sub_field) {
          int32_t value = 0;
          if (OB_FAIL(input.read_i32(value))) {
          } else {
            num_values = value;
          }
        } else if (2 == sub_field) {
          ret = input.read_i32(encoding);
        } else {
          ret = input.skip(sub_type);
        }
      }
    } else {
      ret = input.skip(type);
    }
  }
  return ret;
}

int ObParquetScanReader::load_column(const ObPqColumnChunk &chunk, const ObPqSchemaLeaf &leaf,
                                     ObPqColumnData &data)
{
  int ret = OB_SUCCESS;
  ObPqColumnData dict;
  char *page_buf = NULL;
  int64_t page_buf_size = 0;
  int64_t pos = (chunk.dict_page_offset_ >= 0) ? chunk.dict_page_offset_ : chunk.data_page_offset_;
  int64_t decoded = 0;
  int64_t end = buf_len_ - 8;
  if (chunk.total_compressed_size_ > 0 && pos + chunk.total_compressed_size_ <= buf_len_) {
    end = pos + chunk.total_compressed_size_;
  }
  while (OB_SUCC(ret) && pos < end && decoded < chunk.num_values_) {
    ObPqInput header_input(buf_, buf_len_, pos);
    int32_t page_type = 0;
    int32_t uncompressed_size = 0;
    int32_t compressed_size = 0;
    int64_t num_values = 0;
    int32_t encoding = 0;
    int32_t def_level_encoding = 3;
    int32_t def_levels_size = 0;
    int32_t rep_levels_size = 0;
    if (OB_FAIL(parse_page_header(header_input, page_type, uncompressed_size, compressed_size,
                                  num_values, encoding, def_level_encoding, def_levels_size,
                                  rep_levels_size))) {
    } else if (compressed_size <= 0 || uncompressed_size <= 0
               || header_input.pos() + compressed_size > buf_len_) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid parquet page size", K(ret), K(compressed_size), K(uncompressed_size));
    } else {
      const char *payload = buf_ + header_input.pos();
      if (uncompressed_size > page_buf_size) {
        if (NULL != page_buf) {
          ob_free(page_buf);
          page_buf = NULL;
        }
        if (OB_ISNULL(page_buf = static_cast<char *>(ob_malloc(uncompressed_size, "PqPage")))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate page buffer failed", K(ret), K(uncompressed_size));
        } else {
          page_buf_size = uncompressed_size;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (2 == page_type) {
        int64_t consumed = 0;
        dict.ints_.reuse();
        dict.doubles_.reuse();
        dict.strings_.reuse();
        if (OB_FAIL(decompress(payload, compressed_size, chunk.codec_, page_buf, uncompressed_size))) {
        } else if (OB_FAIL(decode_plain_values(page_buf, uncompressed_size, num_values, leaf.type_,
                                               dict, consumed))) {
          LOG_WARN("decode dictionary page failed", K(ret));
        }
      } else if (0 == page_type) {
        if (OB_FAIL(decompress(payload, compressed_size, chunk.codec_, page_buf, uncompressed_size))) {
        } else if (OB_FAIL(decode_data_page(page_buf, uncompressed_size, num_values, encoding,
                                            def_level_encoding, -1, leaf, dict, data))) {
          LOG_WARN("decode data page failed", K(ret), K(encoding));
        } else {
          decoded += num_values;
        }
      } else if (3 == page_type) {
        const int64_t levels_len = def_levels_size + rep_levels_size;
        if (levels_len < 0 || levels_len > compressed_size || levels_len >= uncompressed_size) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid v2 level size", K(ret), K(levels_len));
        } else {
          // Data page v2 keeps the levels uncompressed and compresses only the values.
          MEMCPY(page_buf, payload, levels_len);
          if (!page_is_compressed_ && compressed_size != uncompressed_size) {
            ret = OB_INVALID_DATA;
          } else if (!page_is_compressed_) {
            MEMCPY(page_buf + levels_len, payload + levels_len, compressed_size - levels_len);
            if (OB_FAIL(decode_data_page(page_buf, uncompressed_size, num_values, encoding,
                                         def_level_encoding, levels_len, leaf, dict, data))) {
            } else {
              decoded += num_values;
            }
          } else if (OB_FAIL(decompress(payload + levels_len, compressed_size - levels_len, chunk.codec_,
                                        page_buf + levels_len, uncompressed_size - levels_len))) {
          } else if (OB_FAIL(decode_data_page(page_buf, uncompressed_size, num_values, encoding,
                                              def_level_encoding, levels_len, leaf, dict, data))) {
            LOG_WARN("decode data page v2 failed", K(ret), K(encoding));
          } else {
            decoded += num_values;
          }
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported parquet page type", K(ret), K(page_type));
      }
      pos = header_input.pos() + compressed_size;
    }
  }
  if (NULL != page_buf) {
    ob_free(page_buf);
    page_buf = NULL;
  }
  return ret;
}

int ObParquetScanReader::open(ObIAllocator &alloc, const ObString &path,
                              const ObFileScanOptions &options)
{
  int ret = OB_SUCCESS;
  bool ok = false;
  UNUSED(alloc);
  UNUSED(options);
  if (OB_FAIL(read_file(path))) {
  } else if (OB_FAIL(parse_footer(ok))) {
  } else if (!ok) {
    ret = OB_INVALID_DATA;
  } else if (leaves_.empty()) {
    ret = OB_INVALID_DATA;
    LOG_WARN("parquet file has no column", K(ret));
  } else if (0 == chunks_.count() || 0 != chunks_.count() % leaves_.count()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("flattened or nested parquet columns are not supported",
             K(ret), K(chunks_.count()), K(leaves_.count()));
  } else {
    // One row group is decoded at a time, so a file with many row groups never
    // holds more than a single group in memory.
    row_group_count_ = chunks_.count() / leaves_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < leaves_.count(); ++i) {
      ObPqColumnData data;
      ret = columns_.push_back(data);
    }
    if (OB_SUCC(ret) && OB_FAIL(load_row_group(0))) {
      LOG_WARN("load first parquet row group failed", K(ret));
    }
  }
  return ret;
}

int ObParquetScanReader::load_row_group(const int64_t group_index)
{
  int ret = OB_SUCCESS;
  const int64_t column_count = leaves_.count();
  const int64_t first = group_index * column_count;
  row_index_ = 0;
  row_group_rows_ = 0;
  current_row_group_ = group_index;
  value_alloc_.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
    ObPqColumnData &data = columns_.at(i);
    data.ints_.reuse();
    data.doubles_.reuse();
    data.strings_.reuse();
    data.nulls_.reuse();
    if (OB_FAIL(load_column(chunks_.at(first + i), leaves_.at(i), data))) {
      LOG_WARN("load parquet column failed", K(ret), K(group_index), K(i));
    } else if (data.nulls_.count() > row_group_rows_) {
      row_group_rows_ = data.nulls_.count();
    }
  }
  return ret;
}

int ObParquetScanReader::get_columns(ObIArray<ObFileColumnMeta> &columns)
{
  int ret = OB_SUCCESS;
  if (leaves_.empty()) {
    ret = OB_NOT_INIT;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < leaves_.count(); ++i) {
    ObFileColumnMeta meta;
    meta.name_ = leaves_.at(i).name_;
    switch (leaves_.at(i).type_) {
      case 0: meta.type_ = FILE_SCAN_TYPE_BOOLEAN; break;
      case 1:
      case 2: meta.type_ = FILE_SCAN_TYPE_BIGINT; break;
      case 4:
      case 5: meta.type_ = FILE_SCAN_TYPE_DOUBLE; break;
      default: meta.type_ = FILE_SCAN_TYPE_VARCHAR; break;
    }
    meta.nullable_ = (1 == leaves_.at(i).repetition_);
    if (OB_FAIL(columns.push_back(meta))) {
    }
  }
  return ret;
}

int ObParquetScanReader::next_row(ObIAllocator &alloc, ObIArray<ObFileCell> &cells, bool &eof)
{
  int ret = OB_SUCCESS;
  eof = false;
  UNUSED(alloc);
  if (row_index_ >= row_group_rows_
      && current_row_group_ + 1 < row_group_count_
      && OB_FAIL(load_row_group(current_row_group_ + 1))) {
    LOG_WARN("load next parquet row group failed", K(ret), K(current_row_group_));
  }
  if (OB_FAIL(ret)) {
  } else if (row_index_ >= row_group_rows_) {
    eof = true;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < columns_.count(); ++i) {
      ObFileCell cell;
      const ObPqColumnData &column = columns_.at(i);
      if (row_index_ >= column.nulls_.count() || 0 != column.nulls_.at(row_index_)) {
        cell.is_null_ = true;
      } else if (column.strings_.count() > 0) {
        cell.is_null_ = false;
        cell.string_value_ = column.strings_.at(row_index_);
      } else if (column.doubles_.count() > 0) {
        cell.is_null_ = false;
        cell.double_value_ = column.doubles_.at(row_index_);
      } else if (column.ints_.count() > 0) {
        cell.is_null_ = false;
        cell.int_value_ = column.ints_.at(row_index_);
      } else {
        cell.is_null_ = true;
      }
      if (OB_FAIL(ret)) {
      } else {
        ret = cells.push_back(cell);
      }
    }
    ++row_index_;
  }
  return ret;
}

} // namespace

int ob_create_parquet_reader(ObIAllocator &alloc,
                             const ObString &path,
                             const ObFileScanOptions &options,
                             ObFileScanReader *&reader)
{
  int ret = OB_SUCCESS;
  void *mem = NULL;
  reader = NULL;
  if (OB_ISNULL(mem = alloc.alloc(sizeof(ObParquetScanReader)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate parquet reader failed", K(ret));
  } else {
    ObParquetScanReader *parquet_reader = new (mem) ObParquetScanReader();
    if (OB_FAIL(parquet_reader->open(alloc, path, options))) {
      LOG_WARN("open parquet file failed", K(ret), K(path));
      parquet_reader->close();
    } else {
      reader = parquet_reader;
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
