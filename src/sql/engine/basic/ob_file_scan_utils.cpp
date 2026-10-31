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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <math.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <glob.h>
#include <limits.h>
#include "sql/engine/basic/ob_file_scan_utils.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

const char *ob_file_scan_type_name(const ObFileScanType type)
{
  const char *name = "VARCHAR";
  switch (type) {
    case FILE_SCAN_TYPE_BIGINT: name = "BIGINT"; break;
    case FILE_SCAN_TYPE_DOUBLE: name = "DOUBLE"; break;
    case FILE_SCAN_TYPE_BOOLEAN: name = "BOOLEAN"; break;
    case FILE_SCAN_TYPE_DATE: name = "DATE"; break;
    case FILE_SCAN_TYPE_DATETIME: name = "DATETIME"; break;
    case FILE_SCAN_TYPE_VARCHAR: name = "VARCHAR"; break;
    default: name = "VARCHAR"; break;
  }
  return name;
}

ObObjType ob_file_scan_obj_type(const ObFileScanType type)
{
  ObObjType obj_type = ObVarcharType;
  switch (type) {
    case FILE_SCAN_TYPE_BIGINT: obj_type = ObIntType; break;
    case FILE_SCAN_TYPE_BOOLEAN: obj_type = ObIntType; break;
    case FILE_SCAN_TYPE_DOUBLE: obj_type = ObDoubleType; break;
    case FILE_SCAN_TYPE_DATE: obj_type = ObDateType; break;
    case FILE_SCAN_TYPE_DATETIME: obj_type = ObDateTimeType; break;
    default: obj_type = ObVarcharType; break;
  }
  return obj_type;
}

namespace
{

const int64_t FILE_IO_BUF_SIZE = 1 << 16;

inline bool is_digit(const char c) { return c >= '0' && c <= '9'; }

inline bool text_equal(const ObString &text, const char *lower, const int64_t len)
{
  bool equal = (text.length() == len);
  for (int64_t i = 0; equal && i < len; ++i) {
    char c = text[i];
    if (c >= 'A' && c <= 'Z') {
      c = static_cast<char>(c - 'A' + 'a');
    }
    if (c != lower[i]) {
      equal = false;
    }
  }
  return equal;
}

// Buffered reader over a plain file.
class ObFileStream
{
public:
  ObFileStream() : fp_(NULL), pos_(0), len_(0), eof_(false), size_(0), mtime_(0) {}
  ~ObFileStream() { close(); }
  int open(const ObString &path, const int64_t offset = 0);
  void close();
  int rewind();
  // Returns OB_SUCCESS and sets ch to -1 on EOF.
  int next_char(int &ch);
  int peek_char(int &ch);
  int64_t tell() const { return file_pos_; }
  bool eof() const { return eof_ && pos_ >= len_; }
  int64_t size() const { return size_; }
  int64_t mtime() const { return mtime_; }
private:
  int fill();
  FILE *fp_;
  char buf_[FILE_IO_BUF_SIZE];
  int64_t pos_;
  int64_t len_;
  bool eof_;
  int64_t size_;
  int64_t mtime_;
  int64_t file_pos_;
};

int ObFileStream::open(const ObString &path, const int64_t offset)
{
  int ret = OB_SUCCESS;
  char name[PATH_MAX];
  pos_ = 0;
  len_ = 0;
  eof_ = false;
  file_pos_ = 0;
  size_ = 0;
  mtime_ = 0;
  if (path.length() <= 0 || path.length() >= PATH_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid file path", K(ret), K(path));
  } else {
    MEMCPY(name, path.ptr(), path.length());
    name[path.length()] = '\0';
    if (OB_ISNULL(fp_ = fopen(name, "rb"))) {
      ret = OB_FILE_NOT_EXIST;
      LOG_WARN("open file failed", K(ret), K(path), K(errno));
    } else {
      struct stat st;
      if (0 == stat(name, &st)) {
        size_ = static_cast<int64_t>(st.st_size);
        mtime_ = static_cast<int64_t>(st.st_mtime);
      }
      if (offset > 0 && 0 != fseek(fp_, offset, SEEK_SET)) {
        ret = OB_IO_ERROR;
        LOG_WARN("seek file failed", K(ret), K(offset));
      }
      file_pos_ = offset;
    }
  }
  return ret;
}

void ObFileStream::close()
{
  if (NULL != fp_) {
    fclose(fp_);
    fp_ = NULL;
  }
}

int ObFileStream::rewind()
{
  int ret = OB_SUCCESS;
  if (NULL == fp_) {
    ret = OB_NOT_INIT;
  } else if (0 != fseek(fp_, 0, SEEK_SET)) {
    ret = OB_IO_ERROR;
    LOG_WARN("rewind file failed", K(ret));
  } else {
    pos_ = 0;
    len_ = 0;
    eof_ = false;
    file_pos_ = 0;
  }
  return ret;
}

int ObFileStream::fill()
{
  int ret = OB_SUCCESS;
  if (NULL == fp_) {
    ret = OB_NOT_INIT;
  } else if (eof_) {
  } else {
    len_ = static_cast<int64_t>(fread(buf_, 1, FILE_IO_BUF_SIZE, fp_));
    pos_ = 0;
    if (len_ <= 0) {
      eof_ = true;
      len_ = 0;
      if (0 != ferror(fp_)) {
        ret = OB_IO_ERROR;
        LOG_WARN("read file failed", K(ret));
      }
    }
    file_pos_ += len_;
  }
  return ret;
}

int ObFileStream::next_char(int &ch)
{
  int ret = OB_SUCCESS;
  ch = -1;
  if (pos_ >= len_) {
    if (OB_FAIL(fill())) {
    }
  }
  if (OB_SUCC(ret) && pos_ < len_) {
    ch = static_cast<unsigned char>(buf_[pos_++]);
  }
  return ret;
}

int ObFileStream::peek_char(int &ch)
{
  int ret = OB_SUCCESS;
  ch = -1;
  if (pos_ >= len_) {
    if (OB_FAIL(fill())) {
    }
  }
  if (OB_SUCC(ret) && pos_ < len_) {
    ch = static_cast<unsigned char>(buf_[pos_]);
  }
  return ret;
}

// Growing byte buffer bound to a per-row allocator.
class ObRowBuf
{
public:
  ObRowBuf() : alloc_(NULL), data_(NULL), len_(0), cap_(0) {}
  void reset(ObIAllocator *alloc) { alloc_ = alloc; len_ = 0; }
  // data_ came from the allocator of the previous row, which has already been
  // rewound. Keep using it and the storage handed out by finish() in this row
  // may land on top of it, so drop it once per row and reallocate lazily.
  void invalidate() { data_ = NULL; len_ = 0; cap_ = 0; }
  int append(const char c);
  int append(const char *data, const int64_t len);
  int finish(ObString &str);
  int64_t length() const { return len_; }
private:
  int reserve(const int64_t need);
  ObIAllocator *alloc_;
  char *data_;
  int64_t len_;
  int64_t cap_;
};

int ObRowBuf::reserve(const int64_t need)
{
  int ret = OB_SUCCESS;
  if (need <= cap_) {
  } else if (OB_ISNULL(alloc_)) {
    ret = OB_NOT_INIT;
  } else {
    int64_t new_cap = cap_ == 0 ? 256 : cap_;
    while (new_cap < need) {
      new_cap *= 2;
    }
    char *new_data = static_cast<char *>(alloc_->alloc(new_cap));
    if (OB_ISNULL(new_data)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row buffer failed", K(ret), K(new_cap));
    } else {
      if (len_ > 0) {
        MEMCPY(new_data, data_, len_);
      }
      data_ = new_data;
      cap_ = new_cap;
    }
  }
  return ret;
}

int ObRowBuf::append(const char c)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reserve(len_ + 1))) {
  } else {
    data_[len_++] = c;
  }
  return ret;
}

int ObRowBuf::append(const char *data, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (len <= 0) {
  } else if (OB_FAIL(reserve(len_ + len))) {
  } else {
    MEMCPY(data_ + len_, data, len);
    len_ += len;
  }
  return ret;
}

int ObRowBuf::finish(ObString &str)
{
  int ret = OB_SUCCESS;
  if (len_ <= 0) {
    str.assign_ptr(NULL, 0);
  } else {
    char *ptr = static_cast<char *>(alloc_->alloc(len_));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate field buffer failed", K(ret), K_(len));
    } else {
      MEMCPY(ptr, data_, len_);
      str.assign_ptr(ptr, static_cast<int32_t>(len_));
    }
  }
  return ret;
}

struct ObCsvRecord
{
  ObCsvRecord() : fields_(), quoted_(), is_first_field_(true) {}
  ObArray<ObString> fields_;
  ObArray<uint8_t> quoted_;
  bool is_first_field_;
};

} // anonymous namespace

class ObCsvScanReader : public ObFileScanReader
{
public:
  ObCsvScanReader() : stream_(), options_(), columns_(), record_(), row_index_(0), line_no_(0) {}
  ~ObCsvScanReader() override { close(); }
  int open(ObIAllocator &alloc, const ObString &path, const ObFileScanOptions &options);
  int get_columns(ObIArray<ObFileColumnMeta> &columns) override;
  int next_row(ObIAllocator &alloc, ObIArray<ObFileCell> &cells, bool &eof) override;
  int64_t file_size() const override { return stream_.size(); }
  int64_t file_mtime() const override { return stream_.mtime(); }
  void close() override { stream_.close(); }
private:
  int read_record(ObIAllocator &alloc, ObCsvRecord &record, bool &got_record);
  int infer_schema(ObIAllocator &alloc);
  bool is_null_field(const ObString &field, bool quoted) const;
  ObFileStream stream_;
  ObFileScanOptions options_;
  ObSEArray<ObFileColumnMeta, 8> columns_;
  ObCsvRecord record_;
  ObRowBuf buf_;
  int64_t row_index_;
  int64_t line_no_;
};

int ObCsvScanReader::read_record(ObIAllocator &alloc, ObCsvRecord &record, bool &got_record)
{
  int ret = OB_SUCCESS;
  bool in_quotes = false;
  bool field_quoted = false;
  bool record_done = false;
  bool saw_any = false;
  int ch = -1;
  got_record = false;
  record.fields_.reuse();
  record.quoted_.reuse();
  buf_.reset(&alloc);
  buf_.invalidate();
  while (OB_SUCC(ret) && !record_done) {
    if (OB_FAIL(stream_.next_char(ch))) {
    } else if (-1 == ch) {
      record_done = true;
      if (!saw_any) {
        got_record = false;
      } else {
        ObString field;
        got_record = true;
        if (OB_FAIL(buf_.finish(field))) {
        } else if (OB_FAIL(record.fields_.push_back(field))) {
        } else if (OB_FAIL(record.quoted_.push_back(field_quoted ? 1 : 0))) {
        }
      }
      break;
    } else {
      saw_any = true;
      if (in_quotes) {
        if (ch == options_.quote_) {
          int next = -1;
          if (OB_FAIL(stream_.peek_char(next))) {
          } else if (next == options_.quote_) {
            // A doubled quote inside a quoted field stands for one literal quote.
            stream_.next_char(next);
            if (OB_FAIL(buf_.append(static_cast<char>(next)))) {
            }
          } else {
            in_quotes = false;
          }
        } else if (0 != options_.escape_ && options_.escape_ != options_.quote_
                   && ch == options_.escape_) {
          int next = -1;
          if (OB_FAIL(stream_.peek_char(next))) {
          } else if (-1 != next) {
            stream_.next_char(next);
            if (OB_FAIL(buf_.append(static_cast<char>(next)))) {
            }
          }
        } else if (OB_FAIL(buf_.append(static_cast<char>(ch)))) {
        }
      } else if (ch == options_.quote_) {
        if (0 == buf_.length() && !field_quoted) {
          in_quotes = true;
          field_quoted = true;
        } else if (OB_FAIL(buf_.append(static_cast<char>(ch)))) {
        }
      } else if (ch == options_.delimiter_) {
        ObString field;
        if (OB_FAIL(buf_.finish(field))) {
        } else if (OB_FAIL(record.fields_.push_back(field))) {
        } else if (OB_FAIL(record.quoted_.push_back(field_quoted ? 1 : 0))) {
        } else {
          buf_.reset(&alloc);
          field_quoted = false;
        }
      } else if (ch == '\n' || ch == '\r') {
        if (ch == '\r') {
          int next = -1;
          if (OB_FAIL(stream_.peek_char(next))) {
          } else if ('\n' == next) {
            stream_.next_char(next);
          }
        }
        record_done = true;
        got_record = true;
        ObString field;
        if (OB_FAIL(buf_.finish(field))) {
        } else if (OB_FAIL(record.fields_.push_back(field))) {
        } else if (OB_FAIL(record.quoted_.push_back(field_quoted ? 1 : 0))) {
        }
        ++line_no_;
      } else if (OB_FAIL(buf_.append(static_cast<char>(ch)))) {
      }
    }
  }
  if (OB_SUCC(ret) && got_record) {
    // Drop a trailing empty field caused by a line terminator right after
    // the last delimiter, e.g. "a,b,\n" keeps three fields but a blank line
    // has a single empty field, which is handled by the caller.
  }
  return ret;
}

bool ObCsvScanReader::is_null_field(const ObString &field, bool quoted) const
{
  bool is_null = false;
  if (!quoted) {
    if (0 == field.length()) {
      is_null = true;
    } else if (options_.null_string_.length() > 0
               && field.length() == options_.null_string_.length()
               && 0 == MEMCMP(field.ptr(), options_.null_string_.ptr(), field.length())) {
      is_null = true;
    }
  }
  return is_null;
}

int ObCsvScanReader::infer_schema(ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  bool got_record = false;
  int64_t data_rows = 0;
  if (options_.has_header_) {
    if (OB_FAIL(read_record(alloc, record_, got_record))) {
    } else if (!got_record) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("csv file has no header", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < record_.fields_.count(); ++i) {
        ObFileColumnMeta meta;
        meta.type_ = FILE_SCAN_TYPE_INVALID;
        meta.nullable_ = false;
        ObString unique_name;
        if (OB_FAIL(ObFileScanUtils::make_unique_column_name(alloc, record_.fields_.at(i), columns_, unique_name))) {
        } else {
          meta.name_ = unique_name;
          if (OB_FAIL(columns_.push_back(meta))) {
          }
        }
      }
    }
  }
  while (OB_SUCC(ret)) {
    got_record = false;
    if (OB_FAIL(read_record(alloc, record_, got_record))) {
    } else if (!got_record) {
      break;
    } else {
      bool all_empty = true;
      for (int64_t i = 0; all_empty && i < record_.fields_.count(); ++i) {
        if (record_.fields_.at(i).length() > 0) {
          all_empty = false;
        }
      }
      if (all_empty && 1 == record_.fields_.count()) {
        // Blank line, ignore it.
        continue;
      }
      ++data_rows;
      if (!options_.has_header_ && record_.fields_.count() > columns_.count()) {
        for (int64_t i = columns_.count(); i < record_.fields_.count(); ++i) {
          ObFileColumnMeta meta;
          char name_buf[32];
          int64_t pos = 0;
          if (OB_FAIL(databuff_printf(name_buf, sizeof(name_buf), pos, "column%ld", i))) {
          } else {
            ObString name;
            name.assign_ptr(name_buf, static_cast<int32_t>(pos));
            ObString unique_name;
            if (OB_FAIL(ObFileScanUtils::make_unique_column_name(alloc, name, columns_, unique_name))) {
            } else {
              meta.name_ = unique_name;
              meta.type_ = FILE_SCAN_TYPE_INVALID;
              meta.nullable_ = false;
              if (OB_FAIL(columns_.push_back(meta))) {
              }
            }
          }
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < record_.fields_.count() && i < columns_.count(); ++i) {
        const ObString &field = record_.fields_.at(i);
        ObFileColumnMeta &meta = columns_.at(i);
        const bool is_null = is_null_field(field, 0 != record_.quoted_.at(i));
        if (is_null) {
          meta.nullable_ = true;
        } else {
          meta.type_ = ObFileScanUtils::widen_type(
              meta.type_, ObFileScanUtils::probe_text_type(field, options_.tz_info_));
        }
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < columns_.count(); ++i) {
    if (FILE_SCAN_TYPE_INVALID == columns_.at(i).type_) {
      columns_.at(i).type_ = FILE_SCAN_TYPE_VARCHAR;
    }
  }
  row_index_ = 0;
  return ret;
}

int ObCsvScanReader::open(ObIAllocator &alloc, const ObString &path, const ObFileScanOptions &options)
{
  int ret = OB_SUCCESS;
  options_ = options;
  if (OB_FAIL(stream_.open(path))) {
    LOG_WARN("open csv file failed", K(ret), K(path));
  } else if (OB_FAIL(infer_schema(alloc))) {
    LOG_WARN("infer csv schema failed", K(ret), K(path));
  } else if (OB_FAIL(stream_.rewind())) {
  } else if (options_.has_header_) {
    bool got_record = false;
    if (OB_FAIL(read_record(alloc, record_, got_record))) {
    }
  }
  return ret;
}

int ObCsvScanReader::get_columns(ObIArray<ObFileColumnMeta> &columns)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(columns.assign(columns_))) {
    LOG_WARN("assign columns failed", K(ret));
  }
  return ret;
}

int ObCsvScanReader::next_row(ObIAllocator &alloc, ObIArray<ObFileCell> &cells, bool &eof)
{
  int ret = OB_SUCCESS;
  bool got_record = false;
  cells.reuse();
  eof = false;
  while (OB_SUCC(ret) && !got_record) {
    if (OB_FAIL(read_record(alloc, record_, got_record))) {
    } else if (!got_record) {
      eof = true;
      break;
    } else {
      bool all_empty = true;
      for (int64_t i = 0; all_empty && i < record_.fields_.count(); ++i) {
        if (record_.fields_.at(i).length() > 0) {
          all_empty = false;
        }
      }
      if (all_empty && 1 == record_.fields_.count()) {
        got_record = false;
        continue;
      }
    }
  }
  if (OB_SUCC(ret) && !eof) {
    for (int64_t i = 0; OB_SUCC(ret) && i < columns_.count(); ++i) {
      ObFileCell cell;
      const ObFileColumnMeta &meta = columns_.at(i);
      if (i >= record_.fields_.count()) {
        cell.is_null_ = true;
      } else {
        const ObString &field = record_.fields_.at(i);
        const bool is_null = is_null_field(field, 0 != record_.quoted_.at(i));
        if (is_null) {
          cell.is_null_ = true;
        } else {
          cell.is_null_ = false;
          switch (meta.type_) {
            case FILE_SCAN_TYPE_BIGINT: {
              int64_t value = 0;
              ObFileScanUtils::is_integer_text(field, value);
              cell.int_value_ = value;
              break;
            }
            case FILE_SCAN_TYPE_DOUBLE: {
              double value = 0;
              ObFileScanUtils::is_double_text(field, value);
              cell.double_value_ = value;
              break;
            }
            case FILE_SCAN_TYPE_BOOLEAN: {
              bool value = false;
              ObFileScanUtils::is_boolean_text(field, value);
              cell.int_value_ = value ? 1 : 0;
              break;
            }
            case FILE_SCAN_TYPE_DATE: {
              int32_t value = 0;
              ObFileScanUtils::is_date_text(field, value);
              cell.int_value_ = value;
              break;
            }
            case FILE_SCAN_TYPE_DATETIME: {
              int64_t value = 0;
              ObFileScanUtils::is_datetime_text(field, options_.tz_info_, value);
              cell.int_value_ = value;
              break;
            }
            default: {
              cell.string_value_ = field;
              break;
            }
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(cells.push_back(cell))) {
      }
    }
    ++row_index_;
  }
  return ret;
}

namespace
{

class ObJsonParser
{
public:
  ObJsonParser(ObIAllocator &alloc, const char *data, const int64_t len,
               const ObTimeZoneInfo *tz_info)
    : alloc_(alloc), data_(data), len_(len), pos_(0), tz_info_(tz_info) {}
  // Parses one flat object. Field names and scalar values are returned in
  // order. Nested objects or arrays are rejected.
  int parse_object(ObIArray<ObString> &names,
                   ObIArray<ObFileCell> &values,
                   ObIArray<ObFileScanType> &types);
  int skip_spaces();
  bool at_end() const { return pos_ >= len_; }
  void reset() { pos_ = 0; }
private:
  int parse_string(ObString &str);
  int parse_value(ObFileCell &cell, ObFileScanType &type);
  int parse_number(ObFileCell &cell, ObFileScanType &type);
  int alloc_string(const char *data, const int64_t len, ObString &str);
  ObIAllocator &alloc_;
  const char *data_;
  int64_t len_;
  int64_t pos_;
  const ObTimeZoneInfo *tz_info_;
};

int ObJsonParser::alloc_string(const char *data, const int64_t len, ObString &str)
{
  int ret = OB_SUCCESS;
  if (len <= 0) {
    str.assign_ptr(NULL, 0);
  } else {
    char *ptr = static_cast<char *>(alloc_.alloc(len));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate json string failed", K(ret), K(len));
    } else {
      MEMCPY(ptr, data, len);
      str.assign_ptr(ptr, static_cast<int32_t>(len));
    }
  }
  return ret;
}

int ObJsonParser::skip_spaces()
{
  int ret = OB_SUCCESS;
  while (pos_ < len_ && (' ' == data_[pos_] || '\t' == data_[pos_]
                         || '\r' == data_[pos_] || '\n' == data_[pos_])) {
    ++pos_;
  }
  return ret;
}

int ObJsonParser::parse_string(ObString &str)
{
  int ret = OB_SUCCESS;
  if (pos_ >= len_ || '"' != data_[pos_]) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expect json string", K(ret), K_(pos));
  } else {
    ++pos_;
    ObRowBuf buf;
    buf.reset(&alloc_);
    bool closed = false;
    while (OB_SUCC(ret) && pos_ < len_ && !closed) {
      char c = data_[pos_++];
      if ('"' == c) {
        closed = true;
      } else if ('\\' == c && pos_ < len_) {
        char esc = data_[pos_++];
        char out = esc;
        if ('n' == esc) {
          out = '\n';
        } else if ('t' == esc) {
          out = '\t';
        } else if ('r' == esc) {
          out = '\r';
        } else if ('b' == esc) {
          out = '\b';
        } else if ('f' == esc) {
          out = '\f';
        } else if ('u' == esc) {
          if (pos_ + 4 > len_) {
            ret = OB_INVALID_ARGUMENT;
          } else {
            uint32_t code = 0;
            for (int64_t i = 0; OB_SUCC(ret) && i < 4; ++i) {
              char h = data_[pos_++];
              code <<= 4;
              if (h >= '0' && h <= '9') {
                code |= static_cast<uint32_t>(h - '0');
              } else if (h >= 'a' && h <= 'f') {
                code |= static_cast<uint32_t>(h - 'a' + 10);
              } else if (h >= 'A' && h <= 'F') {
                code |= static_cast<uint32_t>(h - 'A' + 10);
              } else {
                ret = OB_INVALID_ARGUMENT;
              }
            }
            if (OB_SUCC(ret)) {
              char utf8[4];
              int64_t utf8_len = 0;
              if (code < 0x80) {
                utf8[utf8_len++] = static_cast<char>(code);
              } else if (code < 0x800) {
                utf8[utf8_len++] = static_cast<char>(0xC0 | (code >> 6));
                utf8[utf8_len++] = static_cast<char>(0x80 | (code & 0x3F));
              } else {
                utf8[utf8_len++] = static_cast<char>(0xE0 | (code >> 12));
                utf8[utf8_len++] = static_cast<char>(0x80 | ((code >> 6) & 0x3F));
                utf8[utf8_len++] = static_cast<char>(0x80 | (code & 0x3F));
              }
              if (OB_FAIL(buf.append(utf8, utf8_len))) {
              }
            }
            continue;
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(buf.append(out))) {
        }
      } else if (OB_FAIL(buf.append(c))) {
      }
    }
    if (OB_SUCC(ret) && !closed) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unterminated json string", K(ret), K_(pos));
    }
    if (OB_SUCC(ret) && OB_FAIL(buf.finish(str))) {
    }
  }
  return ret;
}

int ObJsonParser::parse_number(ObFileCell &cell, ObFileScanType &type)
{
  int ret = OB_SUCCESS;
  int64_t start = pos_;
  bool is_double = false;
  if (pos_ < len_ && ('-' == data_[pos_] || '+' == data_[pos_])) {
    ++pos_;
  }
  while (pos_ < len_ && is_digit(data_[pos_])) {
    ++pos_;
  }
  if (pos_ < len_ && '.' == data_[pos_]) {
    is_double = true;
    ++pos_;
    while (pos_ < len_ && is_digit(data_[pos_])) {
      ++pos_;
    }
  }
  if (pos_ < len_ && ('e' == data_[pos_] || 'E' == data_[pos_])) {
    is_double = true;
    ++pos_;
    if (pos_ < len_ && ('-' == data_[pos_] || '+' == data_[pos_])) {
      ++pos_;
    }
    while (pos_ < len_ && is_digit(data_[pos_])) {
      ++pos_;
    }
  }
  if (pos_ == start) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid json number", K(ret));
  } else {
    ObString text;
    text.assign_ptr(data_ + start, static_cast<int32_t>(pos_ - start));
    cell.is_null_ = false;
    if (!is_double) {
      int64_t int_value = 0;
      if (ObFileScanUtils::is_integer_text(text, int_value)) {
        cell.int_value_ = int_value;
        type = FILE_SCAN_TYPE_BIGINT;
      } else {
        is_double = true;
      }
    }
    if (is_double) {
      double double_value = 0;
      ObFileScanUtils::is_double_text(text, double_value);
      cell.double_value_ = double_value;
      type = FILE_SCAN_TYPE_DOUBLE;
    }
  }
  return ret;
}

int ObJsonParser::parse_value(ObFileCell &cell, ObFileScanType &type)
{
  int ret = OB_SUCCESS;
  type = FILE_SCAN_TYPE_INVALID;
  if (OB_FAIL(skip_spaces())) {
  } else if (pos_ >= len_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected end of json", K(ret));
  } else if ('{' == data_[pos_] || '[' == data_[pos_]) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "nested json value in a file table");
  } else if ('"' == data_[pos_]) {
    ObString text;
    if (OB_FAIL(parse_string(text))) {
    } else {
      cell.is_null_ = false;
      cell.string_value_ = text;
      type = ObFileScanUtils::probe_string_type(text, tz_info_);
    }
  } else if ('t' == data_[pos_] || 'f' == data_[pos_]) {
    if (pos_ + 4 <= len_ && 0 == strncmp(data_ + pos_, "true", 4)) {
      pos_ += 4;
      cell.is_null_ = false;
      cell.int_value_ = 1;
      type = FILE_SCAN_TYPE_BOOLEAN;
    } else if (pos_ + 5 <= len_ && 0 == strncmp(data_ + pos_, "false", 5)) {
      pos_ += 5;
      cell.is_null_ = false;
      cell.int_value_ = 0;
      type = FILE_SCAN_TYPE_BOOLEAN;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid json literal", K(ret));
    }
  } else if ('n' == data_[pos_] && pos_ + 4 <= len_ && 0 == strncmp(data_ + pos_, "null", 4)) {
    pos_ += 4;
    cell.is_null_ = true;
    type = FILE_SCAN_TYPE_INVALID;
  } else {
    if (OB_FAIL(parse_number(cell, type))) {
    }
  }
  return ret;
}

int ObJsonParser::parse_object(ObIArray<ObString> &names,
                               ObIArray<ObFileCell> &values,
                               ObIArray<ObFileScanType> &types)
{
  int ret = OB_SUCCESS;
  names.reuse();
  values.reuse();
  types.reuse();
  if (OB_FAIL(skip_spaces())) {
  } else if (pos_ >= len_ || '{' != data_[pos_]) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("json line does not start with an object", K(ret));
  } else {
    ++pos_;
    bool done = false;
    while (OB_SUCC(ret) && !done) {
      ObString name;
      ObFileCell cell;
      ObFileScanType type = FILE_SCAN_TYPE_INVALID;
      if (OB_FAIL(skip_spaces())) {
      } else if (pos_ < len_ && '}' == data_[pos_]) {
        ++pos_;
        done = true;
      } else if (OB_FAIL(parse_string(name))) {
      } else if (OB_FAIL(skip_spaces())) {
      } else if (pos_ >= len_ || ':' != data_[pos_]) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("expect colon in json object", K(ret));
      } else {
        ++pos_;
        if (OB_FAIL(parse_value(cell, type))) {
        } else if (OB_FAIL(names.push_back(name))) {
        } else if (OB_FAIL(values.push_back(cell))) {
        } else if (OB_FAIL(types.push_back(type))) {
        } else if (OB_FAIL(skip_spaces())) {
        } else if (pos_ < len_ && ',' == data_[pos_]) {
          ++pos_;
        } else if (pos_ < len_ && '}' == data_[pos_]) {
          ++pos_;
          done = true;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("expect comma or brace end in json object", K(ret));
        }
      }
    }
  }
  return ret;
}

} // anonymous namespace

class ObJsonScanReader : public ObFileScanReader
{
public:
  ObJsonScanReader() : stream_(), options_(), columns_(), line_buf_() {}
  ~ObJsonScanReader() override { close(); }
  int open(ObIAllocator &alloc, const ObString &path, const ObFileScanOptions &options);
  int get_columns(ObIArray<ObFileColumnMeta> &columns) override;
  int next_row(ObIAllocator &alloc, ObIArray<ObFileCell> &cells, bool &eof) override;
  int64_t file_size() const override { return stream_.size(); }
  int64_t file_mtime() const override { return stream_.mtime(); }
  void close() override { stream_.close(); }
private:
  int read_line(ObIAllocator &alloc, ObString &line, bool &eof);
  int infer_schema(ObIAllocator &alloc);
  int find_column(const ObString &name) const;
  ObFileStream stream_;
  ObFileScanOptions options_;
  ObSEArray<ObFileColumnMeta, 8> columns_;
  ObRowBuf line_buf_;
};

int ObJsonScanReader::read_line(ObIAllocator &alloc, ObString &line, bool &eof)
{
  int ret = OB_SUCCESS;
  int ch = -1;
  bool got = false;
  eof = false;
  line_buf_.reset(&alloc);
  line_buf_.invalidate();
  while (OB_SUCC(ret) && !got && !eof) {
    if (OB_FAIL(stream_.next_char(ch))) {
    } else if (-1 == ch) {
      if (line_buf_.length() > 0) {
        got = true;
      } else {
        eof = true;
      }
    } else if ('\n' == ch) {
      got = true;
    } else if ('\r' == ch) {
      int next = -1;
      if (OB_FAIL(stream_.peek_char(next))) {
      } else if ('\n' == next) {
        stream_.next_char(next);
      }
      got = true;
    } else if (OB_FAIL(line_buf_.append(static_cast<char>(ch)))) {
    }
  }
  if (OB_SUCC(ret) && !eof) {
    if (OB_FAIL(line_buf_.finish(line))) {
    }
  }
  return ret;
}

int ObJsonScanReader::find_column(const ObString &name) const
{
  int index = -1;
  for (int64_t i = 0; -1 == index && i < columns_.count(); ++i) {
    const ObString &col_name = columns_.at(i).name_;
    if (col_name.length() == name.length()
        && 0 == MEMCMP(col_name.ptr(), name.ptr(), name.length())) {
      index = static_cast<int>(i);
    }
  }
  return index;
}

int ObJsonScanReader::infer_schema(ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  bool eof = false;
  while (OB_SUCC(ret) && !eof) {
    ObString line;
    if (OB_FAIL(read_line(alloc, line, eof))) {
    } else if (eof) {
      break;
    } else {
      int64_t pos = 0;
      while (pos < line.length() && (' ' == line[pos] || '\t' == line[pos])) {
        ++pos;
      }
      if (pos >= line.length()) {
        continue;
      }
      ObJsonParser parser(alloc, line.ptr() + pos, line.length() - pos, options_.tz_info_);
      ObArray<ObString> names;
      ObArray<ObFileCell> values;
      ObArray<ObFileScanType> types;
      if (OB_FAIL(parser.parse_object(names, values, types))) {
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < names.count(); ++i) {
          int index = find_column(names.at(i));
          if (-1 == index) {
            ObFileColumnMeta meta;
            ObString unique_name;
            if (OB_FAIL(ObFileScanUtils::make_unique_column_name(alloc, names.at(i), columns_, unique_name))) {
            } else {
              meta.name_ = unique_name;
              meta.type_ = types.at(i);
              meta.nullable_ = values.at(i).is_null_;
              if (OB_FAIL(columns_.push_back(meta))) {
              }
            }
          } else {
            ObFileColumnMeta &meta = columns_.at(index);
            if (values.at(i).is_null_) {
              meta.nullable_ = true;
            } else {
              meta.type_ = ObFileScanUtils::widen_type(meta.type_, types.at(i));
            }
          }
        }
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < columns_.count(); ++i) {
    if (FILE_SCAN_TYPE_INVALID == columns_.at(i).type_) {
      columns_.at(i).type_ = FILE_SCAN_TYPE_VARCHAR;
    }
  }
  return ret;
}

int ObJsonScanReader::open(ObIAllocator &alloc, const ObString &path, const ObFileScanOptions &options)
{
  int ret = OB_SUCCESS;
  options_ = options;
  if (OB_FAIL(stream_.open(path))) {
    LOG_WARN("open jsonl file failed", K(ret), K(path));
  } else if (OB_FAIL(infer_schema(alloc))) {
    LOG_WARN("infer jsonl schema failed", K(ret), K(path));
  } else if (OB_FAIL(stream_.rewind())) {
  }
  return ret;
}

int ObJsonScanReader::get_columns(ObIArray<ObFileColumnMeta> &columns)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(columns.assign(columns_))) {
    LOG_WARN("assign columns failed", K(ret));
  }
  return ret;
}

int ObJsonScanReader::next_row(ObIAllocator &alloc, ObIArray<ObFileCell> &cells, bool &eof)
{
  int ret = OB_SUCCESS;
  bool got = false;
  eof = false;
  while (OB_SUCC(ret) && !got && !eof) {
    ObString line;
    if (OB_FAIL(read_line(alloc, line, eof))) {
    } else if (eof) {
      break;
    } else {
      int64_t pos = 0;
      while (pos < line.length() && (' ' == line[pos] || '\t' == line[pos])) {
        ++pos;
      }
      if (pos >= line.length()) {
        continue;
      }
      ObJsonParser parser(alloc, line.ptr() + pos, line.length() - pos, options_.tz_info_);
      ObArray<ObString> names;
      ObArray<ObFileCell> values;
      ObArray<ObFileScanType> types;
      if (OB_FAIL(parser.parse_object(names, values, types))) {
      } else {
        cells.reuse();
        for (int64_t i = 0; OB_SUCC(ret) && i < columns_.count(); ++i) {
          ObFileCell cell;
          cell.is_null_ = true;
          if (OB_FAIL(cells.push_back(cell))) {
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < names.count(); ++i) {
          int index = find_column(names.at(i));
          if (index >= 0 && index < cells.count()) {
            ObFileCell &dst = cells.at(index);
            const ObFileColumnMeta &meta = columns_.at(index);
            dst = values.at(i);
            if (!dst.is_null_) {
              // A value keeps the type it was parsed with, the column type is the
              // union over all lines, so widen or narrow it to the column type.
              switch (meta.type_) {
                case FILE_SCAN_TYPE_BIGINT: {
                  if (FILE_SCAN_TYPE_BIGINT != types.at(i)) {
                    dst.int_value_ = static_cast<int64_t>(dst.double_value_);
                  }
                  break;
                }
                case FILE_SCAN_TYPE_DOUBLE: {
                  if (FILE_SCAN_TYPE_BIGINT == types.at(i)) {
                    dst.double_value_ = static_cast<double>(dst.int_value_);
                  }
                  break;
                }
                case FILE_SCAN_TYPE_DATE: {
                  if (dst.string_value_.length() > 0) {
                    int32_t date_value = 0;
                    ObFileScanUtils::is_date_text(dst.string_value_, date_value);
                    dst.int_value_ = date_value;
                  }
                  break;
                }
                case FILE_SCAN_TYPE_DATETIME: {
                  if (dst.string_value_.length() > 0) {
                    int64_t datetime_value = 0;
                    ObFileScanUtils::is_datetime_text(dst.string_value_, options_.tz_info_,
                                                      datetime_value);
                    dst.int_value_ = datetime_value;
                  }
                  break;
                }
                default:
                  break;
              }
            }
          }
        }
        got = true;
      }
    }
  }
  return ret;
}

class ObGlobScanReader : public ObFileScanReader
{
public:
  ObGlobScanReader() : files_(), columns_(), index_(0) {}
  int open(ObIAllocator &alloc, const ObString &pattern);
  int get_columns(ObIArray<ObFileColumnMeta> &columns) override;
  int next_row(ObIAllocator &alloc, ObIArray<ObFileCell> &cells, bool &eof) override;
  int64_t file_size() const override { return 0; }
  int64_t file_mtime() const override { return 0; }
private:
  ObSEArray<ObString, 8> files_;
  ObSEArray<ObFileColumnMeta, 1> columns_;
  int64_t index_;
};

int ObGlobScanReader::open(ObIAllocator &alloc, const ObString &pattern)
{
  int ret = OB_SUCCESS;
  ObFileColumnMeta meta;
  ObString name;
  name.assign_ptr("file", 4);
  meta.name_ = name;
  meta.type_ = FILE_SCAN_TYPE_VARCHAR;
  meta.nullable_ = false;
  if (OB_FAIL(columns_.push_back(meta))) {
  } else if (OB_FAIL(ObFileScanUtils::expand_glob(alloc, pattern, files_))) {
  }
  return ret;
}

int ObGlobScanReader::get_columns(ObIArray<ObFileColumnMeta> &columns)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(columns.assign(columns_))) {
  }
  return ret;
}

int ObGlobScanReader::next_row(ObIAllocator &alloc, ObIArray<ObFileCell> &cells, bool &eof)
{
  int ret = OB_SUCCESS;
  (void) alloc;
  cells.reuse();
  eof = (index_ >= files_.count());
  if (!eof) {
    ObFileCell cell;
    cell.is_null_ = false;
    cell.string_value_ = files_.at(index_);
    if (OB_FAIL(cells.push_back(cell))) {
    } else {
      ++index_;
    }
  }
  return ret;
}

int ObFileScanReader::create(ObIAllocator &alloc,
                             const ObString &path,
                             const ObFileScanFormat format,
                             const ObFileScanOptions &options,
                             ObFileScanReader *&reader)
{
  int ret = OB_SUCCESS;
  reader = NULL;
  if (FILE_SCAN_PARQUET == format) {
    if (OB_FAIL(ob_create_parquet_reader(alloc, path, options, reader))) {
      LOG_WARN("create parquet reader failed", K(ret), K(path));
    }
  } else {
    void *mem = NULL;
    if (OB_ISNULL(mem = alloc.alloc(FILE_SCAN_GLOB == format
                                        ? sizeof(ObGlobScanReader)
                                        : (FILE_SCAN_JSON == format ? sizeof(ObJsonScanReader)
                                                                    : sizeof(ObCsvScanReader))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate file reader failed", K(ret));
    } else if (FILE_SCAN_GLOB == format) {
      ObGlobScanReader *glob_reader = new (mem) ObGlobScanReader();
      if (OB_FAIL(glob_reader->open(alloc, path))) {
      } else {
        reader = glob_reader;
      }
    } else if (FILE_SCAN_JSON == format) {
      ObJsonScanReader *json_reader = new (mem) ObJsonScanReader();
      if (OB_FAIL(json_reader->open(alloc, path, options))) {
      } else {
        reader = json_reader;
      }
    } else {
      ObCsvScanReader *csv_reader = new (mem) ObCsvScanReader();
      if (OB_FAIL(csv_reader->open(alloc, path, options))) {
      } else {
        reader = csv_reader;
      }
    }
    if (OB_FAIL(ret) && NULL != reader) {
      reader->close();
      reader = NULL;
    }
  }
  return ret;
}

bool ObFileScanUtils::is_integer_text(const ObString &text, int64_t &value)
{
  bool ok = text.length() > 0 && text.length() <= 20;
  int64_t pos = 0;
  bool negative = false;
  if (ok && ('-' == text[0] || '+' == text[0])) {
    negative = ('-' == text[0]);
    ++pos;
  }
  if (pos >= text.length()) {
    ok = false;
  }
  for (int64_t i = pos; ok && i < text.length(); ++i) {
    if (!is_digit(text[i])) {
      ok = false;
    }
  }
  if (ok) {
    int64_t result = 0;
    for (int64_t i = pos; ok && i < text.length(); ++i) {
      int digit = text[i] - '0';
      if (result > (INT64_MAX - digit) / 10) {
        ok = false;
      } else {
        result = result * 10 + digit;
      }
    }
    if (ok) {
      value = negative ? -result : result;
    }
  }
  return ok;
}

bool ObFileScanUtils::is_double_text(const ObString &text, double &value)
{
  bool ok = false;
  if (text.length() > 0 && text.length() < 64) {
    char buf[64];
    MEMCPY(buf, text.ptr(), text.length());
    buf[text.length()] = '\0';
    char *end = NULL;
    errno = 0;
    double result = strtod(buf, &end);
    if (0 == errno && NULL != end && end == buf + text.length() && isfinite(result)) {
      bool has_digit = false;
      for (int64_t i = 0; !has_digit && i < text.length(); ++i) {
        has_digit = is_digit(text[i]);
      }
      if (has_digit) {
        value = result;
        ok = true;
      }
    }
  }
  return ok;
}

bool ObFileScanUtils::is_boolean_text(const ObString &text, bool &value)
{
  bool ok = true;
  if (text_equal(text, "true", 4)) {
    value = true;
  } else if (text_equal(text, "false", 5)) {
    value = false;
  } else {
    ok = false;
  }
  return ok;
}

bool ObFileScanUtils::is_date_text(const ObString &text, int32_t &value)
{
  bool ok = (10 == text.length());
  for (int64_t i = 0; ok && i < 10; ++i) {
    if (4 == i || 7 == i) {
      ok = ('-' == text[i]);
    } else {
      ok = is_digit(text[i]);
    }
  }
  int32_t date_value = 0;
  if (ok) {
    ok = (OB_SUCCESS == ObTimeConverter::str_to_date(text, date_value));
    if (ok) {
      value = date_value;
    }
  }
  return ok;
}

bool ObFileScanUtils::is_datetime_text(const ObString &text,
                                       const ObTimeZoneInfo *tz_info,
                                       int64_t &value)
{
  bool ok = (text.length() >= 19 && text.length() <= 32);
  for (int64_t i = 0; ok && i < 10; ++i) {
    if (4 == i || 7 == i) {
      ok = ('-' == text[i]);
    } else {
      ok = is_digit(text[i]);
    }
  }
  if (ok) {
    ok = (' ' == text[10] || 'T' == text[10] || 't' == text[10]);
  }
  for (int64_t i = 11; ok && i < 19; ++i) {
    if (13 == i || 16 == i) {
      ok = (':' == text[i]);
    } else {
      ok = is_digit(text[i]);
    }
  }
  if (ok) {
    for (int64_t i = 19; ok && i < text.length(); ++i) {
      ok = ('.' == text[i] || is_digit(text[i]));
    }
  }
  if (ok) {
    ObTimeConvertCtx cvrt_ctx(tz_info, false);
    int64_t datetime_value = 0;
    ok = (OB_SUCCESS == ObTimeConverter::str_to_datetime(text, cvrt_ctx, datetime_value, 0));
    if (ok) {
      value = datetime_value;
    }
  }
  return ok;
}

ObFileScanType ObFileScanUtils::widen_type(const ObFileScanType current, const ObFileScanType incoming)
{
  ObFileScanType result = current;
  if (FILE_SCAN_TYPE_INVALID == current || FILE_SCAN_TYPE_INVALID == incoming) {
    result = (FILE_SCAN_TYPE_INVALID == current) ? incoming : current;
  } else if (current == incoming) {
    result = current;
  } else if ((FILE_SCAN_TYPE_BIGINT == current && FILE_SCAN_TYPE_DOUBLE == incoming)
             || (FILE_SCAN_TYPE_DOUBLE == current && FILE_SCAN_TYPE_BIGINT == incoming)) {
    result = FILE_SCAN_TYPE_DOUBLE;
  } else if ((FILE_SCAN_TYPE_DATE == current && FILE_SCAN_TYPE_DATETIME == incoming)
             || (FILE_SCAN_TYPE_DATETIME == current && FILE_SCAN_TYPE_DATE == incoming)) {
    result = FILE_SCAN_TYPE_DATETIME;
  } else {
    result = FILE_SCAN_TYPE_VARCHAR;
  }
  return result;
}

ObFileScanType ObFileScanUtils::probe_text_type(const ObString &text,
                                               const ObTimeZoneInfo *tz_info)
{
  ObFileScanType type = FILE_SCAN_TYPE_VARCHAR;
  int64_t int_value = 0;
  double double_value = 0;
  bool bool_value = false;
  int32_t date_value = 0;
  int64_t datetime_value = 0;
  if (is_integer_text(text, int_value)) {
    type = FILE_SCAN_TYPE_BIGINT;
  } else if (is_double_text(text, double_value)) {
    type = FILE_SCAN_TYPE_DOUBLE;
  } else if (is_boolean_text(text, bool_value)) {
    type = FILE_SCAN_TYPE_BOOLEAN;
  } else if (is_date_text(text, date_value)) {
    type = FILE_SCAN_TYPE_DATE;
  } else if (is_datetime_text(text, tz_info, datetime_value)) {
    type = FILE_SCAN_TYPE_DATETIME;
  }
  return type;
}

ObFileScanType ObFileScanUtils::probe_string_type(const ObString &text,
                                                 const ObTimeZoneInfo *tz_info)
{
  ObFileScanType type = FILE_SCAN_TYPE_VARCHAR;
  int32_t date_value = 0;
  int64_t datetime_value = 0;
  if (is_date_text(text, date_value)) {
    type = FILE_SCAN_TYPE_DATE;
  } else if (is_datetime_text(text, tz_info, datetime_value)) {
    type = FILE_SCAN_TYPE_DATETIME;
  }
  return type;
}

int ObFileScanUtils::make_unique_column_name(ObIAllocator &alloc,
                                             const ObString &name,
                                             ObIArray<ObFileColumnMeta> &columns,
                                             ObString &unique_name)
{
  int ret = OB_SUCCESS;
  bool conflict = false;
  for (int64_t i = 0; !conflict && i < columns.count(); ++i) {
    const ObString &existing = columns.at(i).name_;
    if (existing.length() == name.length()
        && 0 == MEMCMP(existing.ptr(), name.ptr(), name.length())) {
      conflict = true;
    }
  }
  if (!conflict) {
    char *ptr = NULL;
    if (0 == name.length()) {
      unique_name.reset();
    } else if (OB_ISNULL(ptr = static_cast<char *>(alloc.alloc(name.length())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate column name failed", K(ret));
    } else {
      MEMCPY(ptr, name.ptr(), name.length());
      unique_name.assign_ptr(ptr, name.length());
    }
  } else {
    int64_t suffix = 2;
    bool done = false;
    while (OB_SUCC(ret) && !done) {
      char suffix_buf[32];
      int64_t suffix_len = 0;
      int64_t pos = 0;
      if (OB_FAIL(databuff_printf(suffix_buf, sizeof(suffix_buf), pos, "__%ld", suffix))) {
      } else {
        suffix_len = pos;
        int64_t total = name.length() + suffix_len;
        char *ptr = static_cast<char *>(alloc.alloc(total));
        if (OB_ISNULL(ptr)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate column name failed", K(ret));
        } else {
          MEMCPY(ptr, name.ptr(), name.length());
          MEMCPY(ptr + name.length(), suffix_buf, suffix_len);
          ObString candidate;
          candidate.assign_ptr(ptr, static_cast<int32_t>(total));
          bool dup = false;
          for (int64_t i = 0; !dup && i < columns.count(); ++i) {
            const ObString &existing = columns.at(i).name_;
            if (existing.length() == candidate.length()
                && 0 == MEMCMP(existing.ptr(), candidate.ptr(), candidate.length())) {
              dup = true;
            }
          }
          if (!dup) {
            unique_name = candidate;
            done = true;
          } else {
            ++suffix;
          }
        }
      }
    }
  }
  return ret;
}

int ObFileScanUtils::check_file_path(const ObString &path,
                                     const ObString &secure_root,
                                     bool &allowed)
{
  int ret = OB_SUCCESS;
  allowed = false;
  char path_buf[PATH_MAX];
  char real_path[PATH_MAX];
  if (path.length() <= 0 || path.length() >= PATH_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid file path", K(ret), K(path));
  } else {
    MEMCPY(path_buf, path.ptr(), path.length());
    path_buf[path.length()] = '\0';
    if (OB_ISNULL(realpath(path_buf, real_path))) {
      ret = OB_FILE_NOT_EXIST;
      LOG_WARN("resolve real path failed", K(ret), K(path), K(errno));
    } else if (0 == secure_root.length()) {
      allowed = true;
    } else if (secure_root.length() >= PATH_MAX) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("secure root path too long", K(ret));
    } else {
      char root_buf[PATH_MAX];
      char root_real[PATH_MAX];
      MEMCPY(root_buf, secure_root.ptr(), secure_root.length());
      root_buf[secure_root.length()] = '\0';
      if (OB_ISNULL(realpath(root_buf, root_real))) {
        ret = OB_FILE_NOT_EXIST;
        LOG_WARN("resolve secure root failed", K(ret), K(secure_root));
      } else {
        int64_t root_len = static_cast<int64_t>(strlen(root_real));
        while (root_len > 1 && '/' == root_real[root_len - 1]) {
          --root_len;
        }
        if (1 == root_len && '/' == root_real[0]) {
          // secure_file_priv is the filesystem root, every absolute path is allowed.
          allowed = ('/' == real_path[0]);
        } else {
          allowed = (0 == strncmp(real_path, root_real, root_len)
                     && ('\0' == real_path[root_len] || '/' == real_path[root_len]));
        }
      }
    }
  }
  return ret;
}

int ObFileScanUtils::expand_glob(ObIAllocator &alloc,
                                 const ObString &pattern,
                                 ObIArray<ObString> &files)
{
  int ret = OB_SUCCESS;
  char *pattern_buf = NULL;
  if (OB_ISNULL(pattern_buf = static_cast<char *>(alloc.alloc(pattern.length() + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate pattern buffer failed", K(ret));
  } else {
    MEMCPY(pattern_buf, pattern.ptr(), pattern.length());
    pattern_buf[pattern.length()] = '\0';
    glob_t glob_result;
    MEMSET(&glob_result, 0, sizeof(glob_result));
    int glob_ret = glob(pattern_buf, GLOB_ERR, NULL, &glob_result);
    if (0 == glob_ret || GLOB_NOMATCH == glob_ret) {
      for (size_t i = 0; OB_SUCC(ret) && i < glob_result.gl_pathc; ++i) {
        const char *matched = glob_result.gl_pathv[i];
        int64_t len = static_cast<int64_t>(strlen(matched));
        char *copy = static_cast<char *>(alloc.alloc(len));
        if (OB_ISNULL(copy)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate matched path failed", K(ret));
        } else {
          MEMCPY(copy, matched, len);
          ObString file;
          file.assign_ptr(copy, static_cast<int32_t>(len));
          OZ (files.push_back(file));
        }
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("expand glob failed", K(ret), K(pattern), K(glob_ret));
    }
    globfree(&glob_result);
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
