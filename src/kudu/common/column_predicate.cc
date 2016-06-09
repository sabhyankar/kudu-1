// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/common/column_predicate.h"

#include <utility>

#include "kudu/common/key_util.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/util/memory/arena.h"

using std::move;
using std::vector;

namespace kudu {

ColumnPredicate::ColumnPredicate(PredicateType predicate_type,
                                 ColumnSchema column,
                                 const void* lower,
                                 const void* upper)
    : predicate_type_(predicate_type),
      column_(move(column)),
      lower_(lower),
      upper_(upper) {
}

ColumnPredicate::ColumnPredicate(PredicateType predicate_type,
                                 ColumnSchema column,
                                 vector<void*>* values)
  : predicate_type_(predicate_type),
    column_(move(column)),
    lower_(nullptr),
    upper_(nullptr) {
  values_.swap(*values);
}

ColumnPredicate ColumnPredicate::Equality(ColumnSchema column, const void* value) {
  CHECK(value != nullptr);
  return ColumnPredicate(PredicateType::Equality, move(column), value, nullptr);
}

ColumnPredicate ColumnPredicate::Range(ColumnSchema column,
                                       const void* lower,
                                       const void* upper) {
  CHECK(lower != nullptr || upper != nullptr);
  ColumnPredicate pred(PredicateType::Range, move(column), lower, upper);
  pred.Simplify();
  return pred;
}

ColumnPredicate ColumnPredicate::InList(ColumnSchema column,
                                        vector<void*>* values) {
  CHECK(values != nullptr);
  ColumnPredicate pred(PredicateType::InList, move(column), values);
  pred.Simplify();
  return pred;
}

boost::optional<ColumnPredicate> ColumnPredicate::InclusiveRange(ColumnSchema column,
                                                                 const void* lower,
                                                                 const void* upper,
                                                                 Arena* arena) {
  CHECK(lower != nullptr || upper != nullptr);

  if (upper != nullptr) {
    // Transform the upper bound to exclusive by incrementing it.
    // Make a copy of the value before incrementing in case it's aliased.
    size_t size = column.type_info()->size();
    void*  buf = CHECK_NOTNULL(arena->AllocateBytes(size));
    memcpy(buf, upper, size);
    if (!key_util::IncrementCell(column, buf, arena)) {
      if (lower == nullptr) {
        if (column.is_nullable()) {
          // If incrementing the upper bound fails and the column is nullable,
          // then return an IS NOT NULL predicate, so that null values will be
          // filtered.
          return ColumnPredicate::IsNotNull(move(column));
        } else {
          return boost::none;
        }
      } else {
        upper = nullptr;
      }
    } else {
      upper = buf;
    }
  }
  return ColumnPredicate::Range(move(column), lower, upper);
}

ColumnPredicate ColumnPredicate::IsNotNull(ColumnSchema column) {
  CHECK(column.is_nullable());
  return ColumnPredicate(PredicateType::IsNotNull, move(column), nullptr, nullptr);
}

ColumnPredicate ColumnPredicate::None(ColumnSchema column) {
  return ColumnPredicate(PredicateType::None, move(column), nullptr, nullptr);
}

void ColumnPredicate::SetToNone() {
  predicate_type_ = PredicateType::None;
  lower_ = nullptr;
  upper_ = nullptr;
}

void ColumnPredicate::Simplify() {
  switch (predicate_type_) {
    case PredicateType::None:
    case PredicateType::Equality:
    case PredicateType::IsNotNull: return;
    case PredicateType::Range: {
      if (lower_ != nullptr && upper_ != nullptr) {
        if (column_.type_info()->Compare(lower_, upper_) >= 0) {
          // If the range bounds are empty then no results can be returned.
          SetToNone();
        } else if (column_.type_info()->AreConsecutive(lower_, upper_)) {
          // If the values are consecutive, then it is an equality bound.
          predicate_type_ = PredicateType::Equality;
          upper_ = nullptr;
        }
      }
      return;
    };
    case PredicateType::InList: {
      if (values_.size() == 0) {
        // If the list is empty, no results can be returned.
        SetToNone();
      } else if (values_.size() == 1) {
        // List has only one value, so convert to Equality
        predicate_type_ = PredicateType::Equality;
        lower_ = *(values_.begin());
      }
      return;
    };
  }
  LOG(FATAL) << "unknown predicate type";
}

void ColumnPredicate::Merge(const ColumnPredicate& other) {
  CHECK(column_.Equals(other.column_, false));
  switch (predicate_type_) {
    case PredicateType::None: return;
    case PredicateType::Range: {
      MergeIntoRange(other);
      return;
    };
    case PredicateType::Equality: {
      MergeIntoEquality(other);
      return;
    };
    case PredicateType::IsNotNull: {
      // NOT NULL is less selective than all other predicate types, so the
      // intersection of NOT NULL with any other predicate is just the other
      // predicate.
      //
      // Note: this will no longer be true when an IS NULL predicate type is
      // added.
      predicate_type_ = other.predicate_type_;
      lower_ = other.lower_;
      upper_ = other.upper_;
      values_ = other.values_;
      return;
    };
    case PredicateType::InList: {
      MergeIntoInList(other);
      return;
    };
  }
  LOG(FATAL) << "unknown predicate type";
}

void ColumnPredicate::MergeIntoRange(const ColumnPredicate& other) {
  CHECK(predicate_type_ == PredicateType::Range);

  switch (other.predicate_type()) {
    case PredicateType::None: {
      SetToNone();
      return;
    };

    case PredicateType::Range: {
      // Set the lower bound to the larger of the two.
      if (other.lower_ != nullptr &&
          (lower_ == nullptr || column_.type_info()->Compare(lower_, other.lower_) < 0)) {
        lower_ = other.lower_;
      }

      // Set the upper bound to the smaller of the two.
      if (other.upper_ != nullptr &&
          (upper_ == nullptr || column_.type_info()->Compare(upper_, other.upper_) > 0)) {
        upper_ = other.upper_;
      }

      Simplify();
      return;
    };

    case PredicateType::Equality: {
      if (column_.type_info()->Compare(lower_, other.lower_) > 0 ||
          column_.type_info()->Compare(upper_, other.lower_) <= 0) {
        // The equality value does not fall in this range.
        SetToNone();
      } else {
        predicate_type_ = PredicateType::Equality;
        lower_ = other.lower_;
        upper_ = nullptr;
      }
      return;
    };
    case PredicateType::IsNotNull: return;
    case PredicateType::InList : {
      // The InList predicate values are examined to check whether
      // they lie in the range.
      // The current predicate is then converted from a range predicate to
      // an InList predicate (since it is more selective).
      // The number of values in the InList will depend on how many
      // values were within the range.
      // The call to Simplify() will then convert the InList if appropriate:
      // i.e an InList with zero entries gets converted to a NONE
      //     and InList with 1 entry gets converted into an Equality.
      values_ = other.values_;
      values_.erase(std::remove_if(values_.begin(), values_.end(),
                                   [this](const void* v) {
                                     return !CheckValueInRange(v);
                                   }), values_.end());
      predicate_type_ = PredicateType::InList;
      Simplify();
      return;
    };
  }
  LOG(FATAL) << "unknown predicate type";
}

void ColumnPredicate::MergeIntoEquality(const ColumnPredicate& other) {
  CHECK(predicate_type_ == PredicateType::Equality);

  switch (other.predicate_type()) {
    case PredicateType::None: {
      SetToNone();
      return;
    }
    case PredicateType::Range: {
      if (column_.type_info()->Compare(lower_, other.lower_) < 0 ||
          column_.type_info()->Compare(lower_, other.upper_) >= 0) {
        // This equality value does not fall in the other range.
        SetToNone();
      }
      return;
    };
    case PredicateType::Equality: {
      if (column_.type_info()->Compare(lower_, other.lower_) != 0) {
        SetToNone();
      }
      return;
    };
    case PredicateType::IsNotNull: return;
    case PredicateType::InList : {
      // The equality value needs to be a member of the InList
      if (!other.CheckValueInList(lower_)) {
        SetToNone();
      }
      return;
    };
  }
  LOG(FATAL) << "unknown predicate type";
}

void ColumnPredicate::MergeIntoInList(const ColumnPredicate &other) {
  CHECK(predicate_type_ == PredicateType::InList);

  switch (other.predicate_type()) {
    case PredicateType::None : {
      SetToNone();
      return;
    };
    case PredicateType::Range: {
      // Only values within the range should be retained
      values_.erase(std::remove_if(values_.begin(), values_.end(),
                                   [this, other](const void* v){
                                     return !(other.CheckValueInRange(v));
                                   }), values_.end());
      Simplify();
      return;
    }
    case PredicateType::Equality: {
      if (CheckValueInList(other.lower_)) {
        // value falls in list so change to Equality predicate
        predicate_type_ = PredicateType::Equality;
        lower_ = other.lower_;
        upper_ = nullptr;
      } else {
        SetToNone(); // Value does not fall in list
      }
      return;
    }
    case PredicateType::IsNotNull: return;
    case PredicateType::InList: {
      // Merge two InLists
      MergeInLists(other);
      return;
    };
  }
  LOG(FATAL) << "unknown predicate type";

}

namespace {
template <typename P>
void ApplyPredicate(const ColumnBlock& block, SelectionVector* sel, P p) {
  if (block.is_nullable()) {
    for (size_t i = 0; i < block.nrows(); i++) {
      if (!sel->IsRowSelected(i)) continue;
      const void *cell = block.nullable_cell_ptr(i);
      if (cell == nullptr || !p(cell)) {
        BitmapClear(sel->mutable_bitmap(), i);
      }
    }
  } else {
    for (size_t i = 0; i < block.nrows(); i++) {
      if (!sel->IsRowSelected(i)) continue;
      const void *cell = block.cell_ptr(i);
      if (!p(cell)) {
        BitmapClear(sel->mutable_bitmap(), i);
      }
    }
  }
}
} // anonymous namespace

void ColumnPredicate::Evaluate(const ColumnBlock& block, SelectionVector *sel) const {
  CHECK_NOTNULL(sel);

  // The type-specific predicate is provided as a function template to
  // ApplyPredicate in the hope that they are inlined.
  //
  // TODO: In the future we can improve this by also providing the type info as a
  // template, so that the type-specific data comparisons can be inlined.
  //
  // Going a step further we could do runtime codegen to inline the
  // lower/upper/equality bounds.

  // TODO: equality predicates should use the bloomfilter if it's available.

  switch (predicate_type()) {
    case PredicateType::None: {
      ApplyPredicate(block, sel, [] (const void*) {
          return false;
      });
      return;
    };
    case PredicateType::Range: {
      if (lower_ == nullptr) {
        ApplyPredicate(block, sel, [this] (const void* cell) {
            return column_.type_info()->Compare(cell, this->upper_) < 0;
        });
      } else if (upper_ == nullptr) {
        ApplyPredicate(block, sel, [this] (const void* cell) {
            return column_.type_info()->Compare(cell, this->lower_) >= 0;
        });
      } else {
        ApplyPredicate(block, sel, [this] (const void* cell) {
            return column_.type_info()->Compare(cell, this->upper_) < 0 &&
                   column_.type_info()->Compare(cell, this->lower_) >= 0;
        });
      }
      return;
    };
    case PredicateType::Equality: {
        ApplyPredicate(block, sel, [this] (const void* cell) {
            return column_.type_info()->Compare(cell, this->lower_) == 0;
        });
        return;
    };
    case PredicateType::IsNotNull: {
      if (!block.is_nullable()) return;
      // TODO: make this more efficient by using bitwise operations on the
      // null and selection vectors.
      for (size_t i = 0; i < block.nrows(); i++) {
        if (sel->IsRowSelected(i) && block.is_null(i)) {
          BitmapClear(sel->mutable_bitmap(), i);
        }
      }
      return;
    };
    case PredicateType::InList: {
      ApplyPredicate(block, sel, [this] (const void* cell) {
        return this->CheckValueInList(cell);
      });
      return;
    };
  }
  LOG(FATAL) << "unknown predicate type";
}

string ColumnPredicate::ToString() const {
  switch (predicate_type()) {
    case PredicateType::None: return strings::Substitute("`$0` NONE", column_.name());
    case PredicateType::Range: {
      if (lower_ == nullptr) {
        return strings::Substitute("`$0` < $1", column_.name(), column_.Stringify(upper_));
      } else if (upper_ == nullptr) {
        return strings::Substitute("`$0` >= $1", column_.name(), column_.Stringify(lower_));
      } else {
        return strings::Substitute("`$0` >= $1 AND `$0` < $2",
                                   column_.name(),
                                   column_.Stringify(lower_),
                                   column_.Stringify(upper_));
      }
    };
    case PredicateType::Equality: {
      return strings::Substitute("`$0` = $1", column_.name(), column_.Stringify(lower_));
    };
    case PredicateType::IsNotNull: {
      return strings::Substitute("`$0` IS NOT NULL", column_.name());
    };
    case PredicateType::InList: {
      string ss;
      ss.append("(");
      int values_cnt = 0;
      for (auto i = values_.begin(); i != values_.end(); ++i) {
        if (values_cnt++ > 0) {
          ss.append(", ");
        }
        ss.append(column_.Stringify(*i));
      }
      ss.append(")");
      return strings::Substitute("`$0` IN $1", column_.name(), ss);
    };
  }
  LOG(FATAL) << "unknown predicate type";
}

bool ColumnPredicate::operator==(const ColumnPredicate& other) const {
  if (!column_.Equals(other.column_, false)) { return false; }
  if (predicate_type_ != other.predicate_type_) {
    return false;
  } else if (predicate_type_ == PredicateType::Equality) {
    return column_.type_info()->Compare(lower_, other.lower_) == 0;
  } else if (predicate_type_ == PredicateType::Range) {
    return (lower_ == other.lower_ ||
            (lower_ != nullptr && other.lower_ != nullptr &&
             column_.type_info()->Compare(lower_, other.lower_) == 0)) &&
           (upper_ == other.upper_ ||
            (upper_ != nullptr && other.upper_ != nullptr &&
             column_.type_info()->Compare(upper_, other.upper_) == 0));
  } else if (predicate_type_ == PredicateType::InList) {
    if (values_.size() == other.values_.size()) {
      for (auto i = other.values_.begin(); i != other.values_.end(); ++i) {
        if (!CheckValueInList(*i)) {
          return false; // At lease one value doesnt exist in the InList
        }
      }
      return true; // All InLists values match
    } else {
      return false;
    }
  } else {
    return true;
  }
}

bool ColumnPredicate::CheckValueInRange(const void* value) const {
  CHECK(predicate_type_ == PredicateType::Range);
  return ((lower_ == nullptr || column_.type_info()->Compare(lower_, value) <= 0) &&
          (upper_ == nullptr || column_.type_info()->Compare(upper_, value) > 0));
}

bool ColumnPredicate::CheckValueInList(const void *value) const {
  if (values_.size() < 1) {
    return false;
  }

  return std::binary_search(values_.begin(), values_.end(), value,
                            [this](const void* lhs, const void* rhs) {
                              return (this->column_.type_info()->Compare(lhs, rhs) < 0);
                            });
}

void ColumnPredicate::MergeInLists(const ColumnPredicate& other) {
  // We iterate through the values_ list and remove elements that dont
  // exist in the other ColumnPredicate's list
  // Each list vector is sorted and contains only unique values.
  values_.erase(std::remove_if(values_.begin(), values_.end(),
                               [this, other](const void *v) {
                                 return !(other.CheckValueInList(v));
                               }), values_.end());
}

namespace {
int SelectivityRank(const ColumnPredicate& predicate) {
  int rank;
  switch (predicate.predicate_type()) {
    case PredicateType::None: rank = 0; break;
    case PredicateType::Equality: rank = 1; break;
    case PredicateType::InList: rank = 2; break;
    case PredicateType::Range: rank = 3; break;
    case PredicateType::IsNotNull: rank = 4; break;
    default: LOG(FATAL) << "unknown predicate type";
  }
  return rank * (kLargestTypeSize + 1) + predicate.column().type_info()->size();
}
} // anonymous namespace

int SelectivityComparator(const ColumnPredicate& left, const ColumnPredicate& right) {
  return SelectivityRank(left) - SelectivityRank(right);
}

} // namespace kudu
