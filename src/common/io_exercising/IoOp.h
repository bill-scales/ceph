// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <string>
#include <memory>
#include "include/ceph_assert.h"

/* Overview
 *
 * enum Op
 *   Enumeration of different types of I/O operation
 *
 * class IoOp
 *   Stores details for an I/O operation. Generated by IoSequences
 *   and applied by IoExerciser's
 */

enum Op {
  IO_OP_DONE,       // End of I/O sequence
  IO_OP_BARRIER,    // Barrier - all prior I/Os must complete
  IO_OP_CREATE,     // Create object and pattern with data
  IO_OP_REMOVE,     // Remove object
  IO_OP_READ,       // Read
  IO_OP_READ2,      // 2 Reads in one op
  IO_OP_READ3,      // 3 Reads in one op
  IO_OP_WRITE,      // Write
  IO_OP_WRITE2,     // 2 Writes in one op
  IO_OP_WRITE3,     // 3 Writes in one op
  IO_OP_FAILWRITE   // Fail Write
};

class IoOp {
protected:
  std::string value_to_string(uint64_t v)
  {
    if (v < 1024 || (v % 1024) != 0) {
      return std::to_string(v);
    }else if (v < 1024*1024 || (v % (1024 * 1024)) != 0 ) {
      return std::to_string(v / 1024) + "K";
    }else{
      return std::to_string(v / 1024 / 1024) + "M";
    }
  }

public:
  Op  op;
  uint64_t offset1;
  uint64_t length1;
  uint64_t offset2;
  uint64_t length2;
  uint64_t offset3;
  uint64_t length3;

  IoOp( Op op,
	uint64_t offset1 = 0, uint64_t length1 = 0,
	uint64_t offset2 = 0, uint64_t length2 = 0,
	uint64_t offset3 = 0, uint64_t length3 = 0 ) :
    op(op),
    offset1(offset1), length1(length1),
    offset2(offset2), length2(length2),
    offset3(offset3), length3(length3) {}

  static std::unique_ptr<IoOp> generate_done() {
    return std::make_unique<IoOp>(IO_OP_DONE);
  }

  static std::unique_ptr<IoOp> generate_barrier() {
    return std::make_unique<IoOp>(IO_OP_BARRIER);
  }

  static std::unique_ptr<IoOp> generate_create(uint64_t size) {
    return std::make_unique<IoOp>(IO_OP_CREATE,0,size);
  }

  static std::unique_ptr<IoOp> generate_remove() {
    return std::make_unique<IoOp>(IO_OP_REMOVE);
  }

  static std::unique_ptr<IoOp> generate_read(uint64_t offset,
					     uint64_t length) {
    return std::make_unique<IoOp>(IO_OP_READ, offset, length);
  }

  static std::unique_ptr<IoOp> generate_read2(uint64_t offset1,
					      uint64_t length1,
					      uint64_t offset2,
					      uint64_t length2) {
    if (offset1 < offset2) {
      ceph_assert( offset1 + length1 <= offset2 );
    } else {
      ceph_assert( offset2 + length2 <= offset1 );
    }
    return std::make_unique<IoOp>(IO_OP_READ2,
				  offset1, length1,
				  offset2, length2);
  }

  static std::unique_ptr<IoOp> generate_read3(uint64_t offset1,
					      uint64_t length1,
					      uint64_t offset2,
					      uint64_t length2,
					      uint64_t offset3,
					      uint64_t length3) {
    if (offset1 < offset2) {
      ceph_assert( offset1 + length1 <= offset2 );
    } else {
      ceph_assert( offset2 + length2 <= offset1 );
    }
    if (offset1 < offset3) {
      ceph_assert( offset1 + length1 <= offset3 );
    } else {
      ceph_assert( offset3 + length3 <= offset1 );
    }
    if (offset2 < offset3) {
      ceph_assert( offset2 + length2 <= offset3 );
    } else {
      ceph_assert( offset3 + length3 <= offset2 );
    }
    return std::make_unique<IoOp>(IO_OP_READ3,
				  offset1, length1,
				  offset2, length2,
				  offset3, length3);
  }

  static std::unique_ptr<IoOp> generate_write(uint64_t offset,
					      uint64_t length) {
    return std::make_unique<IoOp>(IO_OP_WRITE, offset, length);
  }

  static std::unique_ptr<IoOp> generate_write2(uint64_t offset1,
					       uint64_t length1,
					       uint64_t offset2,
					       uint64_t length2) {
    if (offset1 < offset2) {
      ceph_assert( offset1 + length1 <= offset2 );
    } else {
      ceph_assert( offset2 + length2 <= offset1 );
    }
    return std::make_unique<IoOp>(IO_OP_WRITE2,
				  offset1, length1,
				  offset2, length2);
  }

  static std::unique_ptr<IoOp> generate_write3(uint64_t offset1,
					       uint64_t length1,
					       uint64_t offset2,
					       uint64_t length2,
					       uint64_t offset3,
					       uint64_t length3) {
    if (offset1 < offset2) {
      ceph_assert( offset1 + length1 <= offset2 );
    } else {
      ceph_assert( offset2 + length2 <= offset1 );
    }
    if (offset1 < offset3) {
      ceph_assert( offset1 + length1 <= offset3 );
    } else {
      ceph_assert( offset3 + length3 <= offset1 );
    }
    if (offset2 < offset3) {
      ceph_assert( offset2 + length2 <= offset3 );
    } else {
      ceph_assert( offset3 + length3 <= offset2 );
    }
    return std::make_unique<IoOp>(IO_OP_WRITE3,
				  offset1, length1,
				  offset2, length2,
				  offset3, length3);
  }

  static std::unique_ptr<IoOp> generate_fail_write(uint64_t offset,
					           uint64_t length) {
    return std::make_unique<IoOp>(IO_OP_FAILWRITE, offset, length);
  }

  bool done() {
    return (op == IO_OP_DONE);
  }

  std::string to_string(uint64_t block_size)
  {
    switch (op) {
    case IO_OP_DONE:
      return "Done";
    case IO_OP_BARRIER:
      return "Barrier";
    case IO_OP_CREATE:
      return "Create (size=" + value_to_string(length1 * block_size) + ")";
    case IO_OP_REMOVE:
      return "Remove";
    case IO_OP_READ:
      return "Read (offset=" + value_to_string(offset1 * block_size) +
	",length=" + value_to_string(length1 * block_size) + ")";
    case IO_OP_READ2:
      return "Read2 (offset1=" + value_to_string(offset1 * block_size) +
	",length1=" + value_to_string(length1 * block_size) +
        ",offset2=" + value_to_string(offset2 * block_size) +
	",length2=" + value_to_string(length2 * block_size) + ")";
    case IO_OP_READ3:
      return "Read3 (offset1=" + value_to_string(offset1 * block_size) +
	",length1=" + value_to_string(length1 * block_size) +
        ",offset2=" + value_to_string(offset2 * block_size) +
	",length2=" + value_to_string(length2 * block_size) +
        ",offset3=" + value_to_string(offset3 * block_size) +
	",length3=" + value_to_string(length3 * block_size) + ")";
    case IO_OP_WRITE:
      return "Write (offset=" + value_to_string(offset1 * block_size) +
	",length=" + value_to_string(length1 * block_size) + ")";
    case IO_OP_WRITE2:
      return "Write2 (offset1=" + value_to_string(offset1 * block_size) +
	",length1=" + value_to_string(length1 * block_size) +
        ",offset2=" + value_to_string(offset2 * block_size) +
	",length2=" + value_to_string(length2 * block_size) + ")";
    case IO_OP_WRITE3:
      return "Write3 (offset1=" + value_to_string(offset1 * block_size) +
	",length1=" + value_to_string(length1 * block_size) +
        ",offset2=" + value_to_string(offset2 * block_size) +
	",length2=" + value_to_string(length2 * block_size) +
        ",offset3=" + value_to_string(offset3 * block_size) +
	",length3=" + value_to_string(length3 * block_size) + ")";
    case IO_OP_FAILWRITE:
      return "Fail Write (offset=" + value_to_string(offset1 * block_size) +
	",length=" + value_to_string(length1 * block_size) + ")";
    default:
      break;
    }
    return "Unknown";
  }
};
