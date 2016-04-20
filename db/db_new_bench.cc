// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include "db/db_impl.h"
#include "db/version_set.h"
#include "leveldb/cache.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"
#include "port/port.h"
#include "util/crc32c.h"
#include "util/histogram.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/testutil.h"

#define READ_FRAC 0.5
#define SEED 0 

static int FLAGS_num = 100000;

static int FLAGS_reads = -1;

// Number of concurrent threads to run.
static int FLAGS_threads = 1;

// Size of each value
static int FLAGS_value_size = 100;

// Arrange to generate values that shrink to this fraction of
// their original size after compression
static double FLAGS_compression_ratio = 0.5;

// Print histogram of operation timings
static bool FLAGS_histogram = false;

// Number of bytes to buffer in memtable before compacting
// (initialized to default value by "main")
static int FLAGS_write_buffer_size = 0;

// Number of bytes to use as a cache of uncompressed data.
// Negative means use default settings.
static int FLAGS_cache_size = -1;

// Maximum number of files to keep open at the same time (use default if == 0)
static int FLAGS_open_files = 0;

// Bloom filter bits per key.
// Negative means use default settings.
static int FLAGS_bloom_bits = -1;

// If true, do not destroy the existing database.  If you set this
// flag and also specify a benchmark that wants a fresh database, that
// benchmark will fail.
static bool FLAGS_use_existing_db = false;

// If true, reuse existing log/MANIFEST files when re-opening a database.
static bool FLAGS_reuse_logs = false;

// Use the db with the following name.
static const char* FLAGS_db = "albertsam_bench";

namespace leveldb {

namespace {

struct CompareBenchResults {
    int num_writes;
    int num_reads;
    double read_micros;
    double write_micros;
};

class RandomGenerator {
 private:
  std::string data_;
  int pos_;

 public:
  RandomGenerator() {
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    Random rnd(301);
    std::string piece;
    while (data_.size() < 1048576) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      test::CompressibleString(&rnd, FLAGS_compression_ratio, 100, &piece);
      data_.append(piece);
    }
    pos_ = 0;
  }

  Slice Generate(size_t len) {
    if (pos_ + len > data_.size()) {
      pos_ = 0;
      assert(len < data_.size());
    }
    pos_ += len;
    return Slice(data_.data() + pos_ - len, len);
  }
};

} // namespace

class DBCompareBench {
  private:
    DB* db_;
    Random rand_;
    int value_size_;
    WriteOptions write_options_;

    DBImpl* dbfull() {
      return reinterpret_cast<DBImpl*>(db_);
    }

  public:
    DBCompareBench()
    : db_(NULL),
      rand_(SEED),
      value_size_(FLAGS_value_size) {
        write_options_ = WriteOptions();
    }

    ~DBCompareBench() {
      delete db_;
    }

    void Open() {
      assert(db_ == NULL);

      // destroy old database
      DestroyDB(FLAGS_db, Options());

      Options options;
      options.create_if_missing = !FLAGS_use_existing_db;
      options.write_buffer_size = FLAGS_write_buffer_size;
      options.max_open_files = FLAGS_open_files;
      options.reuse_logs = FLAGS_reuse_logs;
      Status s = DB::Open(options, FLAGS_db, &db_);
      if (!s.ok()) {
        fprintf(stderr, "open error: %s\n", s.ToString().c_str());
        exit(1);
      }
    }
 
    void DoWrite(int n) {
      int64_t bytes = 0;
      RandomGenerator gen;
      printf("began filling db\n");
      for (int i = 0; i < n; i++) {
        // TODO: change 
        if (i % 100000 == 0) {
          printf("done %d\n", i);
        }
        const int k = rand_.Next() % FLAGS_num;
        char key[100];
        snprintf(key, sizeof(key), "%016d", k);
        Status s = db_->Put(write_options_, key, gen.Generate(value_size_));
        if (!s.ok()) {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          exit(1);
        }
        bytes += value_size_ + strlen(key);
      }
      printf("done\n");

      // TODO: remove magic number
      /* for (int i = 0; i < 6; i++) { */
      /*   dbfull()->TEST_CompactRange(i, NULL, NULL); */
      /* } */
    }
    
    void ReadWriteRandom(CompareBenchResults* results,
                         int n, float read_fraction, int seed) {
      RandomGenerator gen;
      assert(read_fraction >= 0 && read_fraction <= 1);
      int found = 0;
      for (int i = 0; i < n; i++) {
        char key[100];
    
        // randomly decide whether to do a read or write
        const int read_rand = rand_.Next();

        time_t start = Env::Default()->NowMicros();
        if (read_rand <= (int)(read_fraction * RAND_MAX)) {
          std::string value;
          ReadOptions options;
          results->num_reads++;
          const int k = rand_.Next() % FLAGS_num;
          snprintf(key, sizeof(key), "%016d", k);
          if (db_->Get(options, key, &value).ok()) {
            found++;
          }
          results->read_micros += (Env::Default()->NowMicros() - start);
        } else {
          results->num_writes++;
          const int k = rand_.Next() % FLAGS_num;
          char key[100];
          snprintf(key, sizeof(key), "%016d", k);
          // TODO: fix this call
          db_->Put(write_options_, key, gen.Generate(value_size_));
          results->write_micros += (Env::Default()->NowMicros() - start);
        }
      }
    }
};

} // namespace leveldb


int main(int argc, char** argv) {
  leveldb::DBCompareBench benchmark;

  // create database
  benchmark.Open();

  // write out random amounts of starting data
  benchmark.DoWrite(FLAGS_num);

  // random read / write workload
  leveldb::CompareBenchResults results = {0, 0, 0, 0};
  benchmark.ReadWriteRandom(&results, FLAGS_num, READ_FRAC, SEED);

  printf("Average read time (ms): %f\n", results.read_micros / results.num_reads);
  printf("Average write time (ms): %f\n", results.write_micros / results.num_writes);

  return 0;
}

