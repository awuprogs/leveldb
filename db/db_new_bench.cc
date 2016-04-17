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

#define N 10000
#define READ_FRAC 0.5
#define SEED 0 

namespace leveldb {

namespace {

struct CompareBenchResults {
    int num_writes;
    int num_reads;
    double read_micros;
    double write_micros;
}

} // namespace

class DBCompareBench {
  private:
    DB* db_;
    Random rand_;

  public:
    DBCompareBench()
    : db_(NULL),
      rand_(SEED) {}

    ~DBCompareBench() {
      delete db_;
    }

    void Open() {
      assert(db_ == NULL);
      Options options;
      options.create_if_missing = !FLAGS_use_existing_db;
      options.block_cache = cache_;
      options.write_buffer_size = FLAGS_write_buffer_size;
      options.max_open_files = FLAGS_open_files;
      options.filter_policy = filter_policy_;
      options.reuse_logs = FLAGS_reuse_logs;
      Status s = DB::Open(options, FLAGS_db, &db_);
      if (!s.ok()) {
        fprintf(stderr, "open error: %s\n", s.ToString().c_str());
        exit(1);
      }
    }
 
    void DoWrite(int n) {
      int64_t bytes = 0;
      for (int i = 0; i < n; j++) {
        // TODO: change 
        k = rand_.Next() % FLAGS_num;
        char key[100];
        snprintf(key, sizeof(key), "%016d", k);
        s = db_->Write(write_options_, &batch);
        if (!s.ok()) {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          exit(1);
        }
        bytes += value_size_ + strlen(key);
      }

      // TODO: remove magic number
      for (int i = 0; i < 6; i++) {
        db_->TEST_CompactRange(i, NULL, NULL);
      }
    }
    
    void ReadWriteRandom(CompareBenchResults* results,
                         int n, float read_fraction, int seed) {
      assert(read_fraction >= 0 && read_fraction <= 1);
      ReadOptions options;
      std::string value;
      int found = 0;
      for (int i = 0; i < n; i++) {
        char key[100];
    
        // randomly decide whether to do a read or write
        const int read_rand = rand.Next();

        start = Env::Default()->NowMicros();
        if (read_rand <= (int)(read_fraction * RAND_MAX)) {
          results->num_reads++;
          const int k = rand.Next() % FLAGS_num;
          snprintf(key, sizeof(key), "%016d", k);
          if (db_->Get(options, key, &value).ok()) {
            found++;
          }
          results->read_micros += (Env::Default()->NowMicros() - start);
        } else {
          results->num_writes++;
          const int k = rand.Next() % FLAGS_num;
          char key[100];
          snprintf(key, sizeof(key), "%016d", k);
          // TODO: fix this call
          db_->Write(key, gen.Generate(value_size_));
          results->write_micros += (Env::Default()->NowMicros() - start);
        }
      }
    }
};

} // namespace leveldb


int main(int argc, char** argv) {
  leveldb::DBCompareBench benchmark;

  // create database
  benchmark::Open();

  // write out random amounts of starting data
  benchmark::DoWrite();

  // random read / write workload
  CompareBenchResults results = {0, 0, 0, 0};
  benchmark::ReadWriteRandom(&results, N, READ_FRAC, SEED);

  printf("Average read time (ms): %f\n", results.read_micros / results.num_reads);
  printf("Average write time (ms): %f\n", results.write_micros / results.num_writes);

  return 0;
}

