#include <iostream>
#include <algorithm>
#include <iterator>
#include <random>
#include <string>

#include <chrono>

#include <arrow/api.h>

#include <arrow/compute/api.h>
#include <arrow/acero/api.h>
#include <arrow/dataset/api.h>
#include <arrow/dataset/plan.h>

namespace ac = arrow::acero;
namespace cp = arrow::compute;

arrow::Result<std::shared_ptr<arrow::RecordBatch>> CreateSampleBatch();
arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> CreateRecordBatchReader();
arrow::Status WriteBatches(std::shared_ptr<arrow::RecordBatchReader> reader);
