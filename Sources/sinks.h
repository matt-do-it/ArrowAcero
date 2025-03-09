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

arrow::Result<std::shared_ptr<arrow::Table>> ExecutePlanToTable(ac::Declaration previousNode);
arrow::Status ExecutePlanToDataset(ac::Declaration previousNode, std::string dataset_path);
arrow::Result<std::shared_ptr<arrow::DoubleScalar>> TableToDoubleScalar(std::shared_ptr<arrow::Table> table);
arrow::Result<std::shared_ptr<arrow::ChunkedArray>> TableToArray(std::shared_ptr<arrow::Table> table);
