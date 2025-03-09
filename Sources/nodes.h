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

arrow::Result<ac::Declaration> OpenDatasetNode(std::string dataset_path);
ac::Declaration CalcQuantileNode(ac::Declaration previousNode, double quantile);
ac::Declaration GetValuesWithCountLargerThanNode(ac::Declaration previousNode, double value);
ac::Declaration FilterByValueSet(ac::Declaration previousNode, arrow::Datum valueSet);
ac::Declaration RecordBatchSourceNode(std::shared_ptr<arrow::RecordBatchReader> reader);
