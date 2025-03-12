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

ac::Declaration AggregateValuesGreaterEqualThanNode(ac::Declaration previousNode,
                                                 std::string columnName,
                                                 double value);

ac::Declaration FilterNotInValueSet(ac::Declaration previousNode,
                                    std::string columnName,
                                    arrow::Datum valueSet);

ac::Declaration FilterRegexNode(ac::Declaration previousNode,
                                std::string columnName,
                                  std::string pattern);

ac::Declaration RecordBatchSourceNode(std::shared_ptr<arrow::RecordBatchReader> reader);

ac::Declaration ProjectNode(std::string projectName,
                            ac::Declaration previousNode,
                            std::vector<std::string> keepColumns,
                            std::string columnName,
                            std::string projectedColumnName,
                            std::shared_ptr<cp::FunctionOptions> options);

ac::Declaration FilterNode(std::string filterName,
                           ac::Declaration previousNode,
                           std::string columnName,
                           std::shared_ptr<cp::FunctionOptions> options);
