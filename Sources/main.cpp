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

#import "nodes.h"
#import "sinks.h"
#import "sample.h"


arrow::Status RunMain() {
    /*
     * Calculate the quantile
     */
    auto startTime = std::chrono::high_resolution_clock::now();
    auto endTime = startTime;
    std::chrono::milliseconds duration;
    
    std::cout << "Calculating quantile..." << std::endl;
    
 //   std::shared_ptr<arrow::RecordBatchReader> reader;
 //   ARROW_ASSIGN_OR_RAISE(reader, CreateRecordBatchReader());
    
    ARROW_ASSIGN_OR_RAISE(ac::Declaration sourceNode, OpenDatasetNode("file:///Users/herold/Desktop/test/parquet"));
    ac::Declaration calcQuantileNode = CalcQuantileNode(sourceNode, 0.995);
    
    std::shared_ptr<arrow::Table> table;
    ARROW_ASSIGN_OR_RAISE(table, ExecutePlanToTable(calcQuantileNode));
    
    std::shared_ptr<arrow::DoubleScalar> quantile;
    ARROW_ASSIGN_OR_RAISE(quantile, TableToDoubleScalar(table));
    
    std::cout << "Calculated quantile..." << std::endl;
    std::cout << quantile->ToString() << std::endl;
    
    /* Measure timing */
    endTime = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
    std::cout << "-- Execution duration: " << duration.count() << "ms\n";
    startTime = std::chrono::high_resolution_clock::now();
    /* Measure timing */
    
    /*
     * Filter values with count larger than 10
     */
    std::cout << "Building exclude group list..." << std::endl;
    
    std::shared_ptr<arrow::RecordBatchReader> reader2;
    ARROW_ASSIGN_OR_RAISE(reader2, CreateRecordBatchReader());
    
    ac::Declaration sourceNode2 = RecordBatchSourceNode(reader2);
    ac::Declaration valuesLargerThanNode = GetValuesWithCountLargerThanNode(sourceNode2, quantile->value);
    
    std::shared_ptr<arrow::Table> table2;
    ARROW_ASSIGN_OR_RAISE(table2, ExecutePlanToTable(valuesLargerThanNode));
    
    std::shared_ptr<arrow::ChunkedArray> array;
    ARROW_ASSIGN_OR_RAISE(array, TableToArray(table2));
    
    std::cout << "Excluded groups: " << std::endl;
    std::cout << array->ToString() << std::endl;
    
    /* Measure timing */
    endTime = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
    std::cout << "-- Execution duration: " << duration.count() << "ms\n";
    startTime = std::chrono::high_resolution_clock::now();
    /* Measure timing */
    
    /*
     * Filter original list
     */
    std::cout << "Filtering orginal list..." << std::endl;
    
    std::shared_ptr<arrow::RecordBatchReader> reader3;
    ARROW_ASSIGN_OR_RAISE(reader3, CreateRecordBatchReader());
    
    ac::Declaration sourceNode3 = RecordBatchSourceNode(reader3);
    ac::Declaration valueSetFilter = FilterByValueSet(sourceNode3, array);
    
    std::shared_ptr<arrow::Table> table3;
    ARROW_ASSIGN_OR_RAISE(table3, ExecutePlanToTable(valueSetFilter));
    
    std::cout << "Final results" << std::endl;
    std::cout << table3->ToString() << std::endl;
    
    /* Measure timing */
    endTime = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
    std::cout << "-- Execution duration: " << duration.count() << "ms\n";
    /* Measure timing */
    
    /*
     * Filter and write dataset
     */
    std::shared_ptr<arrow::RecordBatchReader> reader4;
    ARROW_ASSIGN_OR_RAISE(reader4, CreateRecordBatchReader());
    
    ac::Declaration sourceNode4 = RecordBatchSourceNode(reader4);
    ac::Declaration valueSetFilter2 = FilterByValueSet(sourceNode4, array);
    
    ARROW_RETURN_NOT_OK(ExecutePlanToDataset(valueSetFilter2, "file:///Users/herold/Desktop/test/parquet"));
    
    /* Measure timing */
    endTime = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
    std::cout << "-- Execution duration: " << duration.count() << "ms\n";
    /* Measure timing */
    
    return arrow::Status::OK();
}

int main() {
    std::cout << "Thread CPU Pool capacity: " << arrow::GetCpuThreadPoolCapacity() << std::endl;
    std::cout << "Thread I/O Pool capacity: " << arrow::io::GetIOThreadPoolCapacity() << std::endl;
    arrow::dataset::internal::Initialize();
    arrow::Status st = RunMain();
    if (!st.ok()) {
        std::cerr << st << std::endl;
        return 1;
    }
    return 0;
}
