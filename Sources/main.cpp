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

static const std::string groups[] = {
    "group_1",
    "group_2",
    "group_3",
    "group_4",
    "group_5",
    "group_6",
};

arrow::Result<std::shared_ptr<arrow::RecordBatch>> CreateSampleBatch() {
    arrow::StringBuilder stringBuilder;
    arrow::UInt64Builder intBuilder;
    
    std::mt19937 g;
    std::uniform_int_distribution<unsigned> distr;
    
    for (int i = 0; i < 100000; i++) {
        ARROW_RETURN_NOT_OK(stringBuilder.Append(groups[rand() % std::size(groups)]));
        ARROW_RETURN_NOT_OK(intBuilder.Append(i));
    }
    
    // We only have a Builder though, not an Array -- the following code pushes out the
    // built up data into a proper Array.
    std::shared_ptr<arrow::Array> group;
    ARROW_ASSIGN_OR_RAISE(group, stringBuilder.Finish());
    
    std::shared_ptr<arrow::Array> values;
    ARROW_ASSIGN_OR_RAISE(values, intBuilder.Finish());
    
    // Now, we want a RecordBatch, which has columns and labels for said columns.
    // This gets us to the 2d data structures we want in Arrow.
    // These are defined by schema, which have fields -- here we get both those object types
    // ready.
    std::shared_ptr<arrow::Field> field_group = arrow::field("Group", arrow::utf8());
    std::shared_ptr<arrow::Field> field_value = arrow::field("Values", arrow::int64());
    std::shared_ptr<arrow::Schema> schema = arrow::schema({ field_group, field_value });
    
    // With the schema and Arrays full of data, we can make our RecordBatch! Here,
    // each column is internally contiguous. This is in opposition to Tables, which we'll
    // see next.
    std::shared_ptr<arrow::RecordBatch> rbatch;
    
    // The RecordBatch needs the schema, length for columns, which all must match,
    // and the actual data itself.
    rbatch = arrow::RecordBatch::Make(schema, values->length(), {group, values});
    
    return arrow::Result<std::shared_ptr<arrow::RecordBatch>>(rbatch);
}

arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> CreateRecordBatchReader() {
    std::shared_ptr<arrow::RecordBatchReader> reader;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    
    for (int i = 0; i < 1000; i++) {
        std::shared_ptr<arrow::RecordBatch> rbatch;
        ARROW_ASSIGN_OR_RAISE(rbatch, CreateSampleBatch());
        batches.push_back(rbatch);
    }
    
    ARROW_ASSIGN_OR_RAISE(reader, arrow::RecordBatchReader::Make(batches));
    
    return arrow::Result<std::shared_ptr<arrow::RecordBatchReader>>(reader);
}

ac::Declaration RecordBatchSourceNode(std::shared_ptr<arrow::RecordBatchReader> reader) {
    auto source_node_options = ac::RecordBatchReaderSourceNodeOptions{reader};
    
    ac::Declaration source{"record_batch_reader_source", std::move(source_node_options)};
    
    return source;
}

ac::Declaration CalcQuantileNode(ac::Declaration previousNode, double quantile) {
    auto options = std::make_shared<cp::CountOptions>(cp::CountOptions::ONLY_VALID);
    auto group_aggregate_options =
    ac::AggregateNodeOptions{{{"hash_count", options, "Values", "Count(Values)"}},
        {"Group"}};
    ac::Declaration group_aggregate{
        "aggregate", {std::move(previousNode)}, std::move(group_aggregate_options)};
    
    auto quantile_options = std::make_shared<cp::TDigestOptions>(quantile);
    auto aggregate_options =
    ac::AggregateNodeOptions{/*aggregates=*/{{"tdigest", quantile_options, "Count(Values)", "tdigest"}}};
    ac::Declaration aggregate{
        "aggregate", {std::move(group_aggregate)}, std::move(aggregate_options)};
    
    return aggregate;
}


ac::Declaration GetValuesWithCountLargerThanNode(ac::Declaration previousNode, double value) {
    /*
     * Aggregate values and count to a table
     */
    auto count_options = std::make_shared<cp::CountOptions>(cp::CountOptions::ONLY_VALID);
    auto group_aggregate_options =
    ac::AggregateNodeOptions{{{"hash_count", count_options, "Values", "count"}},
        {"Group"}};
    
    ac::Declaration group_aggregate{
        "aggregate", {std::move(previousNode)}, std::move(group_aggregate_options)};
    
    cp::Expression filter_expr = cp::greater_equal(cp::field_ref("count"), cp::literal(value));
    ac::Declaration filter_node{
        "filter", {std::move(group_aggregate)}, ac::FilterNodeOptions(std::move(filter_expr))};
    
    return filter_node;
}

ac::Declaration FilterByValueSet(ac::Declaration previousNode, arrow::Datum valueSet) {
    auto quantile_options = std::make_shared<cp::SetLookupOptions>(arrow::Datum(valueSet));
    
    cp::Expression filter_expr = cp::call("is_in", std::vector<cp::Expression>{
        cp::field_ref("Group")
    }, quantile_options);
    
    cp::Expression notF = cp::not_(filter_expr);
    
    ac::Declaration filter_node{
        "filter", {std::move(previousNode)}, ac::FilterNodeOptions(std::move(notF))};
    
    return filter_node;
}

arrow::Result<std::shared_ptr<arrow::Table>> ExecutePlanToTable(ac::Declaration previousNode) {
    
    std::shared_ptr<arrow::Table> table;
    ARROW_ASSIGN_OR_RAISE(table, ac::DeclarationToTable(previousNode));
    
    return table;
}

arrow::Status ExecutePlanToDataset(ac::Declaration previousNode, std::string dataset_path) {
    std::string root_path;
    
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::fs::FileSystem> filesystem,
                          arrow::fs::FileSystemFromUri(dataset_path, &root_path));
    
    auto set_path = root_path;
    std::cout << "Writing dataset to " << set_path << std::endl;
    
    // ARROW_RETURN_NOT_OK(filesystem->DeleteDirContents(base_path));
    ARROW_RETURN_NOT_OK(filesystem->CreateDir(set_path));
    
    auto partition_schema = arrow::schema({arrow::field("Group", arrow::utf8())});
    
    auto partitioning =
    std::make_shared<arrow::dataset::HivePartitioning>(partition_schema);
    
    // We'll write Parquet files.
    auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();
    
    arrow::dataset::FileSystemDatasetWriteOptions write_options;
    write_options.file_write_options = format->DefaultWriteOptions();
    write_options.filesystem = filesystem;
    write_options.base_dir = set_path;
    write_options.partitioning = partitioning;
    write_options.basename_template = "part{i}.parquet";
    write_options.existing_data_behavior = arrow::dataset::ExistingDataBehavior::kOverwriteOrIgnore;
    arrow::dataset::WriteNodeOptions write_node_options{write_options};
    
    ac::Declaration filter_node{
        "write", {std::move(previousNode)}, write_node_options};

    ARROW_RETURN_NOT_OK(ac::DeclarationToStatus(filter_node, true));
    
     std::cout << "Dataset written to " << set_path << std::endl;
    
    return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::DoubleScalar>> TableToDoubleScalar(std::shared_ptr<arrow::Table> table) {
    // Access the first column (index 0) from the Table
    std::shared_ptr<arrow::ChunkedArray> column = table->column(0);  // First column (int_column)
    
    std::shared_ptr<arrow::Scalar> scalar;
    ARROW_ASSIGN_OR_RAISE(scalar, column->GetScalar(0));
    
    return arrow::Result(std::dynamic_pointer_cast<arrow::DoubleScalar>(scalar));
}


arrow::Result<std::shared_ptr<arrow::ChunkedArray>> TableToArray(std::shared_ptr<arrow::Table> table) {
    // Access the first column (index 0) from the Table
    std::shared_ptr<arrow::ChunkedArray> valuesColumn = table->column(0);  // First column
    
    return valuesColumn;
}

arrow::Status WriteBatches(std::shared_ptr<arrow::RecordBatchReader> reader) {
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        ARROW_RETURN_NOT_OK(reader->ReadNext(&batch));
        if (!batch) {
            break;
        }
        std::cout << batch->ToString();
    }
    
    return arrow::Status::OK();
}



arrow::Status RunMain() {
    /*
     * Calculate the quantile
     */
    auto startTime = std::chrono::high_resolution_clock::now();
    auto endTime = startTime;
    std::chrono::milliseconds duration;
    
    std::cout << "Calculating quantile..." << std::endl;
    
    std::shared_ptr<arrow::RecordBatchReader> reader;
    ARROW_ASSIGN_OR_RAISE(reader, CreateRecordBatchReader());
    
    ac::Declaration sourceNode = RecordBatchSourceNode(reader);
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
