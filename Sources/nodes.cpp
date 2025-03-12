#import "nodes.h"
#import "udf.h"

arrow::Result<ac::Declaration> OpenDatasetNode(std::string dataset_path) {
    std::string root_path;
    
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::fs::FileSystem> filesystem,
                          arrow::fs::FileSystemFromUri(dataset_path, &root_path));
    
    auto set_path = root_path;
    std::cout << "Opening dataset from " << set_path << std::endl;
    
    arrow::fs::FileSelector selector;
    selector.base_dir = set_path;
    selector.recursive = true;  // Make sure to search subdirectories
    
    arrow::dataset::FileSystemFactoryOptions options;
    options.partitioning = arrow::dataset::HivePartitioning::MakeFactory();
    
    
    // We'll reat Parquet files.
    auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();
    
    
    ARROW_ASSIGN_OR_RAISE(
                          auto factory, arrow::dataset::FileSystemDatasetFactory::Make(filesystem, selector, format, options));
    
    ARROW_ASSIGN_OR_RAISE(auto dataset, factory->Finish());
    // Print out the fragments
    
    ARROW_ASSIGN_OR_RAISE(auto fragments, dataset->GetFragments())
    for (const auto& fragment : fragments) {
        std::cout << "Found fragment: " << (*fragment)->ToString() << std::endl;
    }
    
    auto scan_options = std::make_shared<arrow::dataset::ScanOptions>();
    scan_options->projection = cp::project({}, {});  // create empty projection
    
    // construct the scan node
    auto scan_node_options = arrow::dataset::ScanNodeOptions{dataset, scan_options};
    
    ac::Declaration scan{"scan", std::move(scan_node_options)};
    
    return scan;
}

ac::Declaration CalcQuantileNode(ac::Declaration previousNode, double quantile) {
    auto options = std::make_shared<cp::CountOptions>(cp::CountOptions::ONLY_VALID);
    auto group_aggregate_options =
    ac::AggregateNodeOptions{{{"hash_count", options, "value", "Count(value)"}},
        {"group"}};
    ac::Declaration group_aggregate{
        "aggregate", {std::move(previousNode)}, std::move(group_aggregate_options)};
    
    auto quantile_options = std::make_shared<cp::TDigestOptions>(quantile);
    auto aggregate_options =
    ac::AggregateNodeOptions{/*aggregates=*/{{"tdigest", quantile_options, "Count(value)", "tdigest"}}};
    ac::Declaration aggregate{
        "aggregate", {std::move(group_aggregate)}, std::move(aggregate_options)};
    
    return aggregate;
}


ac::Declaration AggregateValuesGreaterEqualThanNode(ac::Declaration previousNode,
                                                    std::string columnName,
                                                    double value) {
    /*
     * Aggregate values and count to a table
     */
    auto count_options = std::make_shared<cp::CountOptions>(cp::CountOptions::ONLY_VALID);
    auto group_aggregate_options =
    ac::AggregateNodeOptions{{{"hash_count", count_options, "value", "count"}},
        {"group"}};
    
    ac::Declaration group_aggregate{
        "aggregate", {std::move(previousNode)}, std::move(group_aggregate_options)};
    
    cp::Expression filter_expr = cp::greater_equal(cp::field_ref("count"), cp::literal(value));
    ac::Declaration filter_node{
        "filter", {std::move(group_aggregate)}, ac::FilterNodeOptions(std::move(filter_expr))};
    
    return filter_node;
}

ac::Declaration FilterNotInValueSet(ac::Declaration previousNode,
                                    std::string columnName,
                                    arrow::Datum valueSet) {
    auto quantile_options = std::make_shared<cp::SetLookupOptions>(arrow::Datum(valueSet));
    
    cp::Expression filter_expr = cp::call("is_in", std::vector<cp::Expression>{
        cp::field_ref(columnName)
    }, quantile_options);
    
    cp::Expression notF = cp::not_(filter_expr);
    
    ac::Declaration filter_node{
        "filter", {std::move(previousNode)}, ac::FilterNodeOptions(std::move(notF))};
    
    return filter_node;
}

ac::Declaration FilterByRegexNode(ac::Declaration previousNode,
                                  std::string columnName,
                                  std::string pattern) {
    auto matchOptions = std::make_shared<cp::MatchSubstringOptions>(pattern);
    
    cp::Expression filter_expr = cp::call("match_substring_regex", std::vector<cp::Expression>{
        cp::field_ref(columnName)
    }, matchOptions);
    
    
    ac::Declaration filter_node{
        "filter", {std::move(previousNode)}, ac::FilterNodeOptions(std::move(filter_expr))};
    
    return filter_node;
}


ac::Declaration ProjectNode(std::string projectName,
                            ac::Declaration previousNode,
                            std::vector<std::string> keepColumns,
                            std::string columnName,
                            std::string projectedColumnName,
                            std::shared_ptr<cp::FunctionOptions> options) {
    cp::Expression projectExpr = cp::call(projectName, std::vector<cp::Expression>{
        cp::field_ref(columnName)
    }, options);
    
    std::vector<cp::Expression> keepColumnsRef;
    std::vector<std::string> fieldNames;
    
    for (const auto& value : keepColumns) {
        keepColumnsRef.push_back(cp::field_ref(value));
        fieldNames.push_back(value);
    }
    keepColumnsRef.push_back(projectExpr);
    fieldNames.push_back(projectedColumnName);
    
    ac::Declaration filter_node{
        "project", {std::move(previousNode)}, ac::ProjectNodeOptions(keepColumnsRef, fieldNames)};
    
    return filter_node;
}

ac::Declaration FilterNode(std::string filterName,
                           ac::Declaration previousNode,
                           std::string columnName,
                           std::shared_ptr<cp::FunctionOptions> options) {
        
    cp::Expression filter_expr = cp::call(filterName, std::vector<cp::Expression>{
        cp::field_ref(columnName)
    }, options);
    
    
    ac::Declaration filter_node{
        "filter", {std::move(previousNode)}, ac::FilterNodeOptions(std::move(filter_expr))};
    
    return filter_node;
}

ac::Declaration RecordBatchSourceNode(std::shared_ptr<arrow::RecordBatchReader> reader) {
    auto source_node_options = ac::RecordBatchReaderSourceNodeOptions{reader};
    
    ac::Declaration source{"record_batch_reader_source", std::move(source_node_options)};
    
    return source;
}
