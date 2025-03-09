#import "sinks.h"

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
