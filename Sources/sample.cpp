#import "sample.h"

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
