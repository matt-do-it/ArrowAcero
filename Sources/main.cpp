#include <iostream>
#include <algorithm>
#include <iterator>
#include <random>
#include <string>

#include <arrow/api.h>
#include <arrow/compute/api.h>  // for Arrow's compute APIs
#include <arrow/acero/api.h>     // for Acero APIs


namespace ac = arrow::acero; 
namespace cp = arrow::compute; 

static const std::string groups[] = {
	"group_1", 
	"group_2"
};

arrow::Result<std::shared_ptr<arrow::RecordBatch>> CreateSampleBatch() {
  arrow::StringBuilder stringBuilder;
  arrow::UInt64Builder intBuilder;
  
  std::mt19937 g;
  std::uniform_int_distribution<unsigned> distr;
  
  for (int i = 0; i < 1000; i++) {
  	ARROW_RETURN_NOT_OK(stringBuilder.Append(groups[rand() % std::size(groups)]));
  	ARROW_RETURN_NOT_OK(intBuilder.Append(i));
  }
  // AppendValues, as called, puts 5 values from days_raw into our Builder object.
  
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
  
  for (int i = 0; i < 10; i++) {
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


arrow::Status SourceSinkExample() {
  std::shared_ptr<arrow::RecordBatchReader> reader;
  ARROW_ASSIGN_OR_RAISE(reader, CreateRecordBatchReader());
		
  auto source_node_options = ac::RecordBatchReaderSourceNodeOptions{reader};
  std::cout << "Source" << std::endl;

  ac::Declaration source{"record_batch_reader_source", std::move(source_node_options)};

/*
  auto options = std::make_shared<cp::CountOptions>(cp::CountOptions::ONLY_VALID);
  auto group_aggregate_options =
      ac::AggregateNodeOptions{{{"hash_sum", options, "Values", "Count(Values)"}},
                               {"Group"}};
  ac::Declaration group_aggregate{
      "aggregate", {std::move(source)}, std::move(group_aggregate_options)};
*/

//   auto quantile_options = std::make_shared<cp::QuantileOptions>();
//   auto quantile_aggregate_options =
//       ac::AggregateNodeOptions{{{"quantile", quantile_options, "Values", "count(a)"}}};
//   ac::Declaration quantile_aggregate{
//       "aggregate", {std::move(source)}, std::move(quantile_aggregate_options)};
 //  auto quantile_options = std::make_shared<cp::TDigestOptions>(0.995);
//   auto aggregate_options =
//       ac::AggregateNodeOptions{/*aggregates=*/{{"tdigest", quantile_options, "Values", "sum(a)"}}};
//   ac::Declaration aggregate{
//       "aggregate", {std::move(source)}, std::move(aggregate_options)};
// 
//   std::cout << "Exec" << std::endl;



  std::shared_ptr<arrow::RecordBatchReader> resultReader;
  ARROW_ASSIGN_OR_RAISE(resultReader, ac::DeclarationToReader(source));
  ARROW_RETURN_NOT_OK(WriteBatches(resultReader));
 
  return arrow::Status::OK();
}


arrow::Result<std::shared_ptr<arrow::DoubleScalar>> CalcQuantile(std::shared_ptr<arrow::RecordBatchReader> reader) {
  auto source_node_options = ac::RecordBatchReaderSourceNodeOptions{reader};
  std::cout << "Source" << std::endl;

  ac::Declaration source{"record_batch_reader_source", std::move(source_node_options)};


  auto options = std::make_shared<cp::CountOptions>(cp::CountOptions::ONLY_VALID);
  auto group_aggregate_options =
      ac::AggregateNodeOptions{{{"hash_sum", options, "Values", "Count(Values)"}},
                               {"Group"}};
  ac::Declaration group_aggregate{
      "aggregate", {std::move(source)}, std::move(group_aggregate_options)};

  auto quantile_options = std::make_shared<cp::TDigestOptions>(0.995);
  auto aggregate_options =
      ac::AggregateNodeOptions{/*aggregates=*/{{"tdigest", quantile_options, "Count(Values)", "tdigest"}}};
  ac::Declaration aggregate{
      "aggregate", {std::move(group_aggregate)}, std::move(aggregate_options)};

  std::shared_ptr<arrow::Table> table;
  ARROW_ASSIGN_OR_RAISE(table, ac::DeclarationToTable(aggregate));

    // Access the first column (index 0) from the Table
    std::shared_ptr<arrow::ChunkedArray> column = table->column(0);  // First column (int_column)

	std::shared_ptr<arrow::Scalar> scalar; 
	ARROW_ASSIGN_OR_RAISE(scalar, column->GetScalar(0));

  return arrow::Result(std::dynamic_pointer_cast<arrow::DoubleScalar>(scalar));
}

arrow::Result<std::shared_ptr<arrow::ChunkedArray>> Blacklist(std::shared_ptr<arrow::RecordBatchReader> reader) {
  auto source_node_options = ac::RecordBatchReaderSourceNodeOptions{reader};
  std::cout << "Source" << std::endl;

  ac::Declaration source{"record_batch_reader_source", std::move(source_node_options)};

  auto options = std::make_shared<cp::CountOptions>(cp::CountOptions::ONLY_VALID);
  auto group_aggregate_options =
      ac::AggregateNodeOptions{{{"hash_sum", options, "Values", "Count(Values)"}},
                               {"Group"}};
  ac::Declaration group_aggregate{
      "aggregate", {std::move(source)}, std::move(group_aggregate_options)};

  auto quantile_options = std::make_shared<cp::TDigestOptions>(0.995);
  auto aggregate_options =
      ac::AggregateNodeOptions{/*aggregates=*/{{"tdigest", quantile_options, "Count(Values)", "tdigest"}}};
  ac::Declaration aggregate{
      "aggregate", {std::move(group_aggregate)}, std::move(aggregate_options)};

  std::shared_ptr<arrow::Table> table;
  ARROW_ASSIGN_OR_RAISE(table, ac::DeclarationToTable(aggregate));

    // Access the first column (index 0) from the Table
    std::shared_ptr<arrow::ChunkedArray> column = table->column(0);  // First column (int_column)

	std::shared_ptr<arrow::Scalar> scalar; 
	ARROW_ASSIGN_OR_RAISE(scalar, column->GetScalar(0));

  return arrow::Result(std::dynamic_pointer_cast<arrow::DoubleScalar>(scalar));
}

arrow::Status RunMain() {
  std::shared_ptr<arrow::RecordBatchReader> reader;
  ARROW_ASSIGN_OR_RAISE(reader, CreateRecordBatchReader());

  std::shared_ptr<arrow::DoubleScalar> ipMaxQuantile;
  ARROW_ASSIGN_OR_RAISE(ipMaxQuantile, CalcQuantile(reader));
  
  std::cout << "Max quantile" << ipMaxQuantile->value << std::endl;
//  ARROW_RETURN_NOT_OK(SourceSinkExample());
  
  return arrow::Status::OK();
}

int main() {
  arrow::Status st = RunMain();
  if (!st.ok()) {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}
