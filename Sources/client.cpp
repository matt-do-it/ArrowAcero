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
#include <arrow/flight/client.h>

#include <boost/url.hpp>

#import "nodes.h"
#import "sinks.h"
#import "sample.h"
#import "udf.h"

arrow::Status RunMain() {
    arrow::flight::Location location;
    ARROW_ASSIGN_OR_RAISE(location,
                          arrow::flight::Location::ForGrpcTcp("localhost", 4500));
    
    std::unique_ptr<arrow::flight::FlightClient> client;
    ARROW_ASSIGN_OR_RAISE(client, arrow::flight::FlightClient::Connect(location));
    std::cout << "Connected to " << location.ToString() << std::endl;
    
    std::unique_ptr<arrow::flight::FlightListing> flightListing;
    ARROW_ASSIGN_OR_RAISE(flightListing, client->ListFlights());
    
    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<arrow::flight::FlightInfo> flight_info, flightListing->Next());
    
    /* Read total */
    std::unique_ptr<arrow::flight::FlightStreamReader> stream;
    ARROW_ASSIGN_OR_RAISE(stream, client->DoGet(flight_info->endpoints()[0].ticket));
    std::shared_ptr<arrow::Table> table;
    
    ARROW_ASSIGN_OR_RAISE(table, stream->ToTable());
    arrow::PrettyPrintOptions print_options(0, 2);
    ARROW_RETURN_NOT_OK(arrow::PrettyPrint(*table, print_options, &std::cout));
    
    return arrow::Status::OK();
}

int main() {
    arrow::dataset::internal::Initialize();
    arrow::Status st = RunMain();
    if (!st.ok()) {
        std::cerr << st << std::endl;
        return 1;
    }
    return 0;
}
