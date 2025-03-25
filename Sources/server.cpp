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
#include <arrow/flight/server.h>

#include <boost/url.hpp>

#import "nodes.h"
#import "sinks.h"
#import "sample.h"
#import "udf.h"

class SampleFlightServer : public arrow::flight::FlightServerBase {
    arrow::Status ListFlights(const arrow::flight::ServerCallContext& context,
                              const arrow::flight::Criteria* criteria,
                              std::unique_ptr<arrow::flight::FlightListing>* listings) override {
        
        std::vector<arrow::flight::FlightInfo> flights;
        
        ARROW_ASSIGN_OR_RAISE(auto flight, MakeFlightInfo("SAMPLE CMD"));
        flights.push_back(flight);
        
        *listings = std::unique_ptr<arrow::flight::FlightListing>(new arrow::flight::SimpleFlightListing(flights));
        
        return arrow::Status::OK();
    }
    
    arrow::Status GetFlightInfo(const arrow::flight::ServerCallContext&,
                                const arrow::flight::FlightDescriptor& descriptor,
                                std::unique_ptr<arrow::flight::FlightInfo>* info) override {
        
        ARROW_ASSIGN_OR_RAISE(auto flight_info, MakeFlightInfo(descriptor.cmd));
        
        *info = std::unique_ptr<arrow::flight::FlightInfo>(new arrow::flight::FlightInfo(std::move(flight_info)));
        
        return arrow::Status::OK();
    }
    
    arrow::Status DoGet(const arrow::flight::ServerCallContext&,
                        const arrow::flight::Ticket& request,
                        std::unique_ptr<arrow::flight::FlightDataStream>* stream) override {
        
        ARROW_ASSIGN_OR_RAISE(auto reader, CreateRecordBatchReader());
        
        *stream = std::unique_ptr<arrow::flight::FlightDataStream>(new arrow::flight::RecordBatchStream(reader));
        
        return arrow::Status::OK();
    }
    
private:
    arrow::Result<arrow::flight::FlightInfo> MakeFlightInfo(const std::string& command) {
        std::shared_ptr<arrow::Schema> schema = CreateSampleSchema();
                        
        arrow::flight::FlightEndpoint endpoint;
        endpoint.ticket.ticket = command;
        arrow::flight::Location location;
        
        ARROW_ASSIGN_OR_RAISE(location,
                              arrow::flight::Location::ForGrpcTcp("localhost", port()));
        endpoint.locations.push_back(location);
                
        auto descriptor = arrow::flight::FlightDescriptor::Path({command});

        return arrow::flight::FlightInfo::Make(*schema, descriptor, {endpoint}, 0, 0);
    }
};

arrow::Status RunMain() {
    std::unique_ptr<arrow::flight::FlightServerBase> server = std::make_unique<SampleFlightServer>();
    
    ARROW_ASSIGN_OR_RAISE(arrow::flight::Location location, arrow::flight::Location::ForGrpcTcp("0.0.0.0", 4500));
    
    arrow::flight::FlightServerOptions options(location);
    std::cout << options.location.scheme() << std::endl;
    
    // Start the server
    ARROW_RETURN_NOT_OK(server->Init(options));
    
    // Exit with a clean error code (0) on SIGTERM
    ARROW_RETURN_NOT_OK(server->SetShutdownOnSignals({SIGTERM}));
    
    std::cout << "Server listening on localhost:" << server->port() << std::endl;
    ARROW_RETURN_NOT_OK(server->Serve());
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
