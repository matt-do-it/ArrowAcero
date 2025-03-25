cmake_minimum_required(VERSION 3.16)

project(ArrowAcero)

set(Boost_DEBUG 1)
set(BOOST_ROOT "/opt/local/libexec/boost/1.81/")

find_package(Arrow REQUIRED)
find_package(ArrowAcero REQUIRED)
find_package(ArrowDataset REQUIRED)
find_package(ArrowFlight REQUIRED)

find_package(Boost REQUIRED COMPONENTS url)

add_executable(sample main.cpp nodes.h nodes.cpp sample.h sample.cpp sinks.h sinks.cpp udf.h udf.cpp)

target_link_libraries(sample PRIVATE
	Arrow::arrow_shared 
	ArrowAcero::arrow_acero_shared
	ArrowDataset::arrow_dataset_shared
    Boost::url
)

add_executable(server server.cpp sample.h sample.cpp)

target_link_libraries(server PRIVATE
    Arrow::arrow_shared
    ArrowAcero::arrow_acero_shared
    ArrowDataset::arrow_dataset_shared
    ArrowFlight::arrow_flight_shared
    Boost::url
)

add_executable(client client.cpp)

target_link_libraries(client PRIVATE
    Arrow::arrow_shared
    ArrowAcero::arrow_acero_shared
    ArrowDataset::arrow_dataset_shared
    ArrowFlight::arrow_flight_shared
    Boost::url
)
