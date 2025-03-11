cmake_minimum_required(VERSION 3.16)

project(ArrowAcero)

find_package(Arrow REQUIRED)
find_package(ArrowAcero REQUIRED)
find_package(ArrowDataset REQUIRED)
find_package(Boost REQUIRED)

add_executable(sample main.cpp nodes.h nodes.cpp sample.h sample.cpp sinks.h sinks.cpp)

target_link_libraries(sample PRIVATE
	Arrow::arrow_shared 
	ArrowAcero::arrow_acero_shared
	ArrowDataset::arrow_dataset_shared
)	
