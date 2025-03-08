cmake_minimum_required(VERSION 3.16)

project(ArrowAcero)

find_package(Arrow REQUIRED)
find_package(ArrowAcero REQUIRED)
find_package(ArrowDataset REQUIRED)

add_executable(sample main.cpp)
target_link_libraries(sample PRIVATE 
	Arrow::arrow_shared 
	ArrowAcero::arrow_acero_shared
	ArrowDataset::arrow_dataset_shared
)	
