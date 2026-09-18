cmake_minimum_required(VERSION 3.14)
project(CppBedrock VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

find_package(nlohmann_json REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)
# libxml2 parses the shared failure_spec.xml (macOS SDK, Ubuntu libxml2-dev).
find_package(LibXml2 REQUIRED)

# yaml-cpp: prefer its CMake config (Homebrew, source builds); fall back to
# pkg-config for distro packages whose config file is missing or broken.
find_package(yaml-cpp CONFIG QUIET)
if(TARGET yaml-cpp::yaml-cpp)
  set(BEDROCK_YAML_TARGET yaml-cpp::yaml-cpp)
elseif(TARGET yaml-cpp)
  set(BEDROCK_YAML_TARGET yaml-cpp)
else()
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(YAMLCPP REQUIRED IMPORTED_TARGET GLOBAL yaml-cpp)
  set(BEDROCK_YAML_TARGET PkgConfig::YAMLCPP)
endif()

# gRPC: prefer its CMake package (Homebrew, source installs); fall back to
# pkg-config for distro packages, since Ubuntu's libgrpc++-dev ships no
# gRPCConfig.cmake.
find_package(gRPC CONFIG QUIET)
if(gRPC_FOUND)
  message(STATUS "Using gRPC via CMake config")
else()
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(GRPCPP REQUIRED IMPORTED_TARGET GLOBAL grpc++)
  add_library(gRPC::grpc++ ALIAS PkgConfig::GRPCPP)
  find_program(GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin REQUIRED)
  add_executable(gRPC::grpc_cpp_plugin IMPORTED GLOBAL)
  set_target_properties(gRPC::grpc_cpp_plugin PROPERTIES
    IMPORTED_LOCATION "${GRPC_CPP_PLUGIN_EXECUTABLE}")
  message(STATUS "Using gRPC via pkg-config (plugin: ${GRPC_CPP_PLUGIN_EXECUTABLE})")
endif()
find_package(Protobuf REQUIRED)

set(SOURCES
    src/core/Log.cpp
    src/core/Committee.cpp
    src/core/state/StateMachine.cpp
    src/core/state/DataSet.cpp
    src/core/state/EntityState.cpp
    src/core/events/Event.cpp
    src/core/events/EventFactory.cpp
    src/core/events/EventFactoryProtocolSpecific.cpp
    src/core/events/Message.cpp
    src/core/Entity.cpp
    src/core/EntityBatching.cpp
    src/core/EntityRequests.cpp
    src/core/EntityViewChange.cpp
    src/core/Consensus.cpp
    src/core/EntityConsensus.cpp
    src/core/EntityRecovery.cpp
    src/core/agent/AgentClient.cpp
    src/core/net/AsyncSender.cpp
    src/core/net/ClientStreams.cpp
    src/core/TimeKeeper.cpp
    src/core/PendingRequestTimer.cpp
    src/core/TaskScheduler.cpp
    src/core/FailureSpec.cpp
    src/core/crypto/OpenSSLCryptoProvider.cpp
    src/core/crypto/CryptoUtils.cpp
    src/coordination/grpc/NodeServiceImpl.cpp
)

# === gRPC/protobuf codegen ===
set(PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
set(PROTO_FILE ${PROTO_DIR}/bedrock.proto)
set(GENERATED_PROTO_DIR ${CMAKE_BINARY_DIR}/proto)

add_custom_command(
  OUTPUT
    ${GENERATED_PROTO_DIR}/bedrock.pb.cc
    ${GENERATED_PROTO_DIR}/bedrock.pb.h
    ${GENERATED_PROTO_DIR}/bedrock.grpc.pb.cc
    ${GENERATED_PROTO_DIR}/bedrock.grpc.pb.h
  COMMAND ${CMAKE_COMMAND} -E make_directory ${GENERATED_PROTO_DIR}
  COMMAND $<TARGET_FILE:protobuf::protoc>
          --proto_path=${PROTO_DIR}
          --cpp_out=${GENERATED_PROTO_DIR}
          ${PROTO_FILE}
  COMMAND $<TARGET_FILE:protobuf::protoc>
          --proto_path=${PROTO_DIR}
          --grpc_out=${GENERATED_PROTO_DIR}
          --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
          ${PROTO_FILE}
  DEPENDS ${PROTO_FILE}
  COMMENT "Generating protobuf and gRPC sources from ${PROTO_FILE}"
)

add_library(bedrock_proto
  ${GENERATED_PROTO_DIR}/bedrock.pb.cc
  ${GENERATED_PROTO_DIR}/bedrock.grpc.pb.cc
)
target_link_libraries(bedrock_proto PUBLIC gRPC::grpc++ protobuf::libprotobuf)
target_include_directories(bedrock_proto PUBLIC ${CMAKE_BINARY_DIR})

# === agent.proto codegen ===
# The learning-agent contract is the repo-wide proto/agent.proto shared with
# SmartBFT, Tendermint, the Rust protocols, and learning_agent; CppBedrock
# does not keep its own copy. BEDROCK_AGENT_PROTO overrides the location for
# builds outside the adaptive-timer checkout.
set(BEDROCK_AGENT_PROTO "${CMAKE_CURRENT_SOURCE_DIR}/../proto/agent.proto"
    CACHE FILEPATH "Path to the shared learning-agent proto (proto/agent.proto in the adaptive-timer repo)")
if(NOT EXISTS "${BEDROCK_AGENT_PROTO}")
  message(FATAL_ERROR "Shared agent proto not found at ${BEDROCK_AGENT_PROTO}; set BEDROCK_AGENT_PROTO.")
endif()
get_filename_component(AGENT_PROTO_DIR "${BEDROCK_AGENT_PROTO}" DIRECTORY)

add_custom_command(
  OUTPUT
    ${GENERATED_PROTO_DIR}/agent.pb.cc
    ${GENERATED_PROTO_DIR}/agent.pb.h
    ${GENERATED_PROTO_DIR}/agent.grpc.pb.cc
    ${GENERATED_PROTO_DIR}/agent.grpc.pb.h
  COMMAND ${CMAKE_COMMAND} -E make_directory ${GENERATED_PROTO_DIR}
  COMMAND $<TARGET_FILE:protobuf::protoc>
          --proto_path=${AGENT_PROTO_DIR}
          --proto_path=${Protobuf_INCLUDE_DIRS}
          --experimental_allow_proto3_optional
          --cpp_out=${GENERATED_PROTO_DIR}
          ${BEDROCK_AGENT_PROTO}
  COMMAND $<TARGET_FILE:protobuf::protoc>
          --proto_path=${AGENT_PROTO_DIR}
          --proto_path=${Protobuf_INCLUDE_DIRS}
          --experimental_allow_proto3_optional
          --grpc_out=${GENERATED_PROTO_DIR}
          --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
          ${BEDROCK_AGENT_PROTO}
  DEPENDS ${BEDROCK_AGENT_PROTO}
  COMMENT "Generating protobuf and gRPC sources from ${BEDROCK_AGENT_PROTO}"
)

add_library(agent_proto
  ${GENERATED_PROTO_DIR}/agent.pb.cc
  ${GENERATED_PROTO_DIR}/agent.grpc.pb.cc
)
target_link_libraries(agent_proto PUBLIC gRPC::grpc++ protobuf::libprotobuf)
target_include_directories(agent_proto PUBLIC ${CMAKE_BINARY_DIR})

add_library(CppBedrockLib ${SOURCES})
add_dependencies(CppBedrockLib bedrock_proto agent_proto)
target_link_libraries(CppBedrockLib PUBLIC
  OpenSSL::Crypto OpenSSL::SSL
  ${BEDROCK_YAML_TARGET} nlohmann_json::nlohmann_json
  bedrock_proto agent_proto gRPC::grpc++ protobuf::libprotobuf
  LibXml2::LibXml2
  Threads::Threads
)
target_include_directories(CppBedrockLib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

# Replica binary
add_executable(${PROJECT_NAME} src/main.cpp)
target_link_libraries(${PROJECT_NAME} PRIVATE CppBedrockLib)

# Benchmark client (rate-paced requests over client streams, Monitor lines)
add_executable(bedrock_client tests/bedrock_client.cpp)
target_link_libraries(bedrock_client PRIVATE CppBedrockLib bedrock_proto gRPC::grpc++ protobuf::libprotobuf)
target_include_directories(bedrock_client PRIVATE ${CMAKE_BINARY_DIR})

# Live smoke test for the learning-agent client (needs a running agent)
add_executable(agent_client_smoke tests/agent_client_smoke.cpp)
add_dependencies(agent_client_smoke agent_proto)
target_link_libraries(agent_client_smoke PRIVATE CppBedrockLib agent_proto gRPC::grpc++ protobuf::libprotobuf)
target_include_directories(agent_client_smoke PRIVATE ${CMAKE_BINARY_DIR})

# === Unit tests (ctest) ===
enable_testing()
foreach(test_name pending_request_timer_test task_scheduler_test failure_spec_test consensus_test)
  add_executable(${test_name} tests/${test_name}.cpp)
  target_link_libraries(${test_name} PRIVATE CppBedrockLib)
  target_include_directories(${test_name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
  target_compile_definitions(${test_name} PRIVATE BEDROCK_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
  add_test(NAME ${test_name} COMMAND ${test_name})
endforeach()
