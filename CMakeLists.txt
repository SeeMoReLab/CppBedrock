cmake_minimum_required(VERSION 3.14) 
project(CppBedrock VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Manually specify the include path for yaml-cpp
include_directories("/opt/homebrew/Cellar/yaml-cpp/0.9.0/include")
# Find nlohmann json package
find_package(nlohmann_json REQUIRED)
find_package(OpenSSL REQUIRED)

# Fallback: define OpenSSL imported targets when the find-module doesn’t
if(NOT TARGET OpenSSL::Crypto AND DEFINED OPENSSL_CRYPTO_LIBRARY)
  add_library(OpenSSL::Crypto UNKNOWN IMPORTED)
  set_target_properties(OpenSSL::Crypto PROPERTIES
    IMPORTED_LOCATION "${OPENSSL_CRYPTO_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${OPENSSL_INCLUDE_DIR}")
endif()
if(NOT TARGET OpenSSL::SSL AND DEFINED OPENSSL_SSL_LIBRARY)
  add_library(OpenSSL::SSL UNKNOWN IMPORTED)
  set_target_properties(OpenSSL::SSL PROPERTIES
    IMPORTED_LOCATION "${OPENSSL_SSL_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${OPENSSL_INCLUDE_DIR}")
endif()

# gRPC will pull protobuf, so don't call find_package(Protobuf) explicitly
# find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf REQUIRED)

# Manually specify the library path for yaml-cpp
link_directories("/opt/homebrew/Cellar/yaml-cpp/0.9.0/lib")

include_directories(include)

# Add source files for your project
set(SOURCES
    src/main.cpp
    src/core/state/StateMachine.cpp
    src/core/state/DataSet.cpp
    src/core/state/EntityState.cpp
    src/core/events/Event.cpp
    src/core/events/EventFactory.cpp
    src/core/Entity.cpp
    src/coordination/connections/TcpConnection.cpp
    src/coordination/CoordinationServer.cpp
    src/coordination/CoordinationUnit.cpp
    src/core/TimeKeeper.cpp
    src/core/crypto/OpenSSLCryptoProvider.cpp
    src/core/crypto/CryptoUtils.cpp
    src/core/events/EventFactoryProtocolSpecific.cpp
    src/utils/Benchmark.cpp
    src/coordination/grpc/GrpcServer.cpp  # NEW
    src/coordination/grpc/NodeServiceImpl.cpp   # NEW
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
# Expose ${CMAKE_BINARY_DIR} so includes like "proto/bedrock.grpc.pb.h" resolve
target_include_directories(bedrock_proto PUBLIC ${CMAKE_BINARY_DIR})

# === agent.proto codegen ===
set(AGENT_PROTO_FILE ${PROTO_DIR}/agent.proto)

add_custom_command(
  OUTPUT
    ${GENERATED_PROTO_DIR}/agent.pb.cc
    ${GENERATED_PROTO_DIR}/agent.pb.h
    ${GENERATED_PROTO_DIR}/agent.grpc.pb.cc
    ${GENERATED_PROTO_DIR}/agent.grpc.pb.h
  COMMAND ${CMAKE_COMMAND} -E make_directory ${GENERATED_PROTO_DIR}
  COMMAND $<TARGET_FILE:protobuf::protoc>
          --proto_path=${PROTO_DIR}
          --proto_path=${Protobuf_INCLUDE_DIRS}
          --cpp_out=${GENERATED_PROTO_DIR}
          ${AGENT_PROTO_FILE}
  COMMAND $<TARGET_FILE:protobuf::protoc>
          --proto_path=${PROTO_DIR}
          --proto_path=${Protobuf_INCLUDE_DIRS}
          --grpc_out=${GENERATED_PROTO_DIR}
          --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
          ${AGENT_PROTO_FILE}
  DEPENDS ${AGENT_PROTO_FILE}
  COMMENT "Generating protobuf and gRPC sources from ${AGENT_PROTO_FILE}"
)

add_library(agent_proto
  ${GENERATED_PROTO_DIR}/agent.pb.cc
  ${GENERATED_PROTO_DIR}/agent.grpc.pb.cc
)
target_link_libraries(agent_proto PUBLIC gRPC::grpc++ protobuf::libprotobuf)
target_include_directories(agent_proto PUBLIC ${CMAKE_BINARY_DIR})

# Add the library and link libraries
add_library(CppBedrockLib ${SOURCES})
add_dependencies(CppBedrockLib bedrock_proto agent_proto)
target_link_libraries(CppBedrockLib PUBLIC
  OpenSSL::Crypto OpenSSL::SSL
  yaml-cpp nlohmann_json::nlohmann_json
  bedrock_proto agent_proto gRPC::grpc++ protobuf::libprotobuf
)
target_include_directories(CppBedrockLib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

# Add the executable and link libraries
add_executable(${PROJECT_NAME} src/main.cpp)
target_link_libraries(${PROJECT_NAME} PRIVATE CppBedrockLib)

# Add integration test executable and link libraries
add_executable(integration_test tests/integration_test.cpp)
add_dependencies(integration_test bedrock_proto)
target_link_libraries(integration_test PRIVATE CppBedrockLib bedrock_proto gRPC::grpc++ protobuf::libprotobuf)
target_include_directories(integration_test PRIVATE ${CMAKE_BINARY_DIR})
