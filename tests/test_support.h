#pragma once

// Minimal assertion helpers for the CppBedrock unit tests (run via ctest).
// A failed check prints the expression and location and exits non-zero.

#include <cstdlib>
#include <iostream>
#include <string>

#define CHECK(cond)                                                                   \
    do {                                                                              \
        if (!(cond)) {                                                                \
            std::cerr << "CHECK failed: " #cond " at " << __FILE__ << ":" << __LINE__ \
                      << std::endl;                                                   \
            std::exit(1);                                                             \
        }                                                                             \
    } while (0)

#define CHECK_EQ(a, b)                                                                        \
    do {                                                                                      \
        const auto va_ = (a);                                                                 \
        const auto vb_ = (b);                                                                 \
        if (!(va_ == vb_)) {                                                                  \
            std::cerr << "CHECK_EQ failed: " #a " == " #b " (" << va_ << " vs " << vb_ << ")" \
                      << " at " << __FILE__ << ":" << __LINE__ << std::endl;                  \
            std::exit(1);                                                                     \
        }                                                                                     \
    } while (0)

inline int testPassed(const std::string& name) {
    std::cout << "[" << name << "] PASSED" << std::endl;
    return 0;
}
