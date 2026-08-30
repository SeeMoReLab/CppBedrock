#include "../../../include/core/crypto/CryptoUtils.h"
#include <openssl/evp.h>
#include <sstream>
#include <iomanip>
#include <stdexcept>
#include <string>

std::string computeSHA256(const std::string& input) {
    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int hashLen = 0;

    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) throw std::runtime_error("EVP_MD_CTX_new failed");

    if (EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) != 1)
        throw std::runtime_error("EVP_DigestInit_ex failed");
    if (EVP_DigestUpdate(ctx, input.data(), input.size()) != 1)
        throw std::runtime_error("EVP_DigestUpdate failed");
    if (EVP_DigestFinal_ex(ctx, hash, &hashLen) != 1)
        throw std::runtime_error("EVP_DigestFinal_ex failed");

    EVP_MD_CTX_free(ctx);

    std::ostringstream oss;
    for (unsigned int i = 0; i < hashLen; ++i)
        oss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
    return oss.str();
}