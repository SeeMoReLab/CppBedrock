#include "../../../include/core/crypto/OpenSSLCryptoProvider.h"
#include <openssl/pem.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <openssl/sha.h>
#include <vector>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <cstring>
#include <iostream>
#include <iomanip>

namespace {
    std::string toHex(const unsigned char* data, size_t len) {
        std::ostringstream oss;
        for (size_t i = 0; i < len; ++i)
            oss << std::hex << std::setw(2) << std::setfill('0') << (int)data[i];
        return oss.str();
    }
}

OpenSSLCryptoProvider::OpenSSLCryptoProvider(const std::string& privateKeyPath) : pkey(nullptr) {
    std::cout << privateKeyPath.c_str() << std::endl << std::endl;
    FILE* fp = fopen(privateKeyPath.c_str(), "r");
    if (!fp) throw std::runtime_error("Cannot open private key file");
    pkey = PEM_read_PrivateKey(fp, nullptr, nullptr, nullptr);
    fclose(fp);
    if (!pkey) throw std::runtime_error("Cannot read private key");
}

OpenSSLCryptoProvider::~OpenSSLCryptoProvider() {
    if (pkey) EVP_PKEY_free((EVP_PKEY*)pkey);
}

std::string OpenSSLCryptoProvider::sign(const std::string& data) {
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) throw std::runtime_error("EVP_MD_CTX_new failed");
    if (EVP_DigestSignInit(ctx, nullptr, EVP_sha256(), nullptr, (EVP_PKEY*)pkey) != 1)
        throw std::runtime_error("DigestSignInit failed");
    if (EVP_DigestSignUpdate(ctx, data.data(), data.size()) != 1)
        throw std::runtime_error("DigestSignUpdate failed");
    size_t siglen = 0;
    if (EVP_DigestSignFinal(ctx, nullptr, &siglen) != 1)
        throw std::runtime_error("DigestSignFinal (size) failed");
    std::vector<unsigned char> sig(siglen);
    if (EVP_DigestSignFinal(ctx, sig.data(), &siglen) != 1)
        throw std::runtime_error("DigestSignFinal failed");
    EVP_MD_CTX_free(ctx);
    return toHex(sig.data(), siglen);
}

bool OpenSSLCryptoProvider::verify(const std::string& data, const std::string& signature, const std::string& pubkeyPath) {
    FILE* fp = fopen(pubkeyPath.c_str(), "r");
    if (!fp) return false;
    EVP_PKEY* pubkey = PEM_read_PUBKEY(fp, nullptr, nullptr, nullptr);
    fclose(fp);
    if (!pubkey) return false;

    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) { EVP_PKEY_free(pubkey); return false; }
    if (EVP_DigestVerifyInit(ctx, nullptr, EVP_sha256(), nullptr, pubkey) != 1) {
        EVP_MD_CTX_free(ctx); EVP_PKEY_free(pubkey); return false;
    }
    if (EVP_DigestVerifyUpdate(ctx, data.data(), data.size()) != 1) {
        EVP_MD_CTX_free(ctx); EVP_PKEY_free(pubkey); return false;
    }

    // Convert hex signature to binary
    std::vector<unsigned char> sig;
    for (size_t i = 0; i < signature.length(); i += 2) {
        std::string byteString = signature.substr(i, 2);
        unsigned char byte = (unsigned char) strtol(byteString.c_str(), nullptr, 16);
        sig.push_back(byte);
    }

    int ret = EVP_DigestVerifyFinal(ctx, sig.data(), sig.size());
    EVP_MD_CTX_free(ctx);
    EVP_PKEY_free(pubkey);
    return ret == 1;
}
