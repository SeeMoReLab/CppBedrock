#pragma once

#include "CryptoProvider.h"
#include <string>
#include <mutex>
#include <unordered_map>

class OpenSSLCryptoProvider : public CryptoProvider {
public:
    OpenSSLCryptoProvider(const std::string& privateKeyPath);
    ~OpenSSLCryptoProvider();

    std::string sign(const std::string& data) override;
    bool verify(const std::string& data, const std::string& signature, const std::string& pubkey) override;

private:
    std::mutex keysMutex_;
    std::unordered_map<std::string, void*> publicKeys_;
    void* pkey; // EVP_PKEY*, opaque to avoid OpenSSL headers in .h
};