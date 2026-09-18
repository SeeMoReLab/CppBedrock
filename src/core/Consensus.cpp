#include "core/Consensus.h"
#include "core/crypto/CryptoUtils.h"
#include <stdexcept>

namespace bedrock {
std::string encodeEnvelope(const ProtocolEnvelope& env) {
    const auto bytes = env.SerializeAsString();
    static constexpr char hex[] = "0123456789abcdef";
    std::string out;
    out.reserve(bytes.size() * 2);
    for (unsigned char c : bytes) { out += hex[c >> 4]; out += hex[c & 15]; }
    return out;
}

ProtocolEnvelope decodeEnvelope(const nlohmann::json& encoded) {
    const auto& text = encoded.get_ref<const std::string&>();
    if (text.size() % 2 || text.size() > 16 * 1024 * 1024)
        throw std::invalid_argument("invalid consensus proof encoding");
    auto digit = [](char c) -> unsigned {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        throw std::invalid_argument("invalid consensus proof hex digit");
    };
    std::string bytes;
    bytes.reserve(text.size() / 2);
    for (size_t i = 0; i < text.size(); i += 2)
        bytes += static_cast<char>((digit(text[i]) << 4) | digit(text[i + 1]));
    ProtocolEnvelope env;
    if (!env.ParseFromString(bytes)) throw std::invalid_argument("invalid consensus proof protobuf");
    return env;
}

std::string requestDigest(const PrePrepare& p) {
    // Stable across views. Length-prefixed fields give an unambiguous,
    // protobuf-version-independent encoding; order is part of the digest.
    std::string bytes = "CppBedrock/batch/v3/";
    auto number = [&](uint64_t value) {
        for (int shift = 56; shift >= 0; shift -= 8) bytes.push_back(static_cast<char>(value >> shift));
    };
    auto field = [&](const std::string& value) { number(value.size()); bytes += value; };
    number(p.requests_size());
    for (const auto& r : p.requests()) {
        field(r.client_id()); number(r.request_id()); field(r.timestamp());
        field(r.operation()); field(r.transaction().from()); field(r.transaction().to());
        number(static_cast<uint64_t>(static_cast<int64_t>(r.transaction().amount())));
        field(r.signature());
    }
    return computeSHA256(bytes);
}

std::string signedHeaderBytes(const ProtocolEnvelope& envelope) {
    ProtocolEnvelope header = envelope;
    header.clear_signature();
    if (header.has_pre_prepare()) header.mutable_pre_prepare()->clear_requests();
    return header.SerializeAsString();
}

ProtocolEnvelope strippedProposal(const ProtocolEnvelope& envelope) {
    ProtocolEnvelope header = envelope;
    if (header.has_pre_prepare()) header.mutable_pre_prepare()->clear_requests();
    return header;
}

const std::string& emptyBatchDigest() {
    static const std::string digest = requestDigest(PrePrepare{});
    return digest;
}

bool bodyMatchesDigest(const ProtocolEnvelope& envelope) {
    return envelope.has_pre_prepare() && envelope.digest() == requestDigest(envelope.pre_prepare());
}

bool carriesBody(const ProtocolEnvelope& envelope) {
    if (!envelope.has_pre_prepare()) return false;
    if (envelope.pre_prepare().requests_size() > 0) return true;
    return envelope.digest() == emptyBatchDigest();
}

ProtocolEnvelope voteEnvelope(const std::string& phase, int view, int seq, int sender,
                              const std::string& digest) {
    ProtocolEnvelope env;
    env.set_digest(digest);
    if (phase == "prepare") {
        auto* m = env.mutable_prepare();
        m->set_type(phase); m->set_view(view); m->set_sequence(seq); m->set_message_sender_id(sender);
    } else {
        auto* m = env.mutable_commit();
        m->set_type(phase); m->set_view(view); m->set_sequence(seq); m->set_message_sender_id(sender);
    }
    return env;
}

ProtocolEnvelope certificateVote(const AggregatedMessage& share, const std::string& phase) {
    auto env = voteEnvelope(phase, share.view(), share.sequence(), share.message_sender_id(), share.digest());
    env.set_signature(share.signature());
    return env;
}

void appendCertificateVote(Commit& certificate, const ProtocolEnvelope& vote) {
    auto* share = certificate.add_combined_messages();
    const bool prepare = vote.has_prepare();
    share->set_view(prepare ? vote.prepare().view() : vote.commit().view());
    share->set_sequence(prepare ? vote.prepare().sequence() : vote.commit().sequence());
    share->set_message_sender_id(prepare ? vote.prepare().message_sender_id() : vote.commit().message_sender_id());
    share->set_digest(vote.digest());
    share->set_signature(vote.signature());
}
} // namespace bedrock
