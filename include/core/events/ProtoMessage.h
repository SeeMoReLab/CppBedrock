#pragma once
#include <string>
#include "proto/bedrock.pb.h"
#include "../../../include/core/events/Message.h"
#include <nlohmann/json.hpp>

class ProtoMessage : public Message {
public:
    explicit ProtoMessage(const bedrock::ProtocolEnvelope& env)
        : Message(""), envelope_(env) {}

    static std::string toJsonString(const bedrock::ProtocolEnvelope& env) {
        nlohmann::json j;
        if (env.has_pre_prepare()) {
            const auto& m = env.pre_prepare();
            j = {
                {"type","PrePrepare"},
                {"view", m.view()},
                {"sequence", m.sequence()},
                {"timestamp", m.timestamp()},
                {"operation", m.operation()},
                {"transaction", {
                    {"from", m.transaction().from()},
                    {"to", m.transaction().to()},
                    {"amount", m.transaction().amount()}
                }},
                {"client_listen_port", m.client_listen_port()},
                {"signature", m.signature()},
                {"client_id", m.client_id()},
                {"message_sender_id", m.message_sender_id()}
            };
        } else if (env.has_prepare()) {
            const auto& m = env.prepare();
            j = {
                {"type","Prepare"},
                {"view", m.view()},
                {"sequence", m.sequence()},
                {"operation", m.operation()},
                {"message_sender_id", m.message_sender_id()}
            };
        } else if (env.has_commit()) {
            const auto& m = env.commit();
            j = {
                {"type","Commit"},
                {"view", m.view()},
                {"sequence", m.sequence()},
                {"operation", m.operation()},
                {"message_sender_id", m.message_sender_id()}
            };
        } else {
            return "{}";
        }
        return j.dump();
    }

    std::string phase() const {
        if (envelope_.has_pre_prepare()) return "PrePrepare";
        if (envelope_.has_prepare())     return "Prepare";
        if (envelope_.has_commit())      return "Commit";
        return "";
    }
    int view() const {
        if (envelope_.has_pre_prepare()) return envelope_.pre_prepare().view();
        if (envelope_.has_prepare())     return envelope_.prepare().view();
        if (envelope_.has_commit())      return envelope_.commit().view();
        return -1;
    }
    int sequence() const {
        if (envelope_.has_pre_prepare()) return envelope_.pre_prepare().sequence();
        if (envelope_.has_prepare())     return envelope_.prepare().sequence();
        if (envelope_.has_commit())      return envelope_.commit().sequence();
        return -1;
    }
    std::string operation() const {
        if (envelope_.has_pre_prepare()) return envelope_.pre_prepare().operation();
        if (envelope_.has_prepare())     return envelope_.prepare().operation();
        if (envelope_.has_commit())      return envelope_.commit().operation();
        return "";
    }
    int sender_id() const {
        if (envelope_.has_pre_prepare()) return envelope_.pre_prepare().message_sender_id();
        if (envelope_.has_prepare())     return envelope_.prepare().message_sender_id();
        if (envelope_.has_commit())      return envelope_.commit().message_sender_id();
        return -1;
    }

    // PrePrepare-only
    int client_listen_port() const {
        if (envelope_.has_pre_prepare()) return envelope_.pre_prepare().client_listen_port();
        return -1;
    }
    std::string timestamp() const {
        if (envelope_.has_pre_prepare()) return envelope_.pre_prepare().timestamp();
        return "";
    }
    bool has_tx() const { return envelope_.has_pre_prepare() && envelope_.pre_prepare().has_transaction(); }
    std::string tx_from() const { return has_tx() ? envelope_.pre_prepare().transaction().from() : ""; }
    std::string tx_to() const   { return has_tx() ? envelope_.pre_prepare().transaction().to()   : ""; }
    int tx_amount() const       { return has_tx() ? envelope_.pre_prepare().transaction().amount(): 0; }
    std::string signature() const { return envelope_.has_pre_prepare() ? envelope_.pre_prepare().signature() : ""; }
    std::string client_id() const { return envelope_.has_pre_prepare() ? envelope_.pre_prepare().client_id() : ""; }

    // NEW: combined messages accessors
    bool hasPrepareCombined() const {
        return envelope_.has_prepare() && envelope_.prepare().combined_messages_size() > 0;
    }
    const ::google::protobuf::RepeatedPtrField<bedrock::AggregatedMessage>& prepareCombined() const {
        return envelope_.prepare().combined_messages();
    }
    bool hasCommitCombined() const {
        return envelope_.has_commit() && envelope_.commit().combined_messages_size() > 0;
    }
    const ::google::protobuf::RepeatedPtrField<bedrock::AggregatedMessage>& commitCombined() const {
        return envelope_.commit().combined_messages();
    }
    std::string qc() const {
        if (envelope_.has_pre_prepare()) return envelope_.pre_prepare().qc();
        if (envelope_.has_prepare())     return envelope_.prepare().qc();
        if (envelope_.has_commit())      return envelope_.commit().qc();
        return "";
    }

    bool hasBatchRequests() const {
        return envelope_.has_pre_prepare() && envelope_.pre_prepare().batch_requests_size() > 0;
    }
    const ::google::protobuf::RepeatedPtrField<bedrock::AggregatedMessage>& batchRequests() const {
        return envelope_.pre_prepare().batch_requests();
    }

    const bedrock::ProtocolEnvelope& envelope() const { return envelope_; }

    bool execute(Entity*, const Message*, EntityState*) override { return true; }

    std::string explicit_type() const {
        if (envelope_.has_pre_prepare()) return envelope_.pre_prepare().type();
        if (envelope_.has_prepare())     return envelope_.prepare().type();
        if (envelope_.has_commit())      return envelope_.commit().type();
        return "";
    }

    std::string getContent() const override {
        if (cachedJson_.empty()) {
            nlohmann::json j;
            j["phase"] = phase();
            // Prefer proto “type” if set; else fallback to phase()
            {
                auto t = explicit_type();
                j["type"] = t.empty() ? phase() : t;
            }
            j["view"] = view();
            j["sequence"] = sequence();
            j["operation"] = operation();
            j["message_sender_id"] = sender_id();
            j["timestamp"] = timestamp();
            if (has_tx()) {
                j["transaction"] = {
                    {"from", tx_from()},
                    {"to", tx_to()},
                    {"amount", tx_amount()}
                };
            } else {
                j["transaction"] = nlohmann::json::object(); // avoid null -> type_error.302
            }
            j["qc"] = qc(); // empty string if none

            // Combined messages (prepare/commit fast path)
            if (hasPrepareCombined()) {
                nlohmann::json arr = nlohmann::json::array();
                for (const auto& am : prepareCombined()) {
                    nlohmann::json fm;
                    fm["view"] = am.view();
                    fm["sequence"] = am.sequence();
                    fm["message_sender_id"] = am.message_sender_id();
                    fm["client_listen_port"] = am.client_listen_port();
                    fm["timestamp"] = am.timestamp();
                    if (am.has_transaction()) {
                        fm["transaction"] = {
                            {"from", am.transaction().from()},
                            {"to", am.transaction().to()},
                            {"amount", am.transaction().amount()}
                        };
                    }
                    arr.push_back(std::move(fm));
                }
                j["combinedMessages"] = std::move(arr);
            } else if (hasCommitCombined()) {
                nlohmann::json arr = nlohmann::json::array();
                for (const auto& am : commitCombined()) {
                    nlohmann::json fm;
                    fm["view"] = am.view();
                    fm["sequence"] = am.sequence();
                    fm["message_sender_id"] = am.message_sender_id();
                    fm["client_listen_port"] = am.client_listen_port();
                    fm["timestamp"] = am.timestamp();
                    if (am.has_transaction()) {
                        fm["transaction"] = {
                            {"from", am.transaction().from()},
                            {"to", am.transaction().to()},
                            {"amount", am.transaction().amount()}
                        };
                    }
                    arr.push_back(std::move(fm));
                }
                j["combinedMessages"] = std::move(arr);
            }

            cachedJson_ = j.dump();
        }
        return cachedJson_;
    }

private:
    bedrock::ProtocolEnvelope envelope_;
    mutable std::string cachedJson_;
};