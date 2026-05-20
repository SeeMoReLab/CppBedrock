#include "../../../include/core/events/EventFactory.h"
#include "../../../include/core/events/BaseEvent.h"
#include "../../../include/core/Entity.h"
#include "../../../include/core/events/ProtoMessage.h"
#include <nlohmann/json.hpp>
#include <iostream>

int computeQuorumEventFactoryProtocolSpecific(const std::string& quorumStr, int f) {
    if (quorumStr == "2f") return 2 * f;
    if (quorumStr == "2f+1") return (2 * f) + 1;
    try { return std::stoi(quorumStr); } catch (...) { return 1; }
}

// Example uncommon event
class UncommonEvent : public BaseEvent {
public:
    UncommonEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity*, const Message*, EntityState*) override {
        std::cout << "UncommonEvent executed\n";
        return true;
    }
};

// Protocol-specific PBFT condition check event
class VerifyPBFTConditionsEvent : public BaseEvent {
public:
    VerifyPBFTConditionsEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message* message, EntityState* state) override {
        if (auto p = dynamic_cast<const ProtoMessage*>(message)) {
            // Typed fast-path
            if (p->view() != entity->entityInfo["view"].get<int>()) {
                std::cout << "[Node " << entity->getNodeId()
                          << "] View mismatch: expected " << entity->entityInfo["view"]
                          << ", got " << p->view() << "\n";
                return false;
            }
            entity->entityInfo["sequence"] = p->sequence();
            return true;
        }
        auto j = nlohmann::json::parse(message->getContent());
        if(j["view"].get<int>() != entity->entityInfo["view"].get<int>()) {
            std::cout << "[Node " << entity->getNodeId() << "] View mismatch: expected " << entity->entityInfo["view"]<< ", got " << j["view"].get<int>() << "\n";
            return false;
        }
        entity->entityInfo["sequence"] = j["sequence"].get<int>();
        return true;
    }
};

// Event to store NewView message for HotStuff2
class StoreNewViewMessageEvent : public BaseEvent {
public:
    StoreNewViewMessageEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message* message, EntityState* state) override {
        if (auto p = dynamic_cast<const ProtoMessage*>(message)) {
            // Use proto fields: treat p->view() as new_view; sender_id from proto
            int view = p->view();
            int sender = p->sender_id();
            if (view != -1 && sender != -1) {
                entity->newViewHotstuffSenders[view].insert(sender);
                
            } else {
                std::cout << "[Node " << entity->getNodeId() << "] Invalid NewView (proto)\n";
            }
            return true;
        }
        auto j = nlohmann::json::parse(message->getContent());
        int view = j.value("new_view", -1);
        int sender = j.value("message_sender_id", -1);
        if (view != -1 && sender != -1) {
            entity->newViewHotstuffSenders[view].insert(sender);
            // std::cout << "[Node " << entity->getNodeId() << "] Stored NewView sender " << sender << " for view " << view << std::endl;
        } else {
            std::cout << "[Node " << entity->getNodeId() << "] Invalid NewView message: " << j.dump() << std::endl;
        }
        return true;
    }
};

// Event to check if NewView quorum is reached for HotStuff2
class CheckNewViewQuorumEvent : public BaseEvent {
public:
    CheckNewViewQuorumEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message* message, EntityState* state) override {
        if (dynamic_cast<const ProtoMessage*>(message)) {
            int view = entity->entityInfo["view"].get<int>();
            int quorum = computeQuorumEventFactoryProtocolSpecific("2f+1", entity->getF());
            size_t count = entity->newViewHotstuffSenders[view].size();
            std::cout << "[Node " << entity->getNodeId() << "] NewView quorum check for view "
                      << view << ": " << count << " / " << quorum << std::endl;
            return count >= static_cast<size_t>(quorum);
        }
        auto j = nlohmann::json::parse(message->getContent());
        int view = entity->entityInfo["view"].get<int>();
        int quorum = computeQuorumEventFactoryProtocolSpecific("2f+1", entity->getF()); // or use your config/quorum logic

        if (view != -1) {
            size_t count = entity->newViewHotstuffSenders[view].size();
            std::cout << "[Node " << entity->getNodeId() << "] NewView quorum check for view " << view
                      << ": " << count << " / " << quorum << std::endl;
            if (count >= static_cast<size_t>(quorum)) {
                std::cout << "[Node " << entity->getNodeId() << "] NewView quorum reached for view " << view << std::endl;
                // You can trigger next protocol step here if needed
                return true;
            }
        }
        return false;
    }
};

// Event to send NewView message to the next leader
class SendNewViewToNextLeaderEvent : public BaseEvent {
public:
    SendNewViewToNextLeaderEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message*, EntityState*) override {
        int currentView = entity->entityInfo["view"].get<int>();
        currentView += 1;
        entity->entityInfo["view"] = currentView;
        int nextLeader = (currentView + 1) % entity->peerPorts.size();

        // Fast path: proto envelope (PrePrepare used as carrier with type="NewViewforHotstuff")
        bedrock::ProtocolEnvelope env;
        auto* m = env.mutable_pre_prepare();
        m->set_view(currentView);
        m->set_sequence(currentView);                 // reuse view as sequence for routing
        m->set_operation("NewViewforHotstuff");
        m->set_message_sender_id(entity->getNodeId());
        m->set_type("NewViewforHotstuff");            // requires 'type' field in proto

        entity->sendProtocolTo(nextLeader, env);
        // std::cout << "[Node " << entity->getNodeId() << "] Sent NewView (proto) to node " << nextLeader << "\n";
        return true;
    }
};


class SpeculativeCompleteEvent : public BaseEvent {
public:
    SpeculativeCompleteEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message* message, EntityState* state) override {
        if (auto p = dynamic_cast<const ProtoMessage*>(message)) {
            // Typed fast-path
            int seq = p->sequence();
            std::string txnId = p->timestamp();
            // Record speculative-exec latency directly using client timestamp
            {
                long long nowUs = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                auto sep = txnId.find('_');
                if (sep != std::string::npos) {
                    try {
                        long long clientMs = std::stoll(txnId.substr(0, sep));
                        long long latUs = nowUs - clientMs * 1000LL;
                        if (latUs > 0) entity->phaseBench_preprepare.record(latUs);
                    } catch (...) {}
                }
            }
            // Populate commitOperations so markOperationProcessed can compute end-to-end latency
            if (!txnId.empty())
                entity->commitOperations[seq] = txnId;
            if (!txnId.empty() && !entity->hasExecutedSpeculativeTransaction(txnId)) {
                std::string from = p->has_tx() ? p->tx_from() : "";
                std::string to   = p->has_tx() ? p->tx_to()   : "";
                int amount       = p->has_tx() ? p->tx_amount(): 0;
                if (!from.empty() && !to.empty() && amount > 0) {
                    entity->updateSpeculativeBalances(from, to, amount);
                    entity->appendSpeculativeEntry(seq, txnId, from, to, amount);
                    entity->markSpeculativeTransactionExecuted(txnId);
                    std::cout << "[Node " << entity->getNodeId() << "] SPECULATIVE Transaction: "
                              << from << " -> " << to << " : " << amount
                              << " Sequence: " << seq << std::endl;
                }
            }
            entity->markOperationProcessed(seq);
            int clientPort = p->client_listen_port();
            if (clientPort > 0) {
                nlohmann::json response{
                    {"type","Response"},
                    {"view", p->view()},
                    {"sequence", p->sequence()},
                    {"timestamp", p->timestamp()},
                    {"message_sender_id", entity->getNodeId()},
                    {"result","speculative"},
                    {"client_listen_port", clientPort}
                };
                Message BalancesReply(response.dump());
                entity->sendTo(clientPort, BalancesReply);
            }
            return true;
        }
        auto j = nlohmann::json::parse(message->getContent());
        std::string type = j.value("type","");
        if (type == "PrePrepare" || type == "SpeculativePrePrepare") {
            int seq = j.value("sequence",-1);
            if (seq > 0) entity->cachePrePrepare(seq, j);
            int maxSeq = entity->getMaxSpeculativeSeq();
            if (seq > maxSeq + 1) {
                // Gap detected, request missing range
                entity->sendFillHole(maxSeq + 1, seq - 1, false);
                // Do not process this out-of-order message yet
                return true;
            }
        }

        std::string operation = j.value("operation", "");
        int seq = j.value("sequence", -1);

        // Use timestamp as unique transaction ID
        std::string txnId = j.value("timestamp", "");
        // Record speculative-exec latency directly using client timestamp
        {
            long long nowUs = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            auto sep = txnId.find('_');
            if (sep != std::string::npos) {
                try {
                    long long clientMs = std::stoll(txnId.substr(0, sep));
                    long long latUs = nowUs - clientMs * 1000LL;
                    if (latUs > 0) entity->phaseBench_preprepare.record(latUs);
                } catch (...) {}
            }
        }
        if (!txnId.empty() && !entity->hasExecutedSpeculativeTransaction(txnId)) {
            if (j.contains("transaction")) {
                auto tx = j["transaction"];
                std::string from = tx.value("from", "");
                std::string to = tx.value("to", "");
                int amount = tx.value("amount", 0);

                if (!from.empty() && !to.empty() && amount > 0) {
                    entity->updateSpeculativeBalances(from, to, amount);
                    entity->appendSpeculativeEntry(seq, txnId, from, to, amount); // NEW: log it
                    entity->markSpeculativeTransactionExecuted(txnId);
                    std::cout << "[Node " << entity->getNodeId() << "] SPECULATIVE Transaction: "
                              << from << " -> " << to << " : " << amount
                              << " Sequence: " << seq << std::endl;
                }
            }
        }

        entity->markOperationProcessed(seq);

        // Send speculative response to client
        if (j.contains("client_listen_port")) {
            int clientPort = j.value("client_listen_port", -1);
            nlohmann::json response;
            response["type"] = "Response";
            response["view"] = j.value("view", -1);
            response["sequence"] = seq;
            response["timestamp"] = j.value("timestamp", "");
            response["message_sender_id"] = entity->getNodeId();
            response["result"] = "speculative";
            response["clientid"] = j.value("clientid", "");
            response["client_listen_port"] = clientPort;
            Message BalancesReply(response.dump());
            if (clientPort != -1) {
                entity->sendTo(clientPort, BalancesReply);
            }
        }
        return true;
    }
};

// Commit-certificate handler for Zyzzyva slow-path
class CommitCertificateEvent : public BaseEvent {
public:
    CommitCertificateEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message* message, EntityState*) override {
        if (auto p = dynamic_cast<const ProtoMessage*>(message)) {
            int s = p->sequence();
            std::string txnId = p->timestamp();
            if (s < 0 && !txnId.empty()) {
                s = entity->findSeqByTxnId(txnId);
            }
            if (s < 0) {
                std::cout << "[Node " << entity->getNodeId() << "] CommitCertificateEvent(proto): missing sequence, ignoring\n";
                return false;
            }
            // Record commit-cert latency directly using client timestamp
            {
                long long nowUs = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                auto sep = txnId.find('_');
                if (sep != std::string::npos) {
                    try {
                        long long clientMs = std::stoll(txnId.substr(0, sep));
                        long long latUs = nowUs - clientMs * 1000LL;
                        if (latUs > 0) entity->phaseBench_commit.record(latUs);
                    } catch (...) {}
                }
            }
            // Commit all entries with seq ≤ s
            std::vector<int> toErase;
            {
                std::lock_guard<std::mutex> g(entity->speculativeLogMtx);
                for (const auto& [seq, e] : entity->speculativeLog) {
                    if (seq <= s) {
                        entity->updateBalances(e.from, e.to, e.amount);
                        toErase.push_back(seq);
                    }
                }
                for (int seq : toErase) {
                    auto it = entity->speculativeLog.find(seq);
                    if (it != entity->speculativeLog.end()) {
                        entity->executedSpeculativeTransactions.erase(it->second.txnId);
                        entity->speculativeLog.erase(it);
                    }
                }
            }
            entity->committedSeq = std::max(entity->committedSeq, s);
            entity->rebuildSpeculativeBalancesFromLog();
            // Optional client ack (JSON wire)
            int clientPort = p->client_listen_port();
            if (clientPort > 0) {
                nlohmann::json resp{
                    {"type","Response"},
                    {"sequence", s},
                    {"timestamp", txnId},
                    {"message_sender_id", entity->getNodeId()}
                };
                Message ack(resp.dump());
                entity->sendTo(clientPort - 5000, ack);
            }
            std::cout << "[Node " << entity->getNodeId() << "] Committed up to seq " << s << " (slow-path)\n";
            return true;
        }
        auto j = nlohmann::json::parse(message->getContent());
        // Prefer explicit sequence; else derive from txnId (timestamp)
        int s = j.value("sequence", -1);
        std::string txnId = j.value("timestamp", "");
        if (s < 0 && !txnId.empty()) {
            s = entity->findSeqByTxnId(txnId);
        }
        if (s < 0) {
            std::cout << "[Node " << entity->getNodeId() << "] CommitCertificateEvent: missing sequence, ignoring\n";
            return false;
        }

        // Record commit-cert latency directly using client timestamp
        {
            long long nowUs = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            auto sep = txnId.find('_');
            if (sep != std::string::npos) {
                try {
                    long long clientMs = std::stoll(txnId.substr(0, sep));
                    long long latUs = nowUs - clientMs * 1000LL;
                    if (latUs > 0) entity->phaseBench_commit.record(latUs);
                } catch (...) {}
            }
        }

        // 1) Commit all entries with seq ≤ s
        std::vector<int> toErase;
        {
            std::lock_guard<std::mutex> g(entity->speculativeLogMtx);
            for (const auto& [seq, e] : entity->speculativeLog) {
                if (seq <= s) {
                    entity->updateBalances(e.from, e.to, e.amount);        // durable
                    toErase.push_back(seq);
                }
            }
            for (int seq : toErase) {
                auto it = entity->speculativeLog.find(seq);
                if (it != entity->speculativeLog.end()) {
                    entity->executedSpeculativeTransactions.erase(it->second.txnId);
                    entity->speculativeLog.erase(it);
                }
            }
        }
        entity->committedSeq = std::max(entity->committedSeq, s);

        // 2) Rebuild speculative balances from remaining suffix
        entity->rebuildSpeculativeBalancesFromLog();

        // Optional: ack client
        if (j.contains("client_listen_port")) {
            int clientPort = j.value("client_listen_port", -1);
            if (clientPort != -1) {
                nlohmann::json resp{
                    {"type","Response"},
                    {"sequence", s},
                    {"timestamp", txnId},
                    {"message_sender_id", entity->getNodeId()}
                };
                Message ack(resp.dump());
                entity->sendTo(clientPort - 5000, ack);
            }
        }
        std::cout << "[Node " << entity->getNodeId() << "] Committed up to seq " << s << " (slow-path)\n";
        return true;
    }
};

class ZyzzyvaViewChangeEvent : public BaseEvent {
public:
    ZyzzyvaViewChangeEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message* message, EntityState*) override {
        auto j = nlohmann::json::parse(message->getContent());
        if (j.value("type", "") != "ViewChange") return true;

        int view = j.value("view", j.value("new_view", 0));
        int sender = j.value("message_sender_id", -1);
        // de-dupe sender per view
        auto& arr = entity->viewChangeMessages[view];
        bool seen = false;
        for (const auto& m : arr) if (m.value("message_sender_id", -2) == sender) { seen = true; break; }
        if (!seen) arr.push_back(j);

        // if I'm new leader and have 2f+1 reports, compute safe log per Zyzzyva rules
        int n = static_cast<int>(entity->peerPorts.size());
        if (n == 0) return true;
        int leaderId = entity->peerPorts[view % n];
        if (entity->getNodeId() != leaderId) return true;

        int f = entity->f;
        if (static_cast<int>(arr.size()) < 2 * f + 1) return true;

        // Step 1: safe committed prefix
        int safeCommitted = entity->committedSeq;
        for (const auto& r : arr) safeCommitted = std::max(safeCommitted, r.value("committed_seq", entity->committedSeq));

        // Step 2: longest safe suffix: for each seq > safeCommitted, pick value with ≥ f+1 support
        // support[seq][key] = count; keep a representative message for reconstruction
        std::map<int, std::map<std::string, int>> support;
        std::map<int, std::map<std::string, nlohmann::json>> rep;

        for (const auto& r : arr) {
            if (!r.contains("prepare_messages") || !r["prepare_messages"].is_array()) continue;
            for (const auto& pm : r["prepare_messages"]) {
                if (!pm.contains("sequence") || !pm["sequence"].is_number_integer()) continue;
                int seq = pm["sequence"].get<int>();
                if (seq <= safeCommitted) continue;
                std::string ts = pm.value("timestamp", "");
                // key by timestamp + transaction dump to ensure identical requests
                std::string key = ts + "|" + pm.value("transaction", nlohmann::json{}).dump();
                support[seq][key] += 1;
                if (!rep[seq].count(key)) rep[seq][key] = pm;
            }
        }

        // Build selected_log: consecutive from safeCommitted+1 while some key has ≥ f+1 support
        nlohmann::json selectedLog = nlohmann::json::array();
        for (int seq = safeCommitted + 1;; ++seq) {
            if (!support.count(seq)) break;
            const auto& counts = support[seq];
            int best = 0; std::string bestKey;
            for (const auto& [k, c] : counts) if (c > best) { best = c; bestKey = k; }
            if (best >= f + 1) {
                const auto& pm = rep[seq][bestKey];
                nlohmann::json entry;
                entry["sequence"] = seq;
                entry["timestamp"] = pm.value("timestamp", "");
                if (pm.contains("transaction")) entry["transaction"] = pm["transaction"];
                entry["operation"] = pm.value("operation", "");
                selectedLog.push_back(entry);
            } else {
                break; // stop at first gap without ≥ f+1 support
            }
        }

        // Broadcast NewView with committed_seq and selected_log
        nlohmann::json nv{
            {"type","NewView"},
            {"view", view},
            {"message_sender_id", entity->getNodeId()},
            {"committed_seq", safeCommitted},
            {"selected_log", selectedLog}
        };
        Message out(nv.dump());
        entity->sendToAll(out);
        std::cout << "[Node " << entity->getNodeId() << "] NewView(view=" << view
                  << ") committed_seq=" << safeCommitted
                  << " selected_log_len=" << selectedLog.size() << std::endl;
        return true;
    }
};

class ZyzzyvaHandleNewViewEvent : public BaseEvent {
public:
    ZyzzyvaHandleNewViewEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message* message, EntityState*) override {
        auto j = nlohmann::json::parse(message->getContent());
        if (j.value("type", "") != "NewView") return true;

        int view = j.value("view", entity->entityInfo.value("view", 0));
        int s = j.value("committed_seq", entity->committedSeq);
        entity->entityInfo["view"] = view;
        entity->inViewChange = false;

        // 1) Commit prefix ≤ s
        std::vector<int> toErase;
        {
            std::lock_guard<std::mutex> g(entity->speculativeLogMtx);
            for (const auto& [seq, e] : entity->speculativeLog) {
                if (seq <= s) {
                    entity->updateBalances(e.from, e.to, e.amount);
                    toErase.push_back(seq);
                }
            }
            for (int seq : toErase) {
                auto it = entity->speculativeLog.find(seq);
                if (it != entity->speculativeLog.end()) {
                    entity->executedSpeculativeTransactions.erase(it->second.txnId);
                    entity->speculativeLog.erase(it);
                }
            }
        }
        entity->committedSeq = std::max(entity->committedSeq, s);

        // 2) Adopt leader's selected_log for sequences > s (truncate conflicts)
        if (j.contains("selected_log") && j["selected_log"].is_array()) {
            // Truncate all speculative entries > s
            {
                std::lock_guard<std::mutex> g(entity->speculativeLogMtx);
                std::vector<int> rm;
                for (const auto& [seq, _] : entity->speculativeLog) if (seq > s) rm.push_back(seq);
                for (int seq : rm) entity->speculativeLog.erase(seq);
            }
            // Install selected entries
            for (const auto& e : j["selected_log"]) {
                int seq = e.value("sequence", -1);
                if (seq <= s || seq < 0) continue;
                std::string txnId = e.value("timestamp", "");
                std::string from = e.value("transaction", nlohmann::json{}).value("from", "");
                std::string to = e.value("transaction", nlohmann::json{}).value("to", "");
                int amount = e.value("transaction", nlohmann::json{}).value("amount", 0);
                std::lock_guard<std::mutex> g(entity->speculativeLogMtx);
                entity->speculativeLog[seq] = Entity::SpeculativeEntry{seq, txnId, from, to, amount};
                entity->executedSpeculativeTransactions.insert(txnId);
            }
        }

        // 3) Rebuild speculative balances for suffix > committedSeq
        entity->rebuildSpeculativeBalancesFromLog();

        std::cout << "[Node " << entity->getNodeId() << "] Installed NewView " << view
                  << ", committed up to " << s << ", speculative suffix rebuilt\n";
        return true;
    }
};

class FillHoleRequestEvent : public BaseEvent {
public:
    FillHoleRequestEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message* message, EntityState*) override {
        // NOTE: remains JSON-only until from_seq/to_seq are added to your proto
        auto j = nlohmann::json::parse(message->getContent());
        if (j.value("type","") != "FillHole") return true;
        int view = j.value("view", -1);
        if (view != entity->entityInfo["view"].get<int>()) return true; // ignore other views
        int fromSeq = j.value("from_seq", -1);
        int toSeq = j.value("to_seq", -1);
        int requester = j.value("message_sender_id", -1);

        // If we have cached entries, replay them to requester
        entity->replayRangeTo(fromSeq, toSeq, requester);
        return true;
    }
};

// Registration function for uncommon events
void registerUncommonEvents(EventFactory& factory) {
    factory.registerEvent<UncommonEvent>("uncommonEvent");
    factory.registerEvent<VerifyPBFTConditionsEvent>("verifyPBFTConditions");
    factory.registerEvent<StoreNewViewMessageEvent>("storeNewViewMessage");
    factory.registerEvent<CheckNewViewQuorumEvent>("checkNewViewQuorum");
    factory.registerEvent<SendNewViewToNextLeaderEvent>("sendNewViewToNextLeader");
    factory.registerEvent<SpeculativeCompleteEvent>("speculativeComplete");
    factory.registerEvent<CommitCertificateEvent>("commitCertificate"); // NEW
}

// Static registrars to ensure event names are available
namespace {
struct ZyzzyvaVCRegistrar {
    ZyzzyvaVCRegistrar() {
        EventFactory::getInstance().registerEvent<ZyzzyvaViewChangeEvent>("zyzzyvaViewChange");
        EventFactory::getInstance().registerEvent<ZyzzyvaHandleNewViewEvent>("zyzzyvaHandleNewView");
    }
} zyzzyva_vc_registrar;

struct FillHoleRegistrar {
    FillHoleRegistrar() {
        EventFactory::getInstance().registerEvent<FillHoleRequestEvent>("fillHoleRequest");
    }
} fill_hole_registrar;
}

// Explicit template instantiation
template void EventFactory::registerEvent<UncommonEvent>(const std::string&);
template void EventFactory::registerEvent<VerifyPBFTConditionsEvent>(const std::string&);
template void EventFactory::registerEvent<StoreNewViewMessageEvent>(const std::string&);
template void EventFactory::registerEvent<CheckNewViewQuorumEvent>(const std::string&);
template void EventFactory::registerEvent<SendNewViewToNextLeaderEvent>(const std::string&);
template void EventFactory::registerEvent<SpeculativeCompleteEvent>(const std::string&);
template void EventFactory::registerEvent<CommitCertificateEvent>(const std::string&); // NEW
template void EventFactory::registerEvent<ZyzzyvaViewChangeEvent>(const std::string&);
template void EventFactory::registerEvent<ZyzzyvaHandleNewViewEvent>(const std::string&);
template void EventFactory::registerEvent<FillHoleRequestEvent>(const std::string&);