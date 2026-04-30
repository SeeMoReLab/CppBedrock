#include "../../../include/core/events/EventFactory.h"
#include "../../../include/core/events/BaseEvent.h"
#include "../../../include/core/Entity.h"
#include <nlohmann/json.hpp>
#include <iostream>
#include <arpa/inet.h>
#include <unistd.h>
#include "../../../include/core/events/ProtoMessage.h"

void registerUncommonEvents(EventFactory& factory);

int computeQuorumEventFactory(const std::string& quorumStr, int f) {
    if (quorumStr == "2f") return 2 * f;
    if (quorumStr == "2f+1") return (2 * f) + 1;
    try { return std::stoi(quorumStr); } catch (...) { return 1; }
}

// Example derived event class for incrementing sequence
class IncrementSequenceEvent : public BaseEvent {
public:
    IncrementSequenceEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity*, const Message*, EntityState* state) override {
        state->incrementSequenceNumber();
        std::cout << "Sequence incremented" << std::endl;
        return true;
    }
};

class AddLogEvent : public BaseEvent {
public:
    AddLogEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity*, const Message*, EntityState*) override {
        std::cout << "Log entry added" << std::endl;
        return true;
    }
};

class UpdateLogEvent : public BaseEvent {
public:
    UpdateLogEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity*, const Message*, EntityState*) override {
        std::cout << "Log entry updated" << std::endl;
        return true;
    }
};

class SendToClientEvent : public BaseEvent {
public:
    SendToClientEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity*, const Message*, EntityState*) override {
        std::cout << "Sent to client" << std::endl;
        return true;
    }
};


// Protocol Events
class StoreMessageEvent : public BaseEvent {
public:
    StoreMessageEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}

    // Small helper for extracting sender id (avoids repeated branching)
    static int extractSenderId(const nlohmann::json& obj) {
        if (!obj.contains("message_sender_id")) return -1;
        const auto& v = obj["message_sender_id"];
        if (v.is_number_integer()) return v.get<int>();
        if (v.is_string()) {
            try { return std::stoi(v.get<std::string>()); } catch (...) { return -1; }
        }
        return -1;
    }

    bool execute(Entity* entity, const Message* message, EntityState* state) override {
        // -------- Proto fast-path --------
        if (auto p = dynamic_cast<const ProtoMessage*>(message)) {
            const std::string phase = p->explicit_type();
            const int seq = p->sequence();
            const int senderId = p->sender_id();

            // Update global sequence if needed
            if (seq > entity->entityInfo["sequence"].get<int>()) {
                entity->entityInfo["sequence"] = seq;
            }

            const std::string aggKey = phase + "_" + std::to_string(seq);
            {
                std::lock_guard<std::mutex> lk(entity->senderIdsMtx);
                entity->keyToSenderIds[aggKey].insert(senderId);
            }
            //std::cout << "[Node " << entity->getNodeId() << "] Stored sender " << senderId << " for key " << aggKey << std::endl;

            // Ignore commits after completion
            YAML::Node phaseConfig = entity->getPhaseConfigInsensitive(phase);
            if (phaseConfig["next_state"].as<std::string>()=="Request" && phase == "Commit") {
                std::lock_guard<std::mutex> pg(entity->processedMtx);
                if (entity->processedOperations.count(seq)) return true;
            }

            // Index PrePrepare minimal info
            if (phase == "PrePrepare") {
                {
                    long long nowUs = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::system_clock::now().time_since_epoch()).count();
                    std::lock_guard<std::mutex> lk(entity->phaseTsMtx);
                    entity->phaseTs_preprepare.emplace(seq, nowUs);
                }
                if (p->has_tx()) {
                Entity::PrePrepareInfo info;
                info.timestamp   = p->timestamp();
                info.operation   = p->operation();
                info.client_port = p->client_listen_port();
                info.from   = p->tx_from();
                info.to     = p->tx_to();
                info.amount = p->tx_amount();
                if (p->hasBatchRequests()) {
                    for (const auto& br : p->batchRequests()) {
                        Entity::BatchItem bi;
                        bi.timestamp   = br.timestamp();
                        bi.client_port = br.client_listen_port();
                        if (br.has_transaction()) {
                            bi.from   = br.transaction().from();
                            bi.to     = br.transaction().to();
                            bi.amount = br.transaction().amount();
                        }
                        info.batch.push_back(std::move(bi));
                    }
                }
                {
                    std::lock_guard<std::mutex> lk(entity->prePrepareMtx);
                    entity->prePrepareIndex[seq] = std::move(info);
                }

                // Persist lightweight record
                nlohmann::json toStore{
                    {"type","PrePrepare"},
                    {"sequence",seq},
                    {"message_sender_id",senderId},
                    {"view",p->view()},
                    {"operation",p->operation()},
                    {"timestamp",p->timestamp()},
                    {"client_listen_port",p->client_listen_port()}
                };
                if (p->has_tx()) {
                    toStore["transaction"] = {
                        {"from", p->tx_from()},
                        {"to", p->tx_to()},
                        {"amount", p->tx_amount()}
                    };
                }
                entity->dataset.update(
                    "PrePrepare_" + std::to_string(seq) + "_" + std::to_string(senderId),
                    toStore);
                }
            }

            // Combined senders directly from protobuf (no JSON parse needed)
            if (phase == "prepare" && p->hasPrepareCombined()) {
                std::lock_guard<std::mutex> lk(entity->senderIdsMtx);
                auto& setRef = entity->keyToSenderIds[aggKey];
                for (const auto& am : p->prepareCombined()) {
                    setRef.insert(am.message_sender_id());
                }
            } else if (phase == "commit" && p->hasCommitCombined()) {
                std::lock_guard<std::mutex> lk(entity->senderIdsMtx);
                auto& setRef = entity->keyToSenderIds[aggKey];
                for (const auto& am : p->commitCombined()) {
                    setRef.insert(am.message_sender_id());
                }
            }

            // If ProtoMessage content also has combinedMessages JSON (from getContent()), parse once
            // (Only if we need to merge any extra senders not in protobuf combined sections)
            if ((phase == "prepare" || phase == "commit")) {
                const std::string payload = message->getContent();
                if (payload.size() > 2 && payload.front() == '{') {
                    auto j = nlohmann::json::parse(payload, nullptr, false);
                    if (!j.is_discarded() && j.contains("combinedMessages") && j["combinedMessages"].is_array()) {
                        std::lock_guard<std::mutex> lk(entity->senderIdsMtx);
                        auto& setRef = entity->keyToSenderIds[aggKey];
                        for (const auto& m : j["combinedMessages"]) {
                            int sid = extractSenderId(m);
                            if (sid != -1) setRef.insert(sid);
                        }
                    }
                }
            }

            return true;
        }

        // -------- JSON path (participants / legacy senders) --------
        try {
            auto j = nlohmann::json::parse(message->getContent());
            const std::string phase = j.value("type", "");
            const int seq = j.value("sequence", -1);
            if (phase.empty() || seq < 0) return true;

            const std::string aggKey = phase + "_" + std::to_string(seq);

            // Lock once for all insertions
            {
                std::lock_guard<std::mutex> lk(entity->senderIdsMtx);
                auto& setRef = entity->keyToSenderIds[aggKey];

                int primary = extractSenderId(j);
                if (primary != -1) setRef.insert(primary);

                const auto& cm = j.find("combinedMessages");
                if (cm != j.end() && cm->is_array()) {
                    for (const auto& m : *cm) {
                        int sid = extractSenderId(m);
                        if (sid != -1) setRef.insert(sid);
                    }
                }
            }

            // Lightweight PrePrepare indexing for JSON-origin PrePrepare
            if (phase == "PrePrepare") {
                Entity::PrePrepareInfo info;
                info.timestamp   = j.value("timestamp", std::string());
                info.operation   = j.value("operation", std::string());
                info.client_port = j.value("client_listen_port", -1);
                if (j.contains("transaction") && j["transaction"].is_object()) {
                    const auto& tx = j["transaction"];
                    info.from   = tx.value("from", std::string());
                    info.to     = tx.value("to", std::string());
                    info.amount = tx.value("amount", 0);
                }
                {
                    std::lock_guard<std::mutex> lk(entity->prePrepareMtx);
                    entity->prePrepareIndex[seq] = std::move(info);
                }
                {
                    long long nowUs = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::system_clock::now().time_since_epoch()).count();
                    std::lock_guard<std::mutex> lk(entity->phaseTsMtx);
                    entity->phaseTs_preprepare.emplace(seq, nowUs);
                }
            }
        } catch (...) {
            // Ignore malformed JSON
        }

        return true;
    }
};

class ManageTimerEvent : public BaseEvent {
public:
    ManageTimerEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message* message, EntityState* state) override {
        if (auto p = dynamic_cast<const ProtoMessage*>(message)) {
            std::lock_guard<std::mutex> lk(entity->timerMtx);
            if (p->explicit_type() == "PrePrepare") {
                if (entity->timeKeeper) entity->timeKeeper->start();
            } else {
                if (entity->timeKeeper) entity->timeKeeper->reset();
            }
            return true;
        }
        auto j = nlohmann::json::parse(message->getContent());
        std::string currentPhase = j["type"];
        {
            std::lock_guard<std::mutex> lk(entity->timerMtx);
            if (currentPhase == "PrePrepare") {
                if (entity->timeKeeper) entity->timeKeeper->start();
            } else {
                if (entity->timeKeeper) entity->timeKeeper->reset();
            }
        }
        return true;
    }
};

class StartTimerEvent : public BaseEvent {
public:
    StartTimerEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message* message, EntityState* state) override {
        //std::cout << "[Node " << entity->getNodeId() << "] Starting timer" << std::endl;
        {
            std::lock_guard<std::mutex> lk(entity->timerMtx);
            if (entity->timeKeeper){
                entity->timeKeeper->start();
                // std::cout << "[Node " << entity->getNodeId() << "] Timer started" << std::endl;
            }
        }
        return true;
    }
};

class ResetTimerEvent : public BaseEvent {
public:
    ResetTimerEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message* message, EntityState* state) override {
        //std::cout << "[Node " << entity->getNodeId() << "] Resetting timer" << std::endl;
        {
            std::lock_guard<std::mutex> lk(entity->timerMtx);
            if (entity->timeKeeper) entity->timeKeeper->reset();
        }
        return true;
    }
};

class StopTimerEvent : public BaseEvent {
public:
    StopTimerEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message* message, EntityState* state) override {
        //std::cout << "[Node " << entity->getNodeId() << "] Resetting timer" << std::endl;
        {
            std::lock_guard<std::mutex> lk(entity->timerMtx);
            if (entity->timeKeeper) {
                entity->timeKeeper->stop();
                entity->timeKeeper.reset();
            }
        }
        return true;
    }
};

class CheckQuorumEvent : public BaseEvent {
public:
    CheckQuorumEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message* message, EntityState* state) override {
        int seq = -1;
        std::string currentPhase;
        if (auto p = dynamic_cast<const ProtoMessage*>(message)) {
            seq = p->sequence();
            currentPhase = p->explicit_type();
        } else {
            auto j = nlohmann::json::parse(message->getContent());
            seq = j.value("sequence", -1);
            currentPhase = state ? state->getState() : j.value("type", "");
        }
        if (seq < 0 || currentPhase.empty()) return false;

        YAML::Node phaseConfig = entity->getPhaseConfigInsensitive(currentPhase);
        if (!phaseConfig || !phaseConfig["quorum"]) return false;
        int quorum = computeQuorumEventFactory(phaseConfig["quorum"].as<std::string>(), entity->getF());

        
        const std::string aggKey = currentPhase + "_" + std::to_string(seq);
        size_t votes = 0;
        {
            std::lock_guard<std::mutex> lk(entity->senderIdsMtx);
            auto it = entity->keyToSenderIds.find(aggKey);
            if (it != entity->keyToSenderIds.end()) votes = it->second.size();
        }
        //std::cout << "[Node " << entity->getNodeId() << "] Quorum check for " << aggKey << ": " << votes << " votes, quorum is " << quorum << std::endl;

        if (static_cast<int>(votes) >= quorum) {
            // Atomically check-and-set: only the first thread to reach quorum proceeds
            std::lock_guard<std::mutex> lk(entity->quorumTriggeredMtx);
            if (entity->quorumTriggered.count(aggKey)) {
                return false; // Already triggered for this phase+seq
            }
            entity->quorumTriggered.insert(aggKey);
            return true;
        }
        return false;
    }
};

class CheckQuorumEventForSBFT : public BaseEvent {
public:
    CheckQuorumEventForSBFT(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message* message, EntityState* state) override {
        
        // -------- Proto fast-path --------
        if (auto p = dynamic_cast<const ProtoMessage*>(message)) {
            const int seq = p->sequence();
            const std::string phase = p->explicit_type(); // "Prepare" / "Commit" / etc.
            
            if (seq < 0 || phase.empty()) return false;
            

            YAML::Node phaseConfig = entity->getPhaseConfigInsensitive(phase);
            if (!phaseConfig || !phaseConfig["quorum"]) return false;

            int quorum = computeQuorumEventFactory(phaseConfig["quorum"].as<std::string>(), entity->getF());
            
            const std::string key = phase + "_" + std::to_string(seq);
            size_t uniqueSendersSize = 0;
            {
                std::lock_guard<std::mutex> lk(entity->senderIdsMtx);
                auto it = entity->keyToSenderIds.find(key);
                if (it != entity->keyToSenderIds.end()) uniqueSendersSize = it->second.size();
            }

            
            bool quorumMet = uniqueSendersSize >= static_cast<size_t>(quorum);
            std::cout << "[Node " << entity->getNodeId() << "] CheckQuorumEventForSBFT for phase " << phase << " seq " << seq
                      << ": " << uniqueSendersSize-1 << " unique senders (excluding self), quorum is " << quorum
                      << ". Quorum met: " << (quorumMet ? "YES" : "NO") << std::endl;
            if (!quorumMet) return false;
            {
                long long nowUs = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                std::lock_guard<std::mutex> lk(entity->phaseTsMtx);
                std::string phaseLowerTmp = phase;
                for (auto& c : phaseLowerTmp) c = (char)std::tolower(c);
                if (phaseLowerTmp == "prepare")
                    entity->phaseTs_prepare.emplace(seq, nowUs);
                else if (phaseLowerTmp == "commit")
                    entity->phaseTs_commit.emplace(seq, nowUs);
            }
            // Timer logic for prepare phase (case-insensitive match to original "prepare")
            std::string phaseLower = phase;
            for (auto& c : phaseLower) c = (char)std::tolower(c);
            std::cout << "[Node " << entity->getNodeId() << "] Quorum met for " << key << " with " << uniqueSendersSize << " unique senders. Timer logic check for phase: " << phaseLower << " " << (entity->entityInfo["view"].get<int>() + 1) % ((entity->getF()*3)+1) << std::endl;
            
            if (phaseLower == "prepare" && (entity->entityInfo["view"].get<int>() + 1) % ((entity->getF()*3)+1) == entity->getNodeId()) {
               // std::cout << "[Node " << entity->getNodeId() << "] Quorum met for Prepare phase of seq " << seq << " with " << uniqueSendersSize << " unique senders. Checking timer logic..." << std::endl;
                if (!entity->preparePhaseTimerRunning[seq].exchange(true) && uniqueSendersSize < (entity->getF() * 3)) {
                    auto byzState = entity->getActiveByzantineState();
                    int fastPathWait = entity->fastPathWaitMs + byzState.fastPathExtraDelayMs;
                    bool skipFastPath = byzState.skipFastPath;
                    if (byzState.fastPathExtraDelayMs > 0)
                        std::cout << "[Node " << entity->getNodeId()
                                  << "] Byzantine delay_fast_path +" << byzState.fastPathExtraDelayMs
                                  << "ms seq=" << seq << " (total wait=" << fastPathWait << "ms)\n";
                    std::thread([entity, seq, phaseConfig, phase, state, fastPathWait, skipFastPath]() {
                        auto t0 = std::chrono::steady_clock::now();
                        std::this_thread::sleep_for(std::chrono::milliseconds(fastPathWait));
                        auto actualMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - t0).count();
                        std::cout << "[Node " << entity->getNodeId()
                                  << "] fast_path_timer done seq=" << seq
                                  << " slept=" << actualMs << "ms\n";
                        entity->preparePhaseTimerRunning[seq] = false;

                        // Decide next state (only advance if exactly 3f+1 senders and not skipping fast path)
                        std::string aggKey = phase + "_" + std::to_string(seq);
                        size_t currentCount = 0;
                        {
                            std::lock_guard<std::mutex> lk(entity->senderIdsMtx);
                            auto it = entity->keyToSenderIds.find(aggKey);
                            if (it != entity->keyToSenderIds.end()) currentCount = it->second.size();
                        }

                        YAML::Node nextPhaseConfig = entity->getPhaseConfigInsensitive(phase);
                        std::string nextState;
                        const bool wouldTakeFastPath = (currentCount == (entity->getF() * 3) && nextPhaseConfig && nextPhaseConfig["next_state"]);
                        if (!skipFastPath && wouldTakeFastPath) {
                            nextState = nextPhaseConfig["next_state"].as<std::string>();
                            entity->sequenceStates[seq].setState(nextState);
                        } else {
                            if (skipFastPath && wouldTakeFastPath)
                                std::cout << "[Node " << entity->getNodeId()
                                          << "] Byzantine skip_fast_path seq=" << seq << "\n";
                            nextState = phase; // stay (slow path)
                            return;
                        }

                        // Build combinedMessages from in-memory sender IDs (fast path)
                        nlohmann::json combinedMessages = nlohmann::json::array();
                        std::unordered_set<int> uniqueSendersSnapshot;
                        {
                            std::lock_guard<std::mutex> lk(entity->senderIdsMtx);
                            auto it = entity->keyToSenderIds.find(aggKey);
                            if (it != entity->keyToSenderIds.end()) {
                                for (int sid : it->second) uniqueSendersSnapshot.insert(sid);
                            }
                        }

                        // Try to enrich using stored PrePrepare info (if still present)
                        Entity::PrePrepareInfo storedInfo;
                        {
                            std::lock_guard<std::mutex> lk(entity->prePrepareMtx);
                            auto it = entity->prePrepareIndex.find(seq);
                            if (it != entity->prePrepareIndex.end()) storedInfo = it->second;
                        }

                        for (int sid : uniqueSendersSnapshot) {
                            nlohmann::json filtered;
                            filtered["view"] = entity->getState().getViewNumber();
                            filtered["sequence"] = seq;
                            filtered["digest"] = "";
                            filtered["message_sender_id"] = sid;
                            filtered["signature"] = "";
                            filtered["client_listen_port"] = storedInfo.client_port >= 0 ? storedInfo.client_port : -1;
                            filtered["clientid"] = "";
                            filtered["timestamp"] = storedInfo.timestamp;
                            if (!storedInfo.from.empty() && !storedInfo.to.empty() && storedInfo.amount > 0) {
                                filtered["transaction"] = {
                                    {"from", storedInfo.from},
                                    {"to", storedInfo.to},
                                    {"amount", storedInfo.amount}
                                };
                            } else {
                                filtered["transaction"] = nlohmann::json{};
                            }
                            combinedMessages.push_back(std::move(filtered));
                            
                        }

                        // Execute configured actions, mirroring original JSON path
                        if (nextPhaseConfig && nextPhaseConfig["actions"] && nextPhaseConfig["actions"].IsSequence()) {
                            for (const auto& actionNode : nextPhaseConfig["actions"]) {
                                std::string actionName = actionNode.as<std::string>();
                                auto itAct = entity->actions.find(actionName);
                                if (itAct != entity->actions.end()) {
                                    nlohmann::json outMsg;
                                    outMsg["type"] = nextState;
                                    outMsg["view"] = entity->getState().getViewNumber();
                                    outMsg["sequence"] = seq;
                                    // Operation fallback order as per original comments
                                    if (entity->prepareOperations.count(seq))
                                        outMsg["operation"] = entity->prepareOperations[seq];
                                    else if (entity->prePrepareOperations.count(seq))
                                        outMsg["operation"] = entity->prePrepareOperations[seq];
                                    else
                                        outMsg["operation"] = "";

                                    outMsg["message_sender_id"] = entity->getNodeId();
                                    outMsg["combinedMessages"] = combinedMessages;
                                    outMsg["qc"] = state ? state->getLockedQC() : "";
                                    // Transaction reconstruction
                                    if (!storedInfo.from.empty() && !storedInfo.to.empty() && storedInfo.amount > 0) {
                                        outMsg["transaction"] = {
                                            {"from", storedInfo.from},
                                            {"to", storedInfo.to},
                                            {"amount", storedInfo.amount}
                                        };
                                    } else {
                                        outMsg["transaction"] = nlohmann::json{};
                                    }
                                    outMsg["client_listen_port"] = storedInfo.client_port >= 0 ? storedInfo.client_port : -1;
                                    outMsg["clientid"] = "";
                                    outMsg["timestamp"] = storedInfo.timestamp;

                                    Message protocolMsg(outMsg.dump());
                                    //std::cout << "execting puk quick message " << actionName << "\n";
                                    itAct->second->execute(entity, &protocolMsg, &entity->sequenceStates[seq]);
                                }
                            }
                        }
                    }).detach();
                }
            }

            return uniqueSendersSize >= static_cast<size_t>(quorum);
        }

        // -------- Existing JSON path (unchanged fallback) --------
        if (!validateMessage(message, entity)) return false;
        auto j = nlohmann::json::parse(message->getContent());
        int seq = j["sequence"].get<int>();
        std::string currentPhase = j["type"];
        YAML::Node phaseConfig = entity->getPhaseConfig(currentPhase);
        if (!phaseConfig || !phaseConfig["quorum"]) return false;
        std::string quorumStr = phaseConfig["quorum"].as<std::string>();
        int quorum = computeQuorumEventFactory(quorumStr, entity->getF());
        bool quorumMet = true;

        std::string key2 = currentPhase + "_" + std::to_string(seq);
        int uniqueSendersSize = entity->keyToSenderIds[key2].size();
        quorumMet = uniqueSendersSize >= quorum;
        if (!quorumMet) return false;
        {
            long long nowUs = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            std::lock_guard<std::mutex> lk(entity->phaseTsMtx);
            std::string phaseLowerTmp = currentPhase;
            for (auto& c : phaseLowerTmp) c = (char)std::tolower(c);
            if (phaseLowerTmp == "prepare")
                entity->phaseTs_prepare.emplace(seq, nowUs);
            else if (phaseLowerTmp == "commit")
                entity->phaseTs_commit.emplace(seq, nowUs);
        }

        if (currentPhase == "prepare") {
            if (!entity->preparePhaseTimerRunning[seq].exchange(true) && uniqueSendersSize <= quorum) {
                auto byzState2 = entity->getActiveByzantineState();
                int fastPathWait = entity->fastPathWaitMs + byzState2.fastPathExtraDelayMs;
                bool skipFastPath2 = byzState2.skipFastPath;
                if (byzState2.fastPathExtraDelayMs > 0)
                    std::cout << "[Node " << entity->getNodeId()
                              << "] Byzantine delay_fast_path +" << byzState2.fastPathExtraDelayMs
                              << "ms (json path) seq=" << seq << "\n";
                std::thread([entity, seq, phaseConfig, currentPhase, uniqueSendersSize, state, j, fastPathWait, skipFastPath2]() {
                    std::this_thread::sleep_for(std::chrono::milliseconds(fastPathWait));
                    entity->preparePhaseTimerRunning[seq] = false;

                    YAML::Node nextPhaseConfig = entity->getPhaseConfig(currentPhase);
                    std::string nextState;
                    std::string key = currentPhase + "_" + std::to_string(seq);
                    const bool wouldTakeFastPath2 = (entity->keyToSenderIds[key].size() == 7 && nextPhaseConfig && nextPhaseConfig["next_state"]);
                    if (!skipFastPath2 && wouldTakeFastPath2) {
                        nextState = nextPhaseConfig["next_state"].as<std::string>();
                        entity->sequenceStates[seq].setState(nextState);
                    } else {
                        if (skipFastPath2 && wouldTakeFastPath2)
                            std::cout << "[Node " << entity->getNodeId()
                                      << "] Byzantine skip_fast_path (json path) seq=" << seq << "\n";
                        nextState = currentPhase;
                    }

                    // Build combinedMessages (legacy path)
                    nlohmann::json combinedMessages = nlohmann::json::array();
                    for (int sid : entity->keyToSenderIds[key]) {
                        nlohmann::json filtered;
                        filtered["view"] = entity->getState().getViewNumber();
                        filtered["sequence"] = seq;
                        filtered["digest"] = "";
                        filtered["message_sender_id"] = sid;
                        filtered["signature"] = "";
                        filtered["client_listen_port"] = j.value("client_listen_port",-1);
                        filtered["clientid"] = j.value("clientid","");
                        filtered["timestamp"] = j.value("timestamp","");
                        filtered["transaction"] = j.value("transaction", nlohmann::json{});
                        combinedMessages.push_back(std::move(filtered));
                    }

                    if (nextPhaseConfig && nextPhaseConfig["actions"] && nextPhaseConfig["actions"].IsSequence()) {
                        for (const auto& actionNode : nextPhaseConfig["actions"]) {
                            std::string actionName = actionNode.as<std::string>();
                            auto it = entity->actions.find(actionName);
                            if (it != entity->actions.end()) {
                                nlohmann::json outMsg;
                                outMsg["type"] = nextState;
                                outMsg["view"] = entity->getState().getViewNumber();
                                outMsg["sequence"] = seq;
                                outMsg["operation"] = j.value("operation","");
                                outMsg["message_sender_id"] = entity->getNodeId();
                                outMsg["combinedMessages"] = combinedMessages;
                                outMsg["qc"] = state ? state->getLockedQC() : "";
                                outMsg["transaction"] = j.value("transaction", nlohmann::json{});
                                outMsg["client_listen_port"] = j.value("client_listen_port",-1);
                                outMsg["clientid"] = j.value("clientid","");
                                outMsg["timestamp"] = j.value("timestamp","");
                                Message protocolMsg(outMsg.dump());
                                it->second->execute(entity, &protocolMsg, &entity->sequenceStates[seq]);
                            }
                        }
                    }
                }).detach();
            }
        }

        if (quorumMet && phaseConfig["next_state"]) {
            std::string nextState = phaseConfig["next_state"].as<std::string>();
            (void)nextState; // original code ignored it here
        }
        return quorumMet;
    }
};

class CheckQCEvent : public BaseEvent {
public:
    CheckQCEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message* message, EntityState* state) override {
        // Proto fast-path
        if (auto p = dynamic_cast<const ProtoMessage*>(message)) {
            const std::string qcStr = p->qc(); // QC stored as string (view number)
            if (qcStr.empty()) return true;    // No QC -> initial message
            try {
                int qcView = std::stoi(qcStr);
                int curView = entity->entityInfo["view"].get<int>();
                if (qcView <= curView) return true; // QC not advancing
                return false; // QC indicates higher view
            } catch (...) {
                return true; // Malformed QC -> treat as absent
            }
        }

        // JSON fallback
        auto j = nlohmann::json::parse(message->getContent());
        if (!j.contains("qc") || j["qc"].is_null() || j["qc"].empty() || j["qc"] == "") {
            return true;
        }
        std::string curviewStr;
        try {
            curviewStr = j["qc"].get<std::string>();
        } catch (...) {
            return true;
        }
        try {
            int qcView = std::stoi(curviewStr);
            int curView = entity->entityInfo["view"].get<int>();
            if (qcView <= curView) return true;
            return false;
        } catch (...) {
            return true;
        }
    }
};

class BroadcastEvent : public BaseEvent {
public:
    BroadcastEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message* message, EntityState* state) override {
        if (auto p = dynamic_cast<const ProtoMessage*>(message)) {
            const std::string phase = p->explicit_type();
            YAML::Node phaseCfg = entity->getPhaseConfigInsensitive(phase);
            if (!phaseCfg || !phaseCfg["next_state"]) return true;
            std::string nextPhase = phaseCfg["next_state"].as<std::string>();
            if(entity->isByzantine && (phase == "Prepare" || phase == "prepare")) {
                return false;
            }
            // Build next-phase envelope
            bedrock::ProtocolEnvelope env;
            if (phase == "PrePrepare") {
                auto* m = env.mutable_prepare();
                m->set_view(p->view());
                m->set_type(nextPhase);
                m->set_sequence(p->sequence());
                m->set_operation(p->operation());
                m->set_message_sender_id(entity->getNodeId());
            } else if (phase == "Prepare" || phase == "prepare") {
                auto* m = env.mutable_commit();
                m->set_view(p->view());
                m->set_type(nextPhase);
                m->set_sequence(p->sequence());
                m->set_operation(p->operation());
                m->set_message_sender_id(entity->getNodeId());
            } else {
                return true; // not a basic phase
            }
            //std::cout << "[Node " << entity->getNodeId() << "] Broadcasting " << nextPhase << " for sequence " << p->sequence() << " (ProtoMessage path)\n";
            entity->sendProtocolToAll(env);
            
            return true;
        }

        // Fallback JSON path
        auto j = nlohmann::json::parse(message->getContent());
        // test to trigger view change by making node 1 as faulty by skipping broadcast
        // Uncomment the following lines to simulate a faulty leader
        
        if(entity->isByzantine && j["type"] == "prepare") {
            return false;
        }
        else{
            // std::cout << "[Node " << entity->getNodeId() << "] Broadcasting prepare: Not Byzantine\n";
        }
        int seq = j["sequence"].get<int>();

        YAML::Node phaseConfig = entity->getPhaseConfig(j["type"]);
        if (phaseConfig["next_state"]) {
            std::string nextPhase = phaseConfig["next_state"].as<std::string>();
            j["type"] = nextPhase;
            j["qc"] = state->getLockedQC(); // Include QC if available
            j["message_sender_id"] = entity->getNodeId(); // Add sender ID to the message
            class ProtocolMessage : public Message {
            public:
                ProtocolMessage(const std::string& content) : Message(content) {}
                bool execute(Entity*, const Message*, EntityState*) override { return true; }
            };
            ProtocolMessage protocolMsg(j.dump());
            entity->sendToAll(protocolMsg);
            // std::cout << "[Node " << entity->getNodeId() << "] Broadcasting message for " << nextPhase << " for seq " << seq << "\n";
        }
        return true;
    }
};

class CompleteEvent : public BaseEvent {
public:
    CompleteEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message* message, EntityState* state) override {
        if (auto p = dynamic_cast<const ProtoMessage*>(message)) {

            // print proto message details
            // std::cout << "[Node " << entity->getNodeId() << "] Completing operation for sequence " << p->sequence() << std::endl;
            // std::cout << "ProtoMessage Details - Phase: " << p->explicit_type()
            //           << ", View: " << p->view()
            //           << ", Sequence: " << p->sequence()
            //           << ", Timestamp: " << p->timestamp()
            //           << ", Operation: " << p->operation()
            //           << ", Client Port: " << p->client_listen_port()
            //           << ", From: " << p->tx_from()
            //           << ", To: " << p->tx_to()
            //           << ", Amount: " << p->tx_amount()
            //           << std::endl;
            
            

            const int seq = p->sequence();
            {
                std::lock_guard<std::mutex> pg(entity->processedMtx);
                if (entity->processedOperations.count(seq)) return true;
            }

            // std::cout << "[Node " << entity->getNodeId() << "] Retrieving PrePrepare Info for sequence " << seq << std::endl;
            // O(1) lookup from prePrepareIndex (set by StoreMessageEvent on PrePrepare)
            // Fall through even if not found (e.g. leader) so markOperationProcessed always runs
            Entity::PrePrepareInfo info;
            info.operation   = p->operation();
            info.client_port = p->client_listen_port();
            info.timestamp   = p->timestamp();
            {
                std::lock_guard<std::mutex> lk(entity->prePrepareMtx);
                auto it = entity->prePrepareIndex.find(seq);
                if (it != entity->prePrepareIndex.end()) {
                    const auto& stored = it->second;
                    if (info.timestamp.empty())  info.timestamp   = stored.timestamp;
                    if (info.operation.empty())  info.operation   = stored.operation;
                    if (info.client_port < 0)    info.client_port = stored.client_port;
                    info.from   = stored.from;
                    info.to     = stored.to;
                    info.amount = stored.amount;
                    info.batch  = stored.batch;
                    entity->prePrepareIndex.erase(it);
                }
            }
            // print info
            // std::cout << "Retrieved PrePrepare Info - Timestamp: " << info.timestamp
            //           << ", Operation: " << info.operation
            //           << ", Client Port: " << info.client_port
            //           << ", From: " << info.from
            //           << ", To: " << info.to
            //           << ", Amount: " << info.amount
            //           << std::endl;
            entity->commitOperations[seq] = info.operation;
            entity->commitBatchSizes[seq] = info.batch.empty() ? 1 : (int)info.batch.size();
            entity->markOperationProcessed(seq);

            if (!info.batch.empty()) {
                // Batch case: execute every item and reply to every client
                for (const auto& bi : info.batch) {
                    if (!bi.timestamp.empty() && entity->executedTransactions.count(bi.timestamp) == 0) {
                        if (!bi.from.empty() && !bi.to.empty() && bi.amount > 0) {
                            entity->updateBalances(bi.from, bi.to, bi.amount);
                            entity->executedTransactions.insert(bi.timestamp);
                        }
                    }
                    if (bi.client_port > 0) {
                        nlohmann::json response{
                            {"type","Response"},
                            {"view", entity->getState().getViewNumber()},
                            {"timestamp", bi.timestamp},
                            {"message_sender_id", entity->getNodeId()},
                            {"result","success"}
                        };
                        Message reply(response.dump());
                        entity->sendTo(bi.client_port, reply);
                    }
                }
            } else {
                // Single-request case: existing behavior
                const std::string& txnId = info.timestamp;
                if (!txnId.empty() && entity->executedTransactions.count(txnId) == 0) {
                    if (!info.from.empty() && !info.to.empty() && info.amount > 0) {
                        entity->updateBalances(info.from, info.to, info.amount);
                        entity->executedTransactions.insert(txnId);
                    }
                }
                if (info.client_port > 0) {
                    nlohmann::json response{
                        {"type","Response"},
                        {"view", entity->getState().getViewNumber()},
                        {"timestamp", info.timestamp},
                        {"message_sender_id", entity->getNodeId()},
                        {"result","success"}
                    };
                    Message reply(response.dump());
                    entity->sendTo(info.client_port, reply);
                }
            }

            return true;
        }

        // Fallback JSON path unchanged
        auto j = nlohmann::json::parse(message->getContent());
        // std::cout << "[Node " << entity->getNodeId() << "] Completing operation for sequence " << j["sequence"] << std::endl;
        // std::cout << "JSON Message Details - Type: " << j["type"]
        //           << ", View: " << j["view"]
        //           << ", Sequence: " << j["sequence"]
        //           << ", Timestamp: " << j["timestamp"]
        //           << ", Operation: " << j["operation"]
        //           << ", Client Port: " << j["client_listen_port"]
        //           << ", From: " << j["transaction"]["from"]
        //           << ", To: " << j["transaction"]["to"]
        //           << ", Amount: " << j["transaction"]["amount"]
        //           << std::endl;
        int seq = j["sequence"].get<int>();
        if (entity->processedOperations.count(seq)) return true;

        // JSON completion logic (parity with proto path)
        Entity::PrePrepareInfo info;
        info.timestamp   = j.value("timestamp", std::string());
        info.operation   = j.value("operation", std::string());
        info.client_port = j.value("client_listen_port", -1);
        if (j.contains("transaction") && j["transaction"].is_object()) {
            const auto& tx = j["transaction"];
            info.from   = tx.value("from", std::string());
            info.to     = tx.value("to", std::string());
            info.amount = tx.value("amount", 0);
        }

        // Fill missing fields from stored PrePrepare (if available)
        {
            std::lock_guard<std::mutex> lk(entity->prePrepareMtx);
            auto it = entity->prePrepareIndex.find(seq);
            if (it != entity->prePrepareIndex.end()) {
                const auto& stored = it->second;
                if (info.timestamp.empty())    info.timestamp = stored.timestamp;
                if (info.operation.empty())    info.operation = stored.operation;
                if (info.client_port < 0)      info.client_port = stored.client_port;
                if (info.from.empty())         info.from = stored.from;
                if (info.to.empty())           info.to = stored.to;
                if (info.amount <= 0)          info.amount = stored.amount;
                entity->prePrepareIndex.erase(it);
            }
        }

        const std::string& txnId = info.timestamp;
        if (!txnId.empty() && entity->executedTransactions.count(txnId) == 0) {
            if (!info.from.empty() && !info.to.empty() && info.amount > 0) {
                entity->updateBalances(info.from, info.to, info.amount);
                entity->executedTransactions.insert(txnId);
                std::cout << "[Node " << entity->getNodeId() << "] Transaction executed: "
                          << info.from << " -> " << info.to << " : " << info.amount
                          << " Sequence: " << seq << std::endl << std::endl;
            }
        }

        entity->commitOperations[seq] = info.operation;
        entity->markOperationProcessed(seq);

        // Reply once to client
        // if (info.client_port > 0) {
        //     nlohmann::json response{
        //         {"type","Response"},
        //         {"view", entity->getState().getViewNumber()},
        //         {"timestamp", info.timestamp},
        //         {"message_sender_id", entity->getNodeId()},
        //         {"result","success"}
        //     };
        //     Message reply(response.dump());
        //     entity->sendTo(info.client_port - 5000, reply);
        // }
        return true;
    }
};

class UpdateLockedQCEvent : public BaseEvent {
public:
    UpdateLockedQCEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message* message, EntityState* state) override {
        // Proto fast-path
        if (auto p = dynamic_cast<const ProtoMessage*>(message)) {
            const std::string qcStr = p->qc(); // QC carried as string (e.g., view number)
            if (qcStr.empty()) return true;   // No QC present
            state->setLockedQC(qcStr);
            return true;
        }

        // JSON fallback (original behavior)
        auto j = nlohmann::json::parse(message->getContent(), nullptr, false);
        if (j.is_discarded() || !j.contains("qc")) {
            return false;
        }
        // Store or update the lockedQC in the state or entity
        try {
            state->setLockedQC(j["qc"].get<std::string>());
        } catch (...) {
            // If qc isn’t a string, store serialized form
            state->setLockedQC(j["qc"].dump());
        }
        return true;
    }
};

class BroadcastIfLeaderEvent : public BaseEvent {
public:
    BroadcastIfLeaderEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message* message, EntityState* state) override {
        const int curView = entity->entityInfo["view"].get<int>();
        const int leaderId = (curView + 1) % entity->peerPorts.size();
        if (entity->getNodeId() != leaderId) {
            // Not leader: do nothing
            return true;
        }

        // Proto fast-path
        if (auto p = dynamic_cast<const ProtoMessage*>(message)) {
            std::string phase = p->explicit_type();
            int seq = p->sequence();
            

            // Faulty leader skips prepare broadcast
            std::string phaseLower = phase;
            for (auto& c : phaseLower) c = (char)std::tolower(c);
            if (entity->isByzantine && phaseLower == "prepare") return true;

            // SBFT prepare timer gating based on unique senders (no dataset scan)
            if (phaseLower == "prepare" &&
                entity->protocolConfig["protocol"].as<std::string>() == "SBFT") {
                size_t uniqueCount = 0;
                {
                    std::lock_guard<std::mutex> lk(entity->senderIdsMtx);
                    const std::string aggKey = phase + "_" + std::to_string(seq);
                    auto it = entity->keyToSenderIds.find(aggKey);
                    if (it != entity->keyToSenderIds.end()) uniqueCount = it->second.size();
                }
                // if (entity->preparePhaseTimerRunning[seq] && uniqueCount == (entity->getF() * 3)+1) {
                //     return false;
                // }
            }

            // Prepare the broadcast message for next_state using protobuf
            YAML::Node phaseConfig = entity->getPhaseConfigInsensitive(phase);
            if (phaseConfig && phaseConfig["next_state"]) {
                bedrock::ProtocolEnvelope env;
                const std::string next = phaseConfig["next_state"].as<std::string>();
                const std::string qcStr = (state && !state->getLockedQC().empty())
                                              ? state->getLockedQC()
                                              : std::string();

                auto fillCombined = [&](auto* msg) {
                    std::lock_guard<std::mutex> lk(entity->senderIdsMtx);
                    const std::string aggKey = phase + "_" + std::to_string(seq);
                    auto it = entity->keyToSenderIds.find(aggKey);
                    if (it == entity->keyToSenderIds.end()) return;
                    for (int sid : it->second) {
                        auto* am = msg->add_combined_messages();
                        am->set_view(p->view());
                        am->set_sequence(seq);
                        am->set_message_sender_id(sid);
                        // Optional richer fields for leader's own PrePrepare
                        if (phase == "PrePrepare" && sid == entity->getNodeId()) {
                            am->set_client_listen_port(p->client_listen_port());
                            am->set_timestamp(p->timestamp());
                            if (p->has_tx()) {
                                auto* t = am->mutable_transaction();
                                t->set_from(p->tx_from());
                                t->set_to(p->tx_to());
                                t->set_amount(p->tx_amount());
                            }
                        }
                    }
                    std::cout << "[Node " << entity->getNodeId() << "] Prepared combined messages for " << next << " of seq " << seq
                              << " with " << it->second.size() << " unique senders.\n";
                };
                if(next=="Request"){
                    return true;
                }
                else if (next == "prepare") {
                    auto* m = env.mutable_prepare();
                    m->set_view(p->view());
                    m->set_type(next);
                    m->set_sequence(seq);
                    m->set_operation(p->operation());
                    m->set_message_sender_id(entity->getNodeId());
                    m->set_qc(qcStr);
                    fillCombined(m);
                } else if (next == "commit") {
                    auto* m = env.mutable_commit();
                    m->set_view(p->view());
                    m->set_type(next);
                    m->set_sequence(seq);
                    m->set_operation(p->operation());
                    m->set_message_sender_id(entity->getNodeId());
                    m->set_qc(qcStr);
                    fillCombined(m);
                    entity->sendProtocolToAll(env);
                    // Leader processes commit locally (sendProtocolToAll skips self)
                    entity->processProtocolEnvelope(env);
                    return true;
                } else {
                    auto* m = env.mutable_prepare();
                    m->set_type(next);
                    m->set_view(p->view());
                    m->set_sequence(seq);
                    m->set_operation(p->operation());
                    m->set_message_sender_id(entity->getNodeId());
                    m->set_qc(qcStr);
                    fillCombined(m);
                }
                // std::cout << "[Node " << entity->getNodeId() << "] Leader broadcasting " << next << " for sequence " << seq << "\n";
                entity->sendProtocolToAll(env);
            }
            return true;
        }

        // JSON fallback (kept functional but lighter; no dataset scan)
        auto j = nlohmann::json::parse(message->getContent());
        std::string phase = j.value("type", "");
        int seq = j.value("sequence", -1);
        if (phase.empty() || seq < 0) return true;

        std::string phaseLower = phase;
        for (auto& c : phaseLower) c = (char)std::tolower(c);
        if (entity->isByzantine && phaseLower == "prepare") return true;

        // SBFT prepare timer gating
        if (phaseLower == "prepare" &&
            entity->protocolConfig["protocol"].as<std::string>() == "SBFT") {
            size_t uniqueCount = 0;
            {
                std::lock_guard<std::mutex> lk(entity->senderIdsMtx);
                const std::string aggKey = phase + "_" + std::to_string(seq);
                auto it = entity->keyToSenderIds.find(aggKey);
                if (it != entity->keyToSenderIds.end()) uniqueCount = it->second.size();
            }
            if (entity->preparePhaseTimerRunning[seq] && uniqueCount <= 7) {
                return false;
            }
        }

        // Combined messages built from unique sender ids
        nlohmann::json combinedMessages = nlohmann::json::array();
        {
            std::lock_guard<std::mutex> lk(entity->senderIdsMtx);
            const std::string aggKey = phase + "_" + std::to_string(seq);
            auto it = entity->keyToSenderIds.find(aggKey);
            if (it != entity->keyToSenderIds.end()) {
                for (int sid : it->second) {
                    nlohmann::json filtered;
                    filtered["view"] = entity->getState().getViewNumber();
                    filtered["sequence"] = seq;
                    filtered["digest"] = "";
                    filtered["message_sender_id"] = sid;
                    filtered["signature"] = "";
                    filtered["client_listen_port"] = j.value("client_listen_port",-1);
                    filtered["clientid"] = j.value("clientid","");
                    filtered["timestamp"] = j.value("timestamp","");
                    filtered["transaction"] = j.value("transaction", nlohmann::json{});
                    combinedMessages.push_back(std::move(filtered));
                }
            }
        }

        YAML::Node phaseConfig = entity->getPhaseConfigInsensitive(phase);
        if (phaseConfig && phaseConfig["next_state"]) {
            nlohmann::json outMsg;
            outMsg["type"] = phase;
            outMsg["view"] = entity->getState().getViewNumber();
            outMsg["sequence"] = seq;
            outMsg["operation"] = j.value("operation", "");
            outMsg["message_sender_id"] = entity->getNodeId();
            outMsg["combinedMessages"] = std::move(combinedMessages);
            outMsg["qc"] = (state && !state->getLockedQC().empty())
                               ? nlohmann::json(state->getLockedQC())
                               : nlohmann::json("");
            outMsg["transaction"] = j.value("transaction", nlohmann::json{});
            outMsg["client_listen_port"] = j.value("client_listen_port", -1);
            outMsg["clientid"] = j.value("clientid", "");
            outMsg["timestamp"] = j.value("timestamp", "");
            Message protocolMsg(outMsg.dump());
            entity->sendToAll(protocolMsg);
        }

        return true;
    }
};

class UnicastIfParticipantEvent : public BaseEvent {
public:
    UnicastIfParticipantEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message* message, EntityState* state) override {
        bool condition = (entity->entityInfo["view"].get<int>() + 1) % (entity->peerPorts.size()) != entity->getNodeId();
        
        if (condition) {
            // Fast-path: if incoming is ProtoMessage, build next-phase ProtocolEnvelope and unicast
            if (auto p = dynamic_cast<const ProtoMessage*>(message)) {
                // Reproduce original JSON-dependent logic via YAML phase config
                YAML::Node phaseConfig = entity->getPhaseConfig(p->explicit_type());
                if (phaseConfig && phaseConfig["next_state"]) {
                    const std::string nextPhase = phaseConfig["next_state"].as<std::string>();
                    // std::cout << "[Node " << entity->getNodeId() << "] Unicasting " << nextPhase << " for sequence " << p->sequence() << " to leader\n";
                    const int seq = p->sequence();
                    {
                        std::lock_guard<std::mutex> pg(entity->processedMtx);
                        if (entity->processedOperations.count(seq)) return false;
                    }
                    const int leaderId = (entity->entityInfo["view"].get<int>() + 1) % entity->peerPorts.size();

                    // Build typed envelope (Prepare or Commit) mirroring original field set
                    bedrock::ProtocolEnvelope env;
                    const std::string qcStr = (state && !state->getLockedQC().empty())
                                              ? state->getLockedQC()
                                              : std::string();

                    if (nextPhase == "Prepare") {
                        auto* m = env.mutable_prepare();
                        m->set_view(entity->entityInfo["view"].get<int>());
                        m->set_type(nextPhase);
                        m->set_sequence(seq);
                        m->set_operation(p->operation());
                        m->set_message_sender_id(entity->getNodeId());
                        m->set_qc(qcStr);
                        // Preserve transaction/timestamp if they were in PrePrepare
                        if (p->has_tx()) {
                            auto* tx = m->add_combined_messages()->mutable_transaction();
                            tx->set_from(p->tx_from());
                            tx->set_to(p->tx_to());
                            tx->set_amount(p->tx_amount());
                        }
                    } else if (nextPhase == "Commit") {
                        auto* m = env.mutable_commit();
                        m->set_type(nextPhase);
                        m->set_view(entity->entityInfo["view"].get<int>());
                        m->set_sequence(seq);
                        m->set_operation(p->operation());
                        m->set_message_sender_id(entity->getNodeId());
                        m->set_qc(qcStr);
                        // (No extra fields added; matches original JSON resend semantics)
                    } else {
                        // If nextPhase not one of expected, fall back to original JSON path
                        // (Do not alter logic)
                        auto* m = env.mutable_prepare();
                        m->set_type(nextPhase);
                        m->set_view(entity->entityInfo["view"].get<int>());
                        m->set_sequence(seq);
                        m->set_operation(p->operation());
                        m->set_message_sender_id(entity->getNodeId());
                        m->set_qc(qcStr);
                    }
                    
                    // Attempt proto unicast (assumes Entity has sendProtocolTo)
                    // If your Entity lacks this method, implement it; otherwise keep JSON path.
                    entity->sendProtocolTo(leaderId, env);
                    // std::cout << "[Node " << entity->getNodeId() << "] Unicasted " << p->explicit_type() << " for sequence " << seq << " to leader\n";
                    return true;
                } else {
                    // No next_state -> keep original behavior (do nothing)
                    return true;
                }
            }

json_fallback:
            // Original JSON logic preserved exactly
            auto j = nlohmann::json::parse(message->getContent());
            int seq = j["sequence"].get<int>();
            YAML::Node phaseConfig = entity->getPhaseConfig(j["type"]);
            if (phaseConfig["next_state"]) {
                std::string nextPhase = phaseConfig["next_state"].as<std::string>();
                j["type"] = nextPhase;
                j["message_sender_id"] = entity->getNodeId();
                j["qc"] = state->getLockedQC(); // Include QC if available
                Message protocolMsg(j.dump());
                int leaderId = (entity->entityInfo["view"].get<int>() + 1) % (entity->peerPorts.size());
                entity->sendTo(leaderId, protocolMsg);
            }
        }
        return true;
    }
};


// filepath: /Users/eswar/Downloads/CppBedrock/src/core/events/EventFactory.cpp
class HandleViewChangeEvent : public BaseEvent {
public:
    HandleViewChangeEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message* message, EntityState*) override {
        std::cout << "[Node " << entity->getNodeId() << "] Handling ViewChange event\n";
        int newView = -1;
        int senderId = -1;
        nlohmann::json j;  // only filled for JSON path

        // ── Proto fast-path ─────────────────────────────────────
        if (auto p = dynamic_cast<const ProtoMessage*>(message)) {
            // assuming your proto has fields: type/view/new_view/message_sender_id/etc.
            // If it only carries a generic envelope, adapt accordingly.
            if (p->explicit_type() != "ViewChange") {
                // Not a ViewChange; ignore
                return true;
            }
            try {
                newView  = p->view();           // or a dedicated new_view field if you have one
                senderId = p->sender_id();
            } catch (...) {
                return true;
            }
        } else {
            // ── Existing JSON path ──────────────────────────────
            j = nlohmann::json::parse(message->getContent());
            newView  = j["new_view"].get<int>();
            senderId = j["message_sender_id"].get<int>();
        }

        if (newView < 0) return true;
        //std::cout << "[Node " << entity->getNodeId() << "] Received ViewChange for view " << newView << " from Node " << senderId << "\n";
        entity->viewChangeMessages[newView].push_back(
            j.is_null() ? nlohmann::json{
                              {"new_view", newView},
                              {"message_sender_id", senderId}}
                        : j);
        std::cout << "[Node " << entity->getNodeId() << "] Handling ViewChange event plus" << entity->viewChangeMessages[newView].size() << "\n";
        int quorum = computeQuorumEventFactory("2f", entity->getF());
        if ((int)entity->viewChangeMessages[newView].size() >= quorum && entity->inViewChange) {
            int leaderId = (newView + 1) % (entity->peerPorts.size());
            if (entity->getNodeId() == leaderId && !entity->isByzantine) {
                std::cout << "[Node " << entity->getNodeId() << "] Achieved quorum for view " << newView << ". Broadcasting NewView and PrePrepares.\n";
                // Broadcast NewView
                nlohmann::json newViewMsg;
                newViewMsg["type"] = "NewView";
                newViewMsg["new_view"] = newView;
                newViewMsg["message_sender_id"] = entity->getNodeId();
                Message msg(newViewMsg.dump());
                entity->sendToAll(msg);

                // --- Aggregate prepare messages (JSON as before) ---
                std::map<int, nlohmann::json> bestPreparePerSeq; // seq -> prepare message
                for (const auto& viewChangeMsg : entity->viewChangeMessages[newView]) {
                    if (viewChangeMsg.contains("prepare_messages") && viewChangeMsg["prepare_messages"].is_array()) {
                        for (const auto& prepareMsg : viewChangeMsg["prepare_messages"]) {
                            if (prepareMsg.contains("sequence")) {
                                int seq = prepareMsg["sequence"].get<int>();
                                bestPreparePerSeq[seq] = prepareMsg;
                            }
                        }
                    }
                }

                for (const auto& item : bestPreparePerSeq) {
                    int seq = item.first;
                    const auto& prepareMsg = item.second;

                    std::string stringforDigest;
                    if (prepareMsg.contains("transaction") && prepareMsg.contains("timestamp")) {
                        stringforDigest = prepareMsg["transaction"].dump() + prepareMsg["timestamp"].get<std::string>();
                    } else {
                        stringforDigest = "";
                    }
                    std::string digest    = stringforDigest.empty() ? "" : computeSHA256(stringforDigest);
                    std::string signature = stringforDigest.empty() ? "" : entity->cryptoProvider->sign(stringforDigest);

                    nlohmann::json preprepareMsg;
                    preprepareMsg["type"]          = "PrePrepare";
                    preprepareMsg["toturnoffflag"] = "true";
                    preprepareMsg["view"]          = entity->entityInfo["view"];
                    preprepareMsg["sequence"]      = seq;
                    preprepareMsg["digest"]        = digest;
                    preprepareMsg["signature"]     = signature;

                    std::string clientid;
                    if (prepareMsg.contains("clientid") && prepareMsg["clientid"].is_string()) {
                        clientid = prepareMsg["clientid"].get<std::string>();
                    } else if (prepareMsg.contains("message_sender_id")) {
                        if (prepareMsg["message_sender_id"].is_string()) {
                            clientid = prepareMsg["message_sender_id"].get<std::string>();
                        } else if (prepareMsg["message_sender_id"].is_number_integer()) {
                            clientid = std::to_string(prepareMsg["message_sender_id"].get<int>());
                        }
                    } else {
                        clientid = "";
                    }
                    preprepareMsg["clientid"]   = clientid;
                    preprepareMsg["transaction"] = prepareMsg.value("transaction", nlohmann::json{});
                    preprepareMsg["timestamp"]   = prepareMsg.value("timestamp", "");
                    preprepareMsg["operation"]   = prepareMsg.value("operation", "");
                    preprepareMsg["message_sender_id"] = entity->getNodeId();

                    Message protocolMsg(preprepareMsg.dump());
                    entity->sendToAll(protocolMsg);
                }
            }
        }

        return true;
    }
};

// NewView Event
class HandleNewViewEvent : public BaseEvent {
public:
    HandleNewViewEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message* message, EntityState*) override {
        int newView = -1;

        // ── Proto fast-path ─────────────────────────────────────
        if (auto p = dynamic_cast<const ProtoMessage*>(message)) {
            if (p->explicit_type() != "NewView") {
                // not a NewView; ignore
                return true;
            }
            try {
                newView = p->view();   // or p->new_view() if your proto defines it
            } catch (...) {
                return true;
            }
        } else {
            // ── JSON fallback ───────────────────────────────────
            auto j = nlohmann::json::parse(message->getContent());
            newView = j["new_view"].get<int>();
        }

        if (newView < 0) return true;

        std::cout << "[Node " << entity->getNodeId() << "] Received NewView for view " << newView << "\n";
        entity->entityInfo["view"] = newView;
        entity->inViewChange = false;

        entity->viewChangeMessages.erase(newView);
        {
            if (entity->timeKeeper) {
                entity->timeKeeper->stop();
                entity->timeKeeper.reset();
            }
        }

        // Discard stale pending requests and stop the batch timer.
        // Requests queued before the view change belong to the old term; the new
        // leader (possibly a different node) will repropose them from its own queue.
        if (entity->batchTimer) {
            entity->batchTimer->stop();
            entity->batchTimer.reset();
        }
        {
            std::lock_guard<std::mutex> lk(entity->batchMtx);
            entity->pendingBatch.clear();
        }

        return true;
    }
};

class HandleViewChangeHotstuffEvent : public BaseEvent {
public:
    HandleViewChangeHotstuffEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message* message, EntityState* state) override {
        auto j = nlohmann::json::parse(message->getContent());
        int newView = j["new_view"].get<int>();
        int sender = j["message_sender_id"].get<int>();

        // Store the sender for quorum counting
        entity->viewChangeMessages[newView].push_back(j);

        // Store all required data in an array for this view
        ViewChangeData data;
        data.sender = sender;
        data.last_sequence = j.value("last_sequence", -1);
        data.last_operation = j.value("last_operation", "");
        data.locked_qc = j.value("locked_qc", nlohmann::json{});
        entity->viewChangeDataArray[newView].push_back(data);
        // Update local view if needed
        if (newView > entity->entityInfo["view"].get<int>()) {
            entity->entityInfo["view"] = newView;
            // entity->saveEntityInfo();  
            //std::cout << "[Node " << entity->getNodeId() << "] Updated to new view: " << newView << std::endl;
        }

        // If this node is the new leader, and has enough view change messages, propose a new block
        int n = entity->peerPorts.size();
        int quorum = computeQuorumEventFactory("2f+1", entity->getF());
        int leaderId = (newView + 1) % n;
        if (entity->getNodeId() == leaderId && entity->viewChangeMessages[newView].size() >= quorum) {
            //std::cout << "[Node " << entity->getNodeId() << "] I am the new leader for view " << newView << ", proposing new block." << std::endl;

            // Find the highest QC and associated data
            int highestViewNum = -1;
            nlohmann::json highestQC;
            int parentSeq = -1;
            std::string lastOp;
            
            nlohmann::json newViewMsg;
            newViewMsg["type"] = "NewView";
            newViewMsg["new_view"] = newView;
            newViewMsg["message_sender_id"] = entity->getNodeId();
            Message msg(newViewMsg.dump());
            entity->sendToAll(msg);
            
            for (const auto& d : entity->viewChangeDataArray[newView]) {
                //std::cout << "[Node " << entity->getNodeId() << "] Processing view change data from sender " << d.sender << "\n";
                if (!d.locked_qc.empty()) {
                    int qcView = std::stoi(d.locked_qc.get<std::string>());
                    //std::cout << "[Node " << entity->getNodeId() << "] Checking QC from sender " << d.sender << " with view " << qcView << "\n";
                    if (qcView > highestViewNum) {
                        highestViewNum = qcView;
                        highestQC = d.locked_qc;
                        parentSeq = d.last_sequence;
                        lastOp = d.last_operation;
                    }
                }
            }
            

            if (parentSeq == -1 || lastOp.empty()) {
                //std::cout << "[Node " << entity->getNodeId() << "] Skipping proposal: parentSeq == -1 or lastOp is empty\n";
                entity->inViewChange = false;
                {
                    std::lock_guard<std::mutex> lock(entity->timerMtx);
                    if (entity->timeKeeper) {
                        entity->timeKeeper->stop();
                        entity->timeKeeper.reset();
                    }
                }
                entity->viewChangeMessages.erase(newView);
                entity->viewChangeDataArray.erase(newView);
                return true;
            }
            // Construct new proposal
            nlohmann::json proposal;
            proposal["type"] = "Prepare";
            proposal["toturnoffflag"] = "true";
            proposal["view"] = newView;
            proposal["sender"] = entity->getNodeId();
            proposal["sequence"] = parentSeq;
            proposal["operation"] = lastOp;
            proposal["qc"] = highestQC;

            //std::cout << "[Node " << entity->getNodeId() << "] New proposal: " << proposal.dump() << std::endl;
            entity->inViewChange = false;
            std::lock_guard<std::mutex> lock(entity->timerMtx);
            if (entity->timeKeeper) {
                entity->timeKeeper->stop();
                entity->timeKeeper.reset();
            }
            entity->viewChangeMessages.erase(newView);
            entity->viewChangeDataArray.erase(newView);
            Message proposalMsg(proposal.dump());
            entity->sendToAll(proposalMsg);
        }
        return true;
    }
};

class HandleClientRequestAsLeaderEvent : public BaseEvent {
public:
    HandleClientRequestAsLeaderEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message* message, EntityState* state) override {
        auto now = std::chrono::system_clock::now();
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % std::chrono::seconds(1);
        std::time_t tt = std::chrono::system_clock::to_time_t(now);
        std::tm tm{};
        localtime_r(&tt, &tm); // thread-safe on macOS

        std::cout << "[Node " << entity->getNodeId() << "] Handling client request " 
                  << " at " << std::put_time(&tm, "%F %T") << '.'
                  << std::setw(3) << std::setfill('0') << ms.count()
                  << "\n";
        int n = entity->peerPorts.size();
        int currentView = entity->entityInfo["view"].get<int>();
        int leaderId = (currentView + 1) % n;
        nlohmann::json j = nlohmann::json::parse(message->getContent());
        if (leaderId != entity->getNodeId()) {
            std::cout << "[Node " << entity->getNodeId()
            << "] Not the leader, trying to reach leader " << leaderId << "\n";

            // Use leader's gRPC port instead of raw TCP PBFT port
            const int grpcLeaderPort = 15000 + leaderId; // e.g. 15001 if node ports start at 5001

            try {
                // Simple TCP connect to gRPC port as a reachability check
                int sock = ::socket(AF_INET, SOCK_STREAM, 0);
                if (sock < 0) {
                    throw std::runtime_error("socket() failed");
                }

                sockaddr_in addr{};
                addr.sin_family = AF_INET;
                addr.sin_port = htons(grpcLeaderPort);
                addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK); // 127.0.0.1

                if (::connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
                    ::close(sock);
                    throw std::runtime_error("connect() failed");
                }
                ::close(sock);

                // Leader gRPC port is reachable: start / arm timeout watchdog
                if (!entity->timeKeeper) {
                    entity->timeKeeper = std::make_unique<TimeKeeper>(entity->viewChangeTimeoutMs, [entity] {
                        entity->onTimeout();
                    });
                }
                entity->timeKeeper->start();
            } catch (...) {
                // Could not reach leader's gRPC port: trigger view change immediately
                entity->onTimeout();
            }
            return false;
        }
        std::cout << "[Node " << entity->getNodeId() << "] Handling Request as leader\n";
        
        std::string operation = j["operation"].get<std::string>();
        std::string currentPhase = j["type"];
        // print entity->entityInfo json
        // std::cout << "[Node " << entity->getNodeId() << "] Entity Info: " << entity->entityInfo.dump(4) << "\n";

        if(entity->isByzantine){
            //std::cout << "[Node " << entity->getNodeId() << "] I am Byzantine, skipping broadcast.\n";
            return false;
        }
        {
            auto byzState = entity->getActiveByzantineState();
            if (byzState.proposalDelayMs > 0) {
                std::cout << "[Node " << entity->getNodeId()
                          << "] Byzantine proposal_delay " << byzState.proposalDelayMs << "ms\n";
                std::this_thread::sleep_for(std::chrono::milliseconds(byzState.proposalDelayMs));
            }
        }
        // Dedup: drop requests for already-committed operations
        const std::string op = j.value("operation", "");
        if (!op.empty() && op.size() > 9) {
            try {
                if (entity->hasProcessedOperation(std::stoi(op.substr(9)))) return true;
            } catch (...) {}
        }

        // Add request to the pending batch, flush when size threshold is hit.
        {
            std::lock_guard<std::mutex> bLk(entity->batchMtx);
            entity->pendingBatch.push_back(j);

            if ((int)entity->pendingBatch.size() < entity->batchSize) {
                return true; // waiting for more requests or timer
            }
            // Size threshold reached,fall through to flush (lock released below).
        }
        // eventMtx is already held by processJsonFromGrpc, use the locked variant.
        entity->flushBatchLocked();
        // Restart the timer.
        if (entity->batchTimer) entity->batchTimer->reset();
        return true;
    }
};

class VerifySignatureEvent : public BaseEvent {
public:
    VerifySignatureEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message* message, EntityState* state) override {
        if (dynamic_cast<const ProtoMessage*>(message)) {
            return true; // TODO: add real proto signature check
        }
        auto j = nlohmann::json::parse(message->getContent());
        // ...existing verification logic...
        return true;
    }
};

class QueryBalancesEvent : public BaseEvent {
public:
    QueryBalancesEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message* message, EntityState* state) override {
        auto j = nlohmann::json::parse(message->getContent());
        
        if (j.contains("client_listen_port")) {
            int clientPort = j["client_listen_port"].get<int>();

            // // Debug: print current committed and speculative balances
            // std::cout << "[Node " << entity->getNodeId() << "] Current balances:";
            // if (entity->balances.empty()) {
            //     std::cout << " <empty>";
            // } else {
            //     for (const auto& [k, v] : entity->balances) {
            //         std::cout << " " << k << "=" << v;
            //     }
            // }
            // std::cout << std::endl;

            // std::cout << "[Node " << entity->getNodeId() << "] Current sbalances:";
            // if (entity->speculativeBalances.empty()) {
            //     std::cout << " <empty>";
            // } else {
            //     for (const auto& [k, v] : entity->speculativeBalances) {
            //         std::cout << " " << k << "=" << v;
            //     }
            // }
            // std::cout << std::endl;

            nlohmann::json response;
            response["type"] = "BalancesReply";
            response["view"] = entity->entityInfo["view"];
            // If balances is empty, send speculativeBalances instead
            if (entity->balances.empty()) {
                response["balances"] = entity->speculativeBalances;
            } else {
                response["balances"] = entity->balances;
            }
            response["message_sender_id"] = entity->getNodeId();
            Message BalancesReply(response.dump());
            entity->sendTo(clientPort-5000, BalancesReply);
        }
        return true;
    }
};

class StorePiggybackEvent : public BaseEvent {
public:
    StorePiggybackEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}
    bool execute(Entity* entity, const Message* message, EntityState* state) override {
        auto j = nlohmann::json::parse(message->getContent());
        int seq = j["sequence"].get<int>();

        YAML::Node phaseConfig = entity->getPhaseConfig(j["type"]);
        if (phaseConfig["next_state"]) {
            std::string nextPhase = phaseConfig["next_state"].as<std::string>();
            j["type"] = nextPhase;
            j["qc"] = state->getLockedQC(); // Include QC if available
            j["message_sender_id"] = entity->getNodeId(); // Add sender ID to the message

            // Store in piggyback array
            {
                std::lock_guard<std::mutex> lock(entity->piggybackMtx);
                entity->piggyback.push_back(j);
            }

            // Optionally print for debug
            // std::cout << "[Node " << entity->getNodeId() << "] Stored message in piggyback: " << j.dump() << std::endl;
        }
        return true;
    }
};

#include <thread>
#include <chrono>

class PeriodicPiggybackBroadcastEvent : public BaseEvent {
public:
    PeriodicPiggybackBroadcastEvent(const nlohmann::json& params = {}) : BaseEvent(params) {}

    bool execute(Entity* entity, const Message*, EntityState* state) override {
        // Only start one broadcast thread per entity
        if (entity->piggybackBroadcastStarted) return true;
        entity->piggybackBroadcastStarted = true;

        std::thread([entity]() {
            while (true) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                nlohmann::json piggybackCopy;
                {
                    std::lock_guard<std::mutex> lock(entity->piggybackMtx);
                    if (entity->piggyback.is_array() && !entity->piggyback.empty()) {
                        piggybackCopy = entity->piggyback;
                        entity->piggyback.clear();
                    }
                }
                if (!piggybackCopy.is_null() && piggybackCopy.is_array() && !piggybackCopy.empty()) {
                    nlohmann::json outMsg;
                    outMsg["type"] = "PiggybackBroadcast";
                    outMsg["view"] = entity->entityInfo["view"];
                    outMsg["message_sender_id"] = entity->getNodeId();
                    outMsg["piggyback"] = piggybackCopy;
                    Message protocolMsg(outMsg.dump());
                    entity->sendToAll(protocolMsg);
                    // std::cout << "[Node " << entity->getNodeId() << "] Periodically broadcasted piggyback: " << piggybackCopy.dump(2) << std::endl;
                }
            }
        }).detach();
        return true;
    }
};

// Singleton instance of the EventFactory
EventFactory& EventFactory::getInstance() {
    static EventFactory instance;
    return instance;
}



// Create event based on name
std::unique_ptr<BaseEvent> EventFactory::createEvent(const std::string& name, const nlohmann::json& params) {
    auto it = factoryMap.find(name);
    if (it != factoryMap.end()) {
        return it->second(params);
    }
    return nullptr;
}

// Register all events
void EventFactory::initialize() {
    // Register base protocol events
    this->registerEvent<StoreMessageEvent>("storeMessage");
    this->registerEvent<CheckQuorumEvent>("checkQuorum");
    this->registerEvent<CheckQuorumEventForSBFT>("checkQuorumForSBFT");
    this->registerEvent<CheckQCEvent>("checkQC");
    this->registerEvent<BroadcastEvent>("broadcast");
    this->registerEvent<ManageTimerEvent>("manageTimer");
    this->registerEvent<CompleteEvent>("complete");
    this->registerEvent<StartTimerEvent>("startTimer");
    this->registerEvent<ResetTimerEvent>("resetTimer");
    this->registerEvent<StopTimerEvent>("stopTimer");
    
    // Register handle events
    
    this->registerEvent<HandleViewChangeEvent>("handleViewChange");
    this->registerEvent<HandleNewViewEvent>("handleNewView");
    this->registerEvent<BroadcastIfLeaderEvent>("broadcastifLeader");
    this->registerEvent<UnicastIfParticipantEvent>("unicastifParticipant");
    this->registerEvent<UpdateLockedQCEvent>("UpdateLockedQC");
    this->registerEvent<HandleViewChangeHotstuffEvent>("handleViewChangeHotstuff");
    this->registerEvent<HandleClientRequestAsLeaderEvent>("handleClientRequestAsLeader");
    this->registerEvent<VerifySignatureEvent>("verifySignature");
    this->registerEvent<QueryBalancesEvent>("queryBalances");
    this->registerEvent<StorePiggybackEvent>("storePiggyback");
    this->registerEvent<PeriodicPiggybackBroadcastEvent>("periodicPiggybackBroadcast");
    

    registerUncommonEvents(*this);
}

// Explicit template instantiations to avoid linker errors (for all events you register)
template void EventFactory::registerEvent<IncrementSequenceEvent>(const std::string&);
template void EventFactory::registerEvent<AddLogEvent>(const std::string&);
template void EventFactory::registerEvent<UpdateLogEvent>(const std::string&);
template void EventFactory::registerEvent<SendToClientEvent>(const std::string&);

template void EventFactory::registerEvent<HandleViewChangeEvent>(const std::string&);
template void EventFactory::registerEvent<HandleNewViewEvent>(const std::string&);
template void EventFactory::registerEvent<StoreMessageEvent>(const std::string&);
template void EventFactory::registerEvent<ManageTimerEvent>(const std::string&);
template void EventFactory::registerEvent<CheckQuorumEvent>(const std::string&);
template void EventFactory::registerEvent<CheckQuorumEventForSBFT>(const std::string&);
template void EventFactory::registerEvent<CheckQCEvent>(const std::string&);
template void EventFactory::registerEvent<BroadcastEvent>(const std::string&);
template void EventFactory::registerEvent<CompleteEvent>(const std::string&);
template void EventFactory::registerEvent<BroadcastIfLeaderEvent>(const std::string&);
template void EventFactory::registerEvent<UnicastIfParticipantEvent>(const std::string&);
template void EventFactory::registerEvent<UpdateLockedQCEvent>(const std::string&);
template void EventFactory::registerEvent<StartTimerEvent>(const std::string&);
template void EventFactory::registerEvent<ResetTimerEvent>(const std::string&);
template void EventFactory::registerEvent<StopTimerEvent>(const std::string&);
template void EventFactory::registerEvent<HandleViewChangeHotstuffEvent>(const std::string&);
template void EventFactory::registerEvent<HandleClientRequestAsLeaderEvent>(const std::string&);
template void EventFactory::registerEvent<VerifySignatureEvent>(const std::string&);
template void EventFactory::registerEvent<QueryBalancesEvent>(const std::string&);
template void EventFactory::registerEvent<StorePiggybackEvent>(const std::string&);
template void EventFactory::registerEvent<PeriodicPiggybackBroadcastEvent>(const std::string&);


#include "../../../include/core/Entity.h" // or the header where computeQuorumEventFactory is defined

