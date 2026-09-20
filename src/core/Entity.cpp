#include "core/Entity.h"
#include "core/Log.h"
#include "core/events/EventFactory.h"
#include "core/events/MessageHandler.h"
#include "core/events/ProtoMessage.h"
#include "core/crypto/OpenSSLCryptoProvider.h"
#include "coordination/grpc/NodeServiceImpl.h"
#include "proto/bedrock.grpc.pb.h"
#include "proto/bedrock.pb.h"

#include <grpcpp/grpcpp.h>
#include <yaml-cpp/yaml.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using json = nlohmann::json;

namespace {

// Number of committed sequences kept behind the newest commit before their
// per-sequence bookkeeping is released. A view change re-proposes from the
// retained prepares, so this also bounds what a new leader can recover.
constexpr int kRetainedSequences = 2000;
constexpr int kPruneEveryCommits = 500;

// Helper: build typed envelope from a protocol JSON message for basic phases
bool buildEnvelopeFromJson(const std::string& jsonStr, bedrock::ProtocolEnvelope& env) {
    try {
        nlohmann::json j = nlohmann::json::parse(jsonStr);
        if (!j.contains("type") || !j["type"].is_string()) return false;
        const std::string t = j["type"].get<std::string>();

        if (t == "PrePrepare") {
            auto* m = env.mutable_pre_prepare();
            m->set_view(j.value("view", 0));
            m->set_sequence(j.value("sequence", 0));
            m->set_timestamp(j.value("timestamp", ""));
            m->set_operation(j.value("operation", ""));
            if (j.contains("transaction") && j["transaction"].is_object()) {
                const auto& txj = j["transaction"];
                auto* tx = m->mutable_transaction();
                tx->set_from(txj.value("from",""));
                tx->set_to(txj.value("to",""));
                tx->set_amount(txj.value("amount", 0));
            }
            m->set_client_id(j.value("client_id", ""));
            m->set_request_id(j.value("request_id", (uint64_t)0));
            m->set_signature(j.value("signature", ""));
            m->set_message_sender_id(j.value("message_sender_id", -1));
            m->set_type("PrePrepare");
            return true;
        }
        if (t == "Prepare") {
            auto* m = env.mutable_prepare();
            m->set_view(j.value("view", 0));
            m->set_sequence(j.value("sequence", 0));
            m->set_operation(j.value("operation", ""));
            m->set_message_sender_id(j.value("message_sender_id", 0));
            m->set_type("Prepare");
            return true;
        }
        if (t == "Commit") {
            auto* m = env.mutable_commit();
            m->set_view(j.value("view", 0));
            m->set_sequence(j.value("sequence", 0));
            m->set_operation(j.value("operation", ""));
            m->set_message_sender_id(j.value("message_sender_id", 0));
            m->set_type("Commit");
            return true;
        }
    } catch (...) {}
    return false;
}

// Parses the trailing "_<seq>" of a "<phase>_<seq>" aggregation key.
bool keySequence(const std::string& key, int& seq) {
    auto pos = key.rfind('_');
    if (pos == std::string::npos || pos + 1 >= key.size()) return false;
    try {
        seq = std::stoi(key.substr(pos + 1));
        return true;
    } catch (...) {
        return false;
    }
}

}  // namespace

// ===================== Entity Methods =====================
Entity::Entity(const EntityOptions& options)
    : options_(options),
      nodeId(options.nodeId),
      committee_(bedrock::Committee::loadFromFile(options.committeePath)),
      _entityState("Replica", "Request", 0, 0),
      sender_(options.nodeId),
      requestTimer_(viewChangeTimeoutMs, [this](uint64_t generation) { onRequestTimerExpired(generation); })
{
    if (!committee_.contains(nodeId)) {
        throw std::runtime_error("node id " + std::to_string(nodeId) + " is not in committee " +
                                 options.committeePath);
    }
    for (const auto& r : committee_.replicas()) peerIds_.push_back(r.id);
    f = committee_.f();

    EventFactory::getInstance().initialize();
    loadProtocolConfig(options.protocolConfigPath);
    pbftCore_ = (protocolName_ == "PBFT" || protocolName_ == "SBFT");
    if (options.initialElectionTimeoutMs > 0) viewChangeTimeoutMs = options.initialElectionTimeoutMs;
    if (options.initialSlowPathTimeoutMs > 0) fastPathWaitMs = options.initialSlowPathTimeoutMs;
    LOG_INFO("loaded protocol " << protocolName_ << " from " << options.protocolConfigPath
             << " (n=" << committee_.size() << " f=" << f
             << " election_timeout_ms=" << viewChangeTimeoutMs.load()
             << " slow_path_timeout_ms=" << fastPathWaitMs.load() << ")");

    timeKeeper = std::make_unique<TimeKeeper>(viewChangeTimeoutMs, [this] { this->onTimeout(); });
    entityInfo["server_name"] = nodeId;
    entityInfo["view"] = 0;
    entityInfo["sequence"] = 0;
    entityInfo["server_status"] = 1;

    const std::string keyPath = options.keysDir + "/server_" + std::to_string(nodeId) + "_private.pem";
    if (!std::filesystem::exists(keyPath)) {
        throw std::runtime_error("replica private key not found: " + keyPath);
    }
    cryptoProvider = std::make_unique<OpenSSLCryptoProvider>(keyPath);
    initializeConsensus();

    if (!options.failureSpecPath.empty()) {
        if (!pbftCore_) {
            throw std::runtime_error("--failure-spec is only supported for PBFT and SBFT (protocol " + protocolName_ + ")");
        }
        std::string section = protocolName_;
        std::transform(section.begin(), section.end(), section.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        proposalDelay_ = bedrock::ProposalDelayController::loadFile(options.failureSpecPath,
                                                                    options.failureStartUnixMs, section);
        LOG_INFO("loaded failure spec " << options.failureSpecPath << " section <" << section << ">: "
                 << proposalDelay_->phases().size() << " proposal-delay phase(s), warm_up_ms="
                 << proposalDelay_->warmUp().count() << ", start_unix_ms=" << options.failureStartUnixMs);
    } else {
        proposalDelay_ = std::make_unique<bedrock::ProposalDelayController>();
    }

    for (const auto& r : committee_.replicas()) {
        if (r.id == nodeId) continue;
        sender_.addPeer(r.id, r.address());
    }

    if (protocolName_ == "ChainedHotstuff") {
        auto event = EventFactory::getInstance().createEvent("periodicPiggybackBroadcast");
        if (event) event->execute(this, nullptr, nullptr);
    }
}

Entity::~Entity() {
    stop();
}

long long Entity::nowUs() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}

int Entity::getMaxSpeculativeSeq() const {
    int maxSeq = committedSeq;
    std::lock_guard<std::mutex> g(speculativeLogMtx);
    if (!speculativeLog.empty()) {
        maxSeq = std::max(maxSeq, speculativeLog.rbegin()->first);
    }
    return maxSeq;
}

void Entity::cachePrePrepare(int seq, const nlohmann::json& msg) {
    preprepareCache[seq] = msg;
}

void Entity::sendFillHole(int fromSeq, int toSeq, bool broadcast) {
    nlohmann::json fh{
        {"type","FillHole"},
        {"view", entityInfo["view"]},
        {"from_seq", fromSeq},
        {"to_seq", toSeq},
        {"message_sender_id", getNodeId()},
        {"broadcast", broadcast}
    };
    Message m(fh.dump());
    if (broadcast) {
        sendToAll(m);
        LOG_DEBUG("Broadcast FillHole request for [" << fromSeq << "," << toSeq << "]");
    } else {
        int primaryId = currentLeader();
        sendTo(primaryId, m);
        LOG_DEBUG("Sent FillHole to primary " << primaryId << " for [" << fromSeq << "," << toSeq << "]");
    }
    fillHolePending = true;
    fillHoleFromSeq = fromSeq;
    fillHoleToSeq = toSeq;
    fillHoleDeadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(fillHoleTimeoutMs);
}

void Entity::replayRangeTo(int fromSeq, int toSeq, int targetNodeId) {
    for (int s = fromSeq; s <= toSeq; ++s) {
        auto it = preprepareCache.find(s);
        if (it == preprepareCache.end()) continue;
        Message resend(it->second.dump());
        sendTo(targetNodeId, resend);
        LOG_DEBUG("Replay seq " << s << " to node " << targetNodeId);
    }
}

void Entity::tryHandleFillHoleTimeout() {
    if (!fillHolePending) return;
    if (std::chrono::steady_clock::now() < fillHoleDeadline) return;
    sendFillHole(fillHoleFromSeq, fillHoleToSeq, true);
    LOG_INFO("FillHole timeout -> initiating view change");
    fillHolePending = false;
    initiateViewChange();
}

void Entity::onTimeout() {
    std::lock_guard<std::recursive_mutex> engine(eventMtx);
    if (!timeKeeper) return;
    tryHandleFillHoleTimeout();
    if (fillHolePending) return;

    int newView = entityInfo["view"].get<int>() + 1;
    LOG_INFO("Timeout occurred (" << viewChangeTimeoutMs.load() << " ms): initiating view change to view "
             << newView);
    entityInfo["view"] = newView;
    if (agentClient_) agentClient_->recordViewChange();

    inViewChange = true;
    nlohmann::json viewChangeMsg;
    viewChangeMsg["type"] = "ViewChange";
    viewChangeMsg["new_view"] = newView;
    viewChangeMsg["view"] = newView; // for Zyzzyva handlers
    viewChangeMsg["message_sender_id"] = getNodeId();

    viewChangeMsg["prepare_messages"] = nlohmann::json::array();

    if (protocolName_ == "Zyzzyva") {
        viewChangeMsg["committed_seq"] = committedSeq;
        Message msg(viewChangeMsg.dump());
        sendTo(leaderForView(newView), msg);
        if (timeKeeper) timeKeeper->start();
        return;
    }

    if (protocolName_ == "Hotstuff") {
        int lastSeq = -1;
        std::string lastOp;
        nlohmann::json lastQC;

        std::string fileName = "messages_" + std::to_string(getNodeId()) + ".json";
        dataset.loadFromFile(fileName);
        auto records = dataset.getRecords();

        for (const auto& [key, record] : records) {
            if (record.contains("sequence") && record["sequence"].is_number_integer()) {
                int seq = record["sequence"].get<int>();
                if (seq > lastSeq) {
                    lastSeq = seq;
                    lastOp = record.value("operation", "");
                    if (record.contains("qc")) {
                        lastQC = record["qc"];
                    } else {
                        lastQC = nullptr;
                    }
                }
            }
        }

        viewChangeMsg["last_sequence"] = lastSeq;
        viewChangeMsg["last_operation"] = lastOp;
        viewChangeMsg["locked_qc"] = (lastSeq != -1 && sequenceStates.count(lastSeq))
            ? nlohmann::json(sequenceStates[lastSeq].getLockedQC())
            : nlohmann::json{};
        Message msg(viewChangeMsg.dump());
        sendTo(leaderForView(newView), msg);
    } else {
        Message msg(viewChangeMsg.dump());
        LOG_DEBUG("Broadcasting ViewChange for new view " << newView);
        sendToAll(msg);
    }

    if (timeKeeper) {
        timeKeeper->start();
    }
}

void Entity::sendNewViewToNextLeader() {
    int currentView = entityInfo["view"].get<int>();
    if (currentView == 0) currentView -= 1;
    currentView += 1;
    entityInfo["view"] = currentView;
    int nextLeader = leaderForView(currentView);

    // A lightweight PrePrepare envelope carries the NewView marker.
    bedrock::ProtocolEnvelope env;
    auto* m = env.mutable_pre_prepare();
    m->set_view(currentView);
    m->set_sequence(currentView);
    m->set_operation("NewViewforHotstuff");
    m->set_message_sender_id(getNodeId());
    m->set_type("NewViewforHotstuff");

    sendProtocolTo(nextLeader, env);
    LOG_DEBUG("Sent NewView (proto) to node " << nextLeader);
}

void Entity::start() {
    running = true;
    if (pbftCore_) requestTimer_.start();
    scheduler_.start();
    scheduleLeaderObservation();
    scheduleConsensusMaintenance();
    nextProposalTick_ = Clock::now() + std::chrono::milliseconds(options_.proposalIntervalMs);
    scheduleProposalTick();
    startGrpcServer();
    sender_.start();

    if (options_.agentPort > 0) {
        AgentClientConfig cfg = options_.agentConfig;
        cfg.nodeId = getNodeId();
        cfg.port = options_.agentPort;
        agentClient_ = std::make_unique<AgentClient>(
            cfg,
            makeProtocolAgentAdapter(protocolName_),
            AgentTimeouts{viewChangeTimeoutMs.load(), fastPathWaitMs.load()},
            [this](const AgentTimeouts& t) { applyAgentTimeouts(t); });
        agentClient_->start();
    }

    if (options_.statsIntervalMs > 0) {
        statsThread_ = std::thread(&Entity::statsLoop, this);
    }

    if (protocolName_ == "Hotstuff" || protocolName_ == "ChainedHotstuff" || protocolName_ == "Hotstuff2") {
        std::lock_guard<std::recursive_mutex> engine(eventMtx);
        sendNewViewToNextLeader();
    }
    LOG_INFO("replica " << getNodeId() << " started: protocol=" << protocolName_
             << " listen=" << committee_.replica(nodeId).address()
             << " leader=" << currentLeader()
             << " proposal_signing=" << (options_.proposalSigning ? "on" : "off")
             << " proposal_interval_ms=" << options_.proposalIntervalMs
             << " batch_max_requests=" << options_.batchMaxRequests
             << " batch_max_bytes=" << options_.batchMaxBytes
             << " max_inflight_batches=" << options_.maxInflightBatches);
}

void Entity::stop() {
    if (!running.exchange(false)) return;
    LOG_INFO("stopping replica " << getNodeId());
    if (agentClient_) {
        agentClient_->stop();
    }
    {
        std::lock_guard<std::mutex> lk(statsMtx_);
    }
    statsCv_.notify_all();
    if (statsThread_.joinable()) statsThread_.join();
    requestTimer_.stop();
    scheduler_.stop();
    {
        std::lock_guard<std::mutex> lk(timerMtx);
        if (timeKeeper) timeKeeper->stop();
    }
    stopGrpcServer();
    sender_.shutdown();
}

void Entity::statsLoop() {
    uint64_t lastCommitted = 0;
    uint64_t lastRequests = 0;
    uint64_t lastPropose = 0, lastValidate = 0, lastExecute = 0;
    uint64_t lastAdmit = 0, lastAssemble = 0, lastTickWait = 0, lastHandle = 0;
    uint64_t lastTicks = 0, lastBatches = 0;
    std::unique_lock<std::mutex> lk(statsMtx_);
    while (running) {
        if (statsCv_.wait_for(lk, std::chrono::milliseconds(options_.statsIntervalMs),
                              [this] { return !running.load(); })) {
            break;
        }
        const uint64_t committed = committedTotal_.load();
        const uint64_t requests = requestsReceived_.load();
        size_t inflight = 0;
        {
            std::lock_guard<std::mutex> pl(prePrepareMtx);
            inflight = prePrepareIndex.size();
        }
        int view = 0;
        int leader = 0;
        int executedSeq = 0;
        int checkpoint = 0;
        bool changing = false;
        std::size_t pending = 0;
        {
            std::lock_guard<std::recursive_mutex> engine(eventMtx);
            view = currentView();
            leader = currentLeader();
            executedSeq = lastExecuted_;
            checkpoint = stableCheckpoint_;
            changing = inViewChange;
            pending = pendingRequests_.size();
            if (pbftCore_) inflight = batchIndex_.size();
        }
        LOG_INFO("Stats view=" << view << " leader=" << leader << " view_changing=" << (changing ? 1 : 0)
                 << " executed_seq=" << executedSeq << " stable_checkpoint=" << checkpoint
                 << " committed_transactions=" << committedTransactions_.load()
                 << " rejected=" << rejectedRequests_.load()
                 << " committed_total=" << committed << " commits=" << (committed - lastCommitted)
                 << " requests=" << (requests - lastRequests) << " inflight=" << inflight
                 << " pending=" << pending << " timer_pending=" << requestTimer_.size()
                 << " view_changes=" << viewChangesStarted_.load() << " new_views=" << newViewsInstalled_.load()
                 << " fast_path=" << fastPathTotal_.load() << " slow_path=" << slowPathTotal_.load()
                 << " delayed_proposals=" << delayedProposals_.load()
                 << " relays_sent=" << relaysSent_.load()
                 << " relays_accepted=" << relaysAccepted_.load()
                 << " relayed_requests=" << relayedRequests_.load()
                 << " timer_expirations=" << requestTimer_.expirations() << " scheduled=" << scheduler_.pending()
                 << " clients=" << clientStreams_.size()
                 << " replies=" << repliesSent_.load() << " reply_drops=" << clientStreams_.droppedReplies()
                 << " sent=" << sender_.sent() << " send_inflight=" << sender_.inflight()
                 << " send_failures=" << sender_.failed()
                 << " propose_us=" << (proposeUs_.load() - lastPropose)
                 << " validate_us=" << (validateUs_.load() - lastValidate)
                 << " execute_us=" << (executeUs_.load() - lastExecute)
                 << " admit_us=" << (admitUs_.load() - lastAdmit)
                 << " assemble_us=" << (assembleUs_.load() - lastAssemble)
                 << " handle_us=" << (handleUs_.load() - lastHandle)
                 << " ticks=" << (proposalTicks_.load() - lastTicks)
                 << " batches=" << (batchesProposed_.load() - lastBatches)
                 << " tick_wait_us=" << (tickWaitUs_.load() - lastTickWait)
                 << " election_timeout_ms=" << viewChangeTimeoutMs.load()
                 << " slow_path_timeout_ms=" << fastPathWaitMs.load());
        lastCommitted = committed;
        lastRequests = requests;
        lastPropose = proposeUs_.load();
        lastValidate = validateUs_.load();
        lastExecute = executeUs_.load();
        lastAdmit = admitUs_.load();
        lastAssemble = assembleUs_.load();
        lastTickWait = tickWaitUs_.load();
        lastHandle = handleUs_.load();
        lastTicks = proposalTicks_.load();
        lastBatches = batchesProposed_.load();
    }
}

void Entity::loadProtocolConfig(const std::string& configFile) {
    if (!std::filesystem::exists(configFile)) {
        throw std::runtime_error("protocol config not found: " + configFile);
    }
    protocolConfig = YAML::LoadFile(configFile);
    if (!protocolConfig["protocol"]) {
        throw std::runtime_error("protocol config " + configFile + " has no 'protocol' key");
    }
    protocolName_ = protocolConfig["protocol"].as<std::string>();
    if (protocolConfig["timers"]) {
        const YAML::Node& timers = protocolConfig["timers"];
        if (timers["view_change_ms"]) viewChangeTimeoutMs = timers["view_change_ms"].as<int>();
        if (timers["fast_path_wait_ms"]) fastPathWaitMs = timers["fast_path_wait_ms"].as<int>();
    }
    const YAML::Node& phases = protocolConfig["phases"];
    if (!phases || !phases.IsMap()) {
        throw std::runtime_error("protocol config " + configFile + ": 'phases' must be a map");
    }
    for (const auto& phase_pair : phases) {
        const YAML::Node& phaseConfig = phase_pair.second;
        if (phaseConfig["actions"] && phaseConfig["actions"].IsSequence()) {
            for (const auto& actionNode : phaseConfig["actions"]) {
                std::string actionName;
                nlohmann::json params;

                if (actionNode.IsScalar()) {
                    actionName = actionNode.as<std::string>();
                } else if (actionNode.IsMap()) {
                    // Copy the nodes: yaml-cpp iterators hand out proxies, so
                    // a reference into it->second would dangle.
                    auto it = actionNode.begin();
                    actionName = it->first.as<std::string>();
                    YAML::Node paramNode = it->second;
                    for (const auto& param : paramNode) {
                        params[param.first.as<std::string>()] = param.second.as<std::string>();
                    }
                }
                std::unique_ptr<BaseEvent> event = EventFactory::getInstance().createEvent(actionName, params);
                if (!event) {
                    throw std::runtime_error("protocol config " + configFile + ": unknown action '" +
                                             actionName + "' in phase " + phase_pair.first.as<std::string>());
                }
                actions[actionName] = std::move(event);
            }
        }
    }
}

YAML::Node Entity::getPhaseConfigInsensitive(const std::string& phase) const {
    auto phases = protocolConfig["phases"];
    if (!phases || !phases.IsMap()) return YAML::Node();
    auto toLower = [](std::string s){ std::transform(s.begin(), s.end(), s.begin(), ::tolower); return s; };
    const std::string want = toLower(phase);
    for (auto it = phases.begin(); it != phases.end(); ++it) {
        std::string key = it->first.as<std::string>();
        if (toLower(key) == want) return it->second;
    }
    return YAML::Node();
}

void Entity::handleEvent(const Event* event, EntityState* context) {
    if (auto p = dynamic_cast<const ProtoMessage*>(event)) {
        try {
            if (pbftCore_) {
                handleConsensusEnvelope(p->envelope());
                return;
            }
            std::string messageType = p->explicit_type();
            LOG_DEBUG("Handling ProtoMessage of type: " << messageType << " seq=" << p->sequence()
                      << " from=" << p->sender_id());
            if (messageType.empty()) return;

            int seq = p->sequence();

            if (sequenceStates.find(seq) == sequenceStates.end()) {
                sequenceStates.emplace(
                    seq,
                    EntityState(getState().getRole(), "Request", getState().getViewNumber(), seq)
                );
            }
            sequenceStates[seq].setState(messageType);

            YAML::Node phaseConfig = getPhaseConfigInsensitive(messageType);
            if (phaseConfig && phaseConfig["actions"] && phaseConfig["actions"].IsSequence()) {
                bool actionsSucceeded = true;

                for (const auto& actionNode : phaseConfig["actions"]) {
                    std::string actionName;
                    nlohmann::json params;
                    if (actionNode.IsScalar()) {
                        actionName = actionNode.as<std::string>();
                    } else if (actionNode.IsMap()) {
                        auto it = actionNode.begin();
                        actionName = it->first.as<std::string>();
                        YAML::Node paramNode = it->second;
                        for (const auto& param : paramNode) {
                            params[param.first.as<std::string>()] = param.second.as<std::string>();
                        }
                    }
                    auto evt = EventFactory::getInstance().createEvent(actionName, params);
                    if (evt) {
                        const Message* msgPtr = dynamic_cast<const Message*>(event);
                        if (!evt->execute(this, msgPtr, &sequenceStates[seq])) {
                            actionsSucceeded = false;
                            break;
                        }
                    }
                }

                if (actionsSucceeded && phaseConfig["next_state"] && context) {
                    context->setState(phaseConfig["next_state"].as<std::string>());
                }
            } else {
                LOG_WARN("No phase configuration for message type " << messageType);
            }
        } catch (const std::exception& e) {
            LOG_ERROR("Typed handleEvent error: " << e.what());
        }
        return;
    }

    if (const Message* message = dynamic_cast<const Message*>(event)) {
        try {
            json j = json::parse(message->getContent());
            std::string messageType = j["type"].get<std::string>();
            if (messageType == "Request") requestsReceived_.fetch_add(1);
            if (pbftCore_ && messageType != "Request" && messageType != "QueryBalances") {
                handleConsensusControl(j);
                return;
            }

            if (messageType == "FillHole") {
                auto ev = EventFactory::getInstance().createEvent("fillHoleRequest");
                if (ev) ev->execute(this, message, &_entityState);
                return;
            }

            int seq = assignSequenceNumber();
            if (j.contains("sequence")) {
                seq = j["sequence"].get<int>();
            }

            if (messageType == "PiggybackBroadcast" && j.contains("piggyback") && j["piggyback"].is_array()) {
                for (const auto& piggyMsg : j["piggyback"]) {
                    Message protocolMsg(piggyMsg.dump());
                    handleEvent(&protocolMsg, context);
                }
                return;
            }

            YAML::Node phaseConfig = getPhaseConfig(messageType);
            if (phaseConfig && phaseConfig["actions"] && phaseConfig["actions"].IsSequence()) {
                bool actionsSucceeded = true;
                for (const auto& actionNode : phaseConfig["actions"]) {
                    std::string actionName;
                    nlohmann::json params;
                    if (actionNode.IsScalar()) {
                        actionName = actionNode.as<std::string>();
                    } else if (actionNode.IsMap()) {
                        auto it = actionNode.begin();
                        actionName = it->first.as<std::string>();
                        YAML::Node paramNode = it->second;
                        for (const auto& param : paramNode) {
                            params[param.first.as<std::string>()] = param.second.as<std::string>();
                        }
                    }
                    auto eventPtr = EventFactory::getInstance().createEvent(actionName, params);
                    if (eventPtr) {
                        bool shouldContinue = eventPtr->execute(this, message, &sequenceStates[seq]);
                        if (!shouldContinue) {
                            actionsSucceeded = false;
                            break;
                        }
                    }
                }
                if (actionsSucceeded && phaseConfig["next_state"] && context) {
                    std::string nextState = phaseConfig["next_state"].as<std::string>();
                    sequenceStates[seq].setState(nextState);
                }
            } else {
                LOG_WARN("No phase configuration for message type " << messageType);
            }

            if (j.contains("sequence")) {
                int s = j["sequence"].get<int>();
                if (sequenceStates.find(s) == sequenceStates.end()) {
                    sequenceStates.emplace(s, EntityState(getState().getRole(), "Request", getState().getViewNumber(), s));
                }
            }
        } catch (const json::exception& e) {
            LOG_ERROR("JSON parsing error: " << e.what());
        }
    }
}

void Entity::sendToAll(const Message& message) {
    for (int peer : peerIds_) {
        if (peer == nodeId) continue;
        sendTo(peer, message);
    }
}

void Entity::sendTo(int peer, const Message& message) {
    if (peer == nodeId) return;
    if (!committee_.contains(peer)) {
        LOG_ERROR("sendTo: unknown replica id " << peer);
        return;
    }
    const std::string content = message.getContent();
    if (options_.controlTransport) {
        options_.controlTransport(peer, content);
        return;
    }
    bedrock::ProtocolEnvelope env;
    if (buildEnvelopeFromJson(content, env)) {
        sender_.sendProtocol(peer, env);
    } else {
        sender_.sendRawJson(peer, content);
    }
}

EntityState& Entity::getState() { return _entityState; }
YAML::Node Entity::getPhaseConfig(const std::string& phase) const { return protocolConfig["phases"][phase]; }

void Entity::removeSequenceState(int seq) {
    sequenceStates.erase(seq);
    prePrepareMessages.erase(seq);
    prepareMessages.erase(seq);
    commitMessages.erase(seq);
    prePrepareOperations.erase(seq);
    prepareOperations.erase(seq);
    commitOperations.erase(seq);
}

bool Entity::hasProcessedOperation(int seq) const {
    std::lock_guard<std::mutex> g(processedMtx);
    if (pbftCore_) return seq > 0 && seq <= lastExecuted_;
    if (seq < pruneFloor_) return true;
    return processedOperations.find(seq) != processedOperations.end();
}

void Entity::noteFirstSeen(int seq, long long firstSeen) {
    std::lock_guard<std::mutex> lk(phaseTsMtx);
    firstSeenUs.emplace(seq, firstSeen);
}

void Entity::markOperationProcessed(int seq, int path, uint32_t transactions, uint32_t batchSize) {
    bool inserted = false;
    {
        std::lock_guard<std::mutex> g(processedMtx);
        if (seq < pruneFloor_) return;
        inserted = processedOperations.insert(seq).second;
    }
    if (!inserted) return;

    const uint64_t committedTotal = committedTotal_.fetch_add(1) + 1;
    committedTransactions_ += transactions;
    if (path == 1) fastPathTotal_.fetch_add(1);
    else if (path == 0) slowPathTotal_.fetch_add(1);
    LOG_DEBUG("Committed seq " << seq << (path == 1 ? " (fast path)" : path == 0 ? " (slow path)" : ""));

    const long long now = nowUs();
    long long e2eLatUs = -1;
    long long ppLatUs = -1, prLatUs = -1, coLatUs = -1;
    {
        std::lock_guard<std::mutex> lk(phaseTsMtx);
        auto it_fs = firstSeenUs.find(seq);
        if (it_fs != firstSeenUs.end() && now > it_fs->second) e2eLatUs = now - it_fs->second;
        // firstSeenUs holds the arrival of the oldest request this batch
        // carries, so the watermark only advances past a request once a batch
        // made entirely of later arrivals has executed. The election watchdog
        // reads it to tell starvation from being overtaken.
        if (it_fs != firstSeenUs.end()) {
            long long seen = lastExecutedArrivalUs_.load(std::memory_order_relaxed);
            while (it_fs->second > seen &&
                   !lastExecutedArrivalUs_.compare_exchange_weak(seen, it_fs->second,
                                                                 std::memory_order_relaxed)) {}
        }
        auto it_pp = phaseTs_preprepare.find(seq);
        auto it_pr = phaseTs_prepare.find(seq);
        auto it_co = phaseTs_commit.find(seq);
        if (it_pp != phaseTs_preprepare.end() && it_pr != phaseTs_prepare.end()) {
            long long ppUs = it_pr->second - it_pp->second;
            if (ppUs > 0) ppLatUs = ppUs;
        }
        if (it_pr != phaseTs_prepare.end() && it_co != phaseTs_commit.end()) {
            long long prUs = it_co->second - it_pr->second;
            if (prUs > 0) prLatUs = prUs;
        }
        if (it_co != phaseTs_commit.end()) {
            long long coUs = now - it_co->second;
            if (coUs > 0) coLatUs = coUs;
        }
        firstSeenUs.erase(seq);
        phaseTs_preprepare.erase(seq);
        phaseTs_prepare.erase(seq);
        phaseTs_commit.erase(seq);
    }

    if (agentClient_) {
        AgentConsensusSample sample;
        sample.sequence = seq > 0 ? static_cast<uint32_t>(seq) : 0;
        sample.latencyUs = e2eLatUs;
        sample.phase1Us = ppLatUs;
        sample.phase2Us = prLatUs;
        sample.phase3Us = coLatUs;
        sample.path = path;
        sample.transactions = transactions;
        sample.batchSize = batchSize;
        agentClient_->recordConsensus(sample);
    }

    if (seq > highestCommittedSeq_) highestCommittedSeq_ = seq;
    if (!pbftCore_ && committedTotal % kPruneEveryCommits == 0) {
        pruneSequenceState();
    }
}

// Releases per-sequence bookkeeping far behind the newest commit. Runs on
// the engine thread (markOperationProcessed is reached from handleEvent
// under eventMtx), so the unlocked maps are safe to touch here.
void Entity::pruneSequenceState() {
    const int floor = highestCommittedSeq_ - kRetainedSequences;
    if (floor <= pruneFloor_) return;

    sequenceStates.erase(sequenceStates.begin(), sequenceStates.lower_bound(floor));
    allMessagesBySeq.erase(allMessagesBySeq.begin(), allMessagesBySeq.lower_bound(floor));
    auto pruneIntMap = [floor](auto& m) {
        for (auto it = m.begin(); it != m.end();) {
            it = (it->first < floor) ? m.erase(it) : std::next(it);
        }
    };
    pruneIntMap(prePrepareMessages);
    pruneIntMap(prepareMessages);
    pruneIntMap(commitMessages);
    pruneIntMap(prePrepareOperations);
    pruneIntMap(prepareOperations);
    pruneIntMap(commitOperations);
    pruneIntMap(receivedMessages);
    pruneIntMap(preparePhaseTimerRunning);
    pruneIntMap(preprepareCache);
    // Bodies are keyed by digest, so they are dropped by the sequence they
    // were proposed at rather than by map order.
    for (auto it = bodiesByDigest_.begin(); it != bodiesByDigest_.end();)
        it = (it->second.sequence() < floor) ? bodiesByDigest_.erase(it) : std::next(it);
    for (auto it = executedRequestBySeq_.begin(); it != executedRequestBySeq_.end() && it->first < floor;) {
        executedRequests_.erase(it->second);
        it = executedRequestBySeq_.erase(it);
    }
    {
        std::lock_guard<std::mutex> lk(prePrepareMtx);
        pruneIntMap(prePrepareIndex);
    }
    {
        std::lock_guard<std::mutex> lk(phaseTsMtx);
        pruneIntMap(phaseTs_preprepare);
        pruneIntMap(phaseTs_prepare);
        pruneIntMap(phaseTs_commit);
        pruneIntMap(firstSeenUs);
    }
    {
        std::lock_guard<std::mutex> lk(senderIdsMtx);
        for (auto it = keyToSenderIds.begin(); it != keyToSenderIds.end();) {
            int s = 0;
            it = (keySequence(it->first, s) && s < floor) ? keyToSenderIds.erase(it) : std::next(it);
        }
    }
    {
        std::lock_guard<std::mutex> lk(quorumTriggeredMtx);
        for (auto it = quorumTriggered.begin(); it != quorumTriggered.end();) {
            int s = 0;
            it = (keySequence(*it, s) && s < floor) ? quorumTriggered.erase(it) : std::next(it);
        }
    }
    {
        std::lock_guard<std::mutex> g(processedMtx);
        processedOperations.erase(processedOperations.begin(), processedOperations.lower_bound(floor));
        pruneFloor_ = floor;
    }
    LOG_DEBUG("pruned sequence state below " << floor);
}

bool Entity::runVerification(const std::string& verifyType, const json& msg, EntityState* context) {
    if (verifyType == "none") return true;
    if (verifyType == "view_match") {
        int expected = context->getViewNumber();
        int actual = msg.contains("view") ? msg["view"].get<int>() : -1;
        if (!msg.contains("view") || actual != expected) {
            LOG_DEBUG("view_match failed: expected " << expected << ", got " << actual);
            return false;
        }
        return true;
    }
    return true;
}

void Entity::replyToClient(const std::string& clientId, uint64_t requestId, const std::string& result) {
    if (clientId.empty()) return;
    bedrock::ClientReply reply;
    reply.set_client_id(clientId);
    reply.set_request_id(requestId);
    reply.set_view(currentView());
    reply.set_replica_id(nodeId);
    reply.set_leader_id(currentLeader());
    reply.set_result(result);
    if (clientStreams_.reply(clientId, reply)) {
        repliesSent_.fetch_add(1);
    }
}

void Entity::applyAgentTimeouts(const AgentTimeouts& timeouts) {
    std::lock_guard<std::recursive_mutex> engine(eventMtx);
    if (timeouts.electionMs > 0) {
        viewChangeTimeoutMs = timeouts.electionMs;
    }
    if (timeouts.slowPathMs > 0) {
        fastPathWaitMs = timeouts.slowPathMs;
    }
    requestTimer_.timeoutChanged();
    if (viewChangeWaitSince_ && timeouts.electionMs > 0) scheduleViewChangeWait();
    LOG_INFO("applied timeouts: election_timeout_ms=" << viewChangeTimeoutMs.load()
             << " slow_path_timeout_ms=" << fastPathWaitMs.load());
}

void Entity::initiateViewChange() {
    LOG_INFO("Initiating view change.");
    if (agentClient_) {
        agentClient_->recordViewChange();
    }

    int newView = entityInfo["view"].get<int>() + 1;
    entityInfo["view"] = newView;

    inViewChange = true;
    nlohmann::json viewChangeMsg;
    viewChangeMsg["type"] = "ViewChange";
    viewChangeMsg["view"] = newView;
    viewChangeMsg["new_view"] = newView;
    viewChangeMsg["message_sender_id"] = getNodeId();
    viewChangeMsg["committed_seq"] = committedSeq;

    nlohmann::json speculativeLogArray = nlohmann::json::array();
    nlohmann::json prepareMsgs = nlohmann::json::array();
    {
        std::lock_guard<std::mutex> lock(speculativeLogMtx);
        for (const auto& [seq, entry] : speculativeLog) {
            if (seq > committedSeq) {
                nlohmann::json logEntry = {
                    {"sequence", seq},
                    {"txnId", entry.txnId},
                    {"from", entry.from},
                    {"to", entry.to},
                    {"amount", entry.amount}
                };
                speculativeLogArray.push_back(logEntry);

                nlohmann::json pm = {
                    {"sequence", seq},
                    {"timestamp", entry.txnId},
                    {"transaction", {
                        {"from", entry.from},
                        {"to", entry.to},
                        {"amount", entry.amount}
                    }},
                    {"operation", entry.txnId}
                };
                prepareMsgs.push_back(pm);
            }
        }
    }
    viewChangeMsg["speculative_log"] = speculativeLogArray;
    viewChangeMsg["prepare_messages"] = prepareMsgs;

    int nextLeader = leaderForView(newView);
    Message msg(viewChangeMsg.dump());
    sendTo(nextLeader, msg);
    LOG_DEBUG("Sent ViewChange(view=" << newView << ", committed_seq=" << committedSeq
              << ") to next leader (Node " << nextLeader << ").");
}

int Entity::allocateNextSequence() {
    std::lock_guard<std::mutex> g(clientRequestMtx);
    int seq = nextSequenceNumber.fetch_add(1) + 1; // start at 1
    int curr = entityInfo["sequence"].get<int>();
    if (seq > curr) entityInfo["sequence"] = seq;
    return seq;
}

bool Entity::processJsonFromGrpc(const std::string& jsonPayload) {
    try {
        std::lock_guard<std::recursive_mutex> lk(eventMtx);
        Message msg(jsonPayload);
        handleEvent(&msg, &_entityState);
        return true;
    } catch (const std::exception& e) {
        static std::atomic<uint64_t> failures{0};
        const uint64_t count = failures.fetch_add(1) + 1;
        if (count == 1 || count % 1000 == 0) {
            LOG_ERROR("processJsonFromGrpc failed (" << count << " so far): " << e.what());
        }
        return false;
    }
}

void Entity::startGrpcServer() {
    const auto& self = committee_.replica(nodeId);
    grpcSvc_ = std::make_unique<NodeServiceImpl>(*this);

    grpc::ServerBuilder builder;
    // Bind on all interfaces at the committee port so the address the peers
    // dial (the namespace IP) and loopback both work.
    std::string addr = "0.0.0.0:" + std::to_string(self.port);
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
    builder.SetMaxReceiveMessageSize(64 * 1024 * 1024);
    builder.SetMaxSendMessageSize(64 * 1024 * 1024);
    builder.RegisterService(grpcSvc_.get());
    grpcServer_ = builder.BuildAndStart();
    if (!grpcServer_) {
        throw std::runtime_error("failed to start gRPC server on " + addr);
    }
    grpcThread_ = std::thread([this, addr]{
        LOG_INFO("gRPC listening on " << addr);
        grpcServer_->Wait();
    });
}

void Entity::stopGrpcServer() {
    if (grpcServer_) {
        grpcServer_->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(2));
    }
    if (grpcThread_.joinable()) grpcThread_.join();
    grpcServer_.reset();
    grpcSvc_.reset();
}

void Entity::processProtocolEnvelope(const bedrock::ProtocolEnvelope& env) {
    std::lock_guard<std::recursive_mutex> lk(eventMtx);
    ProtoMessage pmsg(env);
    handleEvent(&pmsg, &_entityState);
}

void Entity::sendProtocolToAll(const bedrock::ProtocolEnvelope& env) {
    for (int peer : peerIds_) {
        if (peer == nodeId) continue;
        sendProtocolTo(peer, env);
    }
}

void Entity::sendProtocolTo(int peer, const bedrock::ProtocolEnvelope& env) {
    if (peer == nodeId) return;
    if (!committee_.contains(peer)) {
        LOG_ERROR("sendProtocolTo: unknown replica id " << peer);
        return;
    }
    if (options_.protocolTransport) options_.protocolTransport(peer, env);
    else sender_.sendProtocol(peer, env);
}
