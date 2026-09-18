#include "core/Entity.h"
#include "core/Log.h"
#include "core/crypto/CryptoUtils.h"
#include "core/crypto/OpenSSLCryptoProvider.h"
#include "core/events/ProtoMessage.h"
#include "test_support.h"
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <algorithm>
#include <deque>
#include <filesystem>
#include <fstream>
#include <random>
#include <sstream>
#include <unistd.h>

using json = nlohmann::json;
using bedrock::ProtocolEnvelope;
namespace fs = std::filesystem;

struct Fixture {
    fs::path root;
    Fixture() {
        root = fs::temp_directory_path() / ("bedrock-consensus-" + std::to_string(getpid()));
        fs::create_directories(root / "keys");
        for (int id = 0; id < 7; ++id) {
            EVP_PKEY_CTX* ctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr);
            CHECK(ctx); CHECK(EVP_PKEY_keygen_init(ctx) > 0); CHECK(EVP_PKEY_CTX_set_rsa_keygen_bits(ctx, 2048) > 0);
            EVP_PKEY* key = nullptr; CHECK(EVP_PKEY_keygen(ctx, &key) > 0);
            auto prefix = (root / "keys" / ("server_" + std::to_string(id))).string();
            FILE* file = fopen((prefix + "_private.pem").c_str(), "w"); CHECK(file);
            CHECK(PEM_write_PrivateKey(file, key, nullptr, nullptr, 0, nullptr, nullptr)); fclose(file);
            file = fopen((prefix + "_public.pem").c_str(), "w"); CHECK(file);
            CHECK(PEM_write_PUBKEY(file, key)); fclose(file);
            EVP_PKEY_free(key); EVP_PKEY_CTX_free(ctx);
        }
    }
    ~Fixture() { fs::remove_all(root); }
};

struct Packet {
    int from, to;
    ProtocolEnvelope envelope;
    std::string control;
    std::string type() const { return control.empty() ? ProtoMessage(envelope).explicit_type() : json::parse(control).value("type", ""); }
};

struct Network {
    Fixture& fixture;
    std::string protocol, domain;
    std::vector<std::unique_ptr<Entity>> nodes;
    std::vector<bool> active;
    std::deque<Packet> queue;
    std::vector<Packet> history;
    std::function<bool(const Packet&)> drop;
    Network(Fixture& fixture, std::string protocol, int n = 4,
            std::function<void(EntityOptions&)> configure = {}) : fixture(fixture), protocol(std::move(protocol)), active(n, true) {
        json committee{{"replicas", json::array()}};
        std::string keys;
        for (int id = 0; id < n; ++id) {
            committee["replicas"].push_back(json{{"id", id}, {"host", "127.0.0.1"}, {"port", 19000 + id}});
            std::ifstream key(fixture.root / "keys" / ("server_" + std::to_string(id) + "_public.pem"));
            std::ostringstream text; text << key.rdbuf(); keys += std::to_string(id) + ":" + text.str();
        }
        domain = "CppBedrock/consensus/v3/" + this->protocol + "/" + computeSHA256(keys) + "/";
        const auto path = fixture.root / ("committee_" + std::to_string(n) + ".json");
        std::ofstream(path) << committee.dump();
        for (int id = 0; id < n; ++id) {
            EntityOptions options; options.nodeId = id; options.committeePath = path.string();
            std::string lower = this->protocol; std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
            options.protocolConfigPath = std::string(BEDROCK_SOURCE_DIR) + "/config/config." + lower + ".yaml";
            options.keysDir = (fixture.root / "keys").string(); options.statsIntervalMs = 0;
            options.protocolTransport = [this, id](int to, const ProtocolEnvelope& env) { queue.push_back(Packet{id, to, env, {}}); };
            options.controlTransport = [this, id](int to, const std::string& control) { queue.push_back(Packet{id, to, {}, control}); };
            if (configure) configure(options);
            domain = "CppBedrock/consensus/v3/" + this->protocol + "/" + computeSHA256(keys) + "/" +
                std::to_string(options.batchMaxBytes) + "/";
            nodes.push_back(std::make_unique<Entity>(options));
        }
    }
    void sign(ProtocolEnvelope& env, int signer) {
        OpenSSLCryptoProvider crypto((fixture.root / "keys" / ("server_" + std::to_string(signer) + "_private.pem")).string());
        // Replicas sign the proposal header, never the batch body.
        env.clear_signature(); env.set_signature(crypto.sign(domain + "phase/" + bedrock::signedHeaderBytes(env)));
    }
    json sign(json msg, int signer) {
        OpenSSLCryptoProvider crypto((fixture.root / "keys" / ("server_" + std::to_string(signer) + "_private.pem")).string());
        msg.erase("signature"); msg["message_sender_id"] = signer;
        msg["signature"] = crypto.sign(domain + "control/" + msg.dump()); return msg;
    }
    void deliver(const Packet& packet) {
        if (!active[packet.to]) return;
        if (packet.control.empty()) nodes[packet.to]->processProtocolEnvelope(packet.envelope);
        else nodes[packet.to]->processJsonFromGrpc(packet.control);
    }
    void flush(bool shuffle = false) {
        std::mt19937 rng(7823);
        size_t count = 0;
        while (!queue.empty()) {
            CHECK(++count < 100000);
            auto it = queue.begin();
            if (shuffle) it += rng() % queue.size();
            Packet packet = *it; queue.erase(it); history.push_back(packet);
            if (!drop || !drop(packet)) deliver(packet);
        }
    }
    void request(int id, bool broadcast = true, bool tick = true) {
        const json req{{"type", "Request"}, {"client_id", "test-client"}, {"request_id", id},
            {"operation", "transfer"}, {"timestamp", std::to_string(id)},
            {"transaction", {{"from", "a"}, {"to", "b"}, {"amount", 1}}}};
        for (size_t i = 0; i < nodes.size(); ++i)
            if (active[i] && (broadcast || nodes[i]->isCurrentLeader())) nodes[i]->processJsonFromGrpc(req.dump());
        if (tick) for (auto& node : nodes) node->proposalTick();
    }
    void changeView(int view) {
        for (size_t i = 0; i < nodes.size(); ++i) if (active[i]) nodes[i]->startViewChange(view, "test");
        flush();
    }
    void slowPath() {
        for (size_t i = 0; i < nodes.size(); ++i) if (active[i] && nodes[i]->isCurrentLeader())
            nodes[i]->sbftDecide(nodes[i]->executedThrough() + 1, nodes[i]->currentView(), false);
        flush();
    }
    void maintain() {
        for (size_t i = 0; i < nodes.size(); ++i) if (active[i]) {
            nodes[i]->maintainConsensus();
            nodes[i]->processJsonFromGrpc(sign(json{{"type", "RecoveryRequest"}, {"after", nodes[3]->executedThrough()}}, 3).dump());
        }
        flush();
    }
    void checkExecuted(int count) {
        for (size_t i = 0; i < nodes.size(); ++i) if (active[i]) CHECK_EQ(nodes[i]->executedThrough(), count);
    }
};

void normalAndFaults(Fixture& f) {
    for (const std::string protocol : {"PBFT", "SBFT"}) for (int n : {4, 7}) {
        Network net(f, protocol, n);
        net.request(1); net.flush(true); net.checkExecuted(1);
        for (int id = n - (n - 1) / 3; id < n; ++id) net.active[id] = false;
        net.request(2); net.flush(true);
        if (protocol == "SBFT") { net.checkExecuted(1); net.slowPath(); }
        net.checkExecuted(2);
        net.changeView(1);
        net.request(3); net.flush(true);
        if (protocol == "SBFT") net.slowPath();
        net.checkExecuted(3);
    }
}

void reorderedPhases(Fixture& f) {
    Network net(f, "PBFT");
    std::vector<Packet> held;
    net.drop = [&](const Packet& p) {
        if (p.to == 3 && (p.type() == "PrePrepare" || p.type() == "prepare")) { held.push_back(p); return true; }
        return false;
    };
    net.request(1); net.flush();
    CHECK_EQ(net.nodes[3]->executedThrough(), 0);
    CHECK_EQ(net.nodes[0]->executedThrough(), 1);
    for (const auto& p : held) if (p.type() == "PrePrepare") net.deliver(p);
    CHECK_EQ(net.nodes[3]->executedThrough(), 0); // commits alone cannot replace local prepared state
    for (const auto& p : held) if (p.type() == "prepare") net.deliver(p);
    net.drop = {}; net.flush(); net.checkExecuted(1);

    Network ordered(f, "PBFT");
    std::vector<Packet> hole;
    ordered.drop = [&](const Packet& p) {
        if (p.control.empty() && ProtoMessage(p.envelope).sequence() == 1) { hole.push_back(p); return true; }
        return false;
    };
    ordered.request(1); ordered.request(2); ordered.flush(true);
    ordered.checkExecuted(0);
    for (const auto& node : ordered.nodes) { CHECK_EQ(node->stableCheckpoint(), 0); CHECK(node->balances.empty()); }
    ordered.drop = {};
    for (const auto& p : hole) ordered.deliver(p);
    ordered.flush(true); ordered.checkExecuted(2);
}

void authentication(Fixture& f) {
    Network net(f, "PBFT"); net.request(1);
    ProtocolEnvelope proposal;
    for (const auto& p : net.queue) if (p.envelope.has_pre_prepare()) { proposal = p.envelope; break; }
    CHECK(proposal.has_pre_prepare());
    net.nodes[1]->processProtocolEnvelope(proposal);
    auto conflicting = proposal;
    conflicting.mutable_pre_prepare()->mutable_requests(0)->mutable_transaction()->set_amount(70);
    conflicting.set_digest(bedrock::requestDigest(conflicting.pre_prepare())); net.sign(conflicting, 0);
    net.nodes[1]->processProtocolEnvelope(conflicting);
    CHECK_EQ(net.nodes[1]->executedThrough(), 0);
    auto nonleader = conflicting; nonleader.mutable_pre_prepare()->set_message_sender_id(2); net.sign(nonleader, 2);
    net.nodes[2]->processProtocolEnvelope(nonleader); CHECK(!net.nodes[2]->prePrepareIndex.count(1));
    auto corrupt = proposal; corrupt.mutable_pre_prepare()->mutable_requests(0)->mutable_transaction()->set_amount(71);
    net.nodes[2]->processProtocolEnvelope(corrupt); CHECK(!net.nodes[2]->prePrepareIndex.count(1));
    auto forgedVote = bedrock::voteEnvelope("commit", 0, 1, 99, proposal.digest()); net.sign(forgedVote, 0);
    net.nodes[1]->processProtocolEnvelope(forgedVote); CHECK_EQ(net.nodes[1]->executedThrough(), 0);
    net.flush(); net.checkExecuted(1);

    Network sbft(f, "SBFT"); sbft.request(1);
    for (const auto& p : sbft.queue) if (p.envelope.has_pre_prepare()) { proposal = p.envelope; break; }
    sbft.nodes[1]->processProtocolEnvelope(proposal);
    auto cert = bedrock::voteEnvelope("commit", 0, 1, 0, proposal.digest()); cert.mutable_commit()->set_fast_path(true);
    sbft.sign(cert, 0); sbft.nodes[1]->processProtocolEnvelope(cert); CHECK_EQ(sbft.nodes[1]->executedThrough(), 0);
    for (int id = 0; id < 4; ++id) {
        auto* share = cert.mutable_commit()->add_combined_messages();
        share->set_view(0); share->set_sequence(1); share->set_message_sender_id(id); share->set_digest(proposal.digest());
    }
    sbft.sign(cert, 0); sbft.nodes[1]->processProtocolEnvelope(cert); CHECK_EQ(sbft.nodes[1]->executedThrough(), 0);
    sbft.flush(); sbft.checkExecuted(1);
}

void viewChangeProofs(Fixture& f) {
    Network net(f, "PBFT");
    auto fake = net.sign(json{{"type", "NewView"}, {"new_view", 8}, {"low_watermark", 999999},
        {"view_changes", json::array()}, {"pre_prepares", json::array()}}, 0);
    net.nodes[2]->processJsonFromGrpc(fake.dump()); CHECK_EQ(net.nodes[2]->currentView(), 0);
    net.request(1); net.flush();
    std::vector<Packet> held;
    net.drop = [&](const Packet& p) {
        if (p.to == 3 && p.type() == "NewView") { held.push_back(p); return true; }
        return false;
    };
    net.changeView(1); CHECK(!held.empty()); CHECK(net.nodes[3]->inViewChange);
    auto nv = json::parse(held.front().control);
    auto changed = nv;
    // A re-proposal names its batch by digest. Redefining that digest must
    // be rejected however correctly the new leader signs it, since it no
    // longer matches what the view-change evidence carries forward.
    auto env = bedrock::decodeEnvelope(changed["pre_prepares"][0]);
    CHECK_EQ(env.pre_prepare().requests_size(), 0); // evidence carries headers only
    env.set_digest(computeSHA256("a different batch")); net.sign(env, 1);
    changed["pre_prepares"][0] = bedrock::encodeEnvelope(env); changed = net.sign(changed, 1);
    net.nodes[3]->processJsonFromGrpc(changed.dump()); CHECK(net.nodes[3]->inViewChange);
    auto duplicate = nv; duplicate["view_changes"][1] = duplicate["view_changes"][0]; duplicate = net.sign(duplicate, 1);
    net.nodes[3]->processJsonFromGrpc(duplicate.dump()); CHECK(net.nodes[3]->inViewChange);
    net.drop = {}; net.deliver(held.front()); net.flush(); CHECK(!net.nodes[3]->inViewChange);
    net.request(2); net.flush(); net.checkExecuted(2);
}

// A replica that lacks a decided batch fetches it, and takes the bytes only
// when they hash to the digest its own evidence names.
void batchFetch(Fixture& f) {
    for (const std::string protocol : {"PBFT", "SBFT"}) {
        Network net(f, protocol);
        // Replica 3 misses the proposal but sees the votes, so it learns the
        // decision without ever holding the batch.
        net.drop = [](const Packet& p) { return p.to == 3 && p.type() == "PrePrepare"; };
        net.request(1); net.flush();
        if (protocol == "SBFT") net.slowPath();
        CHECK_EQ(net.nodes[0]->executedThrough(), 1);
        CHECK_EQ(net.nodes[3]->executedThrough(), 0);

        // A body that does not hash to the decided digest is ignored.
        bedrock::ProtocolEnvelope forged;
        auto* batch = forged.mutable_pre_prepare();
        auto* request = batch->add_requests();
        request->set_client_id("test-client"); request->set_request_id(99);
        request->set_operation("transfer"); request->set_timestamp("99");
        auto* tx = request->mutable_transaction();
        tx->set_from("a"); tx->set_to("b"); tx->set_amount(500);
        net.nodes[3]->processJsonFromGrpc(net.sign(json{{"type", "BatchResponse"}, {"sequence", 1},
            {"digest", computeSHA256("unrelated")}, {"batch", bedrock::encodeEnvelope(forged)}}, 0).dump());
        CHECK_EQ(net.nodes[3]->executedThrough(), 0);
        CHECK_EQ(net.nodes[3]->balances.count("b"), 0u);

        // The genuine body, fetched from a peer, completes execution.
        net.drop = {}; net.maintain(); net.checkExecuted(1);
        for (const auto& node : net.nodes) CHECK_EQ(node->balances.at("a"), 99);
    }
}

void sbftViewChanges(Fixture& f) {
    // A fast certificate reaches only the old primary. The next quorum must
    // preserve the value using individual fast votes, without that certificate.
    Network net(f, "SBFT");
    net.drop = [](const Packet& p) { return p.type() == "commit"; };
    net.request(1); net.flush(); CHECK_EQ(net.nodes[0]->executedThrough(), 1);
    for (int id = 1; id < 4; ++id) CHECK_EQ(net.nodes[id]->executedThrough(), 0);
    net.active[0] = false; net.drop = {}; net.changeView(1); net.slowPath(); net.checkExecuted(1);
    for (int id = 1; id < 4; ++id) CHECK_EQ(net.nodes[id]->balances.at("a"), 99);

    Network returning(f, "SBFT"); returning.active[3] = false;
    returning.request(1); returning.flush(); CHECK_EQ(returning.nodes[0]->scheduler().pending(), 1u);
    returning.changeView(4); CHECK_EQ(returning.nodes[0]->scheduler().pending(), 1u);
    returning.slowPath(); returning.checkExecuted(1);
}

void checkpointRecovery(Fixture& f) {
    Network net(f, "PBFT"); net.active[3] = false;
    for (int id = 1; id <= bedrock::kCheckpointInterval + 1; ++id) { net.request(id); net.flush(); }
    CHECK_EQ(net.nodes[0]->stableCheckpoint(), bedrock::kCheckpointInterval);
    CHECK_EQ(net.nodes[3]->executedThrough(), 0); CHECK(!net.nodes[3]->hasProcessedOperation(1));
    net.changeView(1);
    Packet newView;
    for (const auto& p : net.history) if (p.to == 3 && p.type() == "NewView") newView = p;
    CHECK(!newView.control.empty());
    net.active[3] = true;
    net.deliver(newView);
    CHECK(net.nodes[3]->inViewChange);
    CHECK_EQ(net.nodes[3]->executedThrough(), 0); // must recover before voting
    // First reject a correctly signed response whose snapshot does not match
    // the certified digest, and a forged checkpoint with duplicate signers.
    std::vector<Packet> responses;
    net.drop = [&](const Packet& p) {
        if (p.to == 3 && p.type() == "RecoveryResponse") { responses.push_back(p); return true; }
        return p.to == 3 && p.type() == "NewView";
    };
    net.flush(); CHECK(!responses.empty());
    auto badState = json::parse(responses.front().control);
    badState["snapshot"]["balances"]["a"] = 123456;
    badState = net.sign(badState, responses.front().from);
    net.nodes[3]->processJsonFromGrpc(badState.dump()); CHECK_EQ(net.nodes[3]->executedThrough(), 0);
    auto badProof = json::parse(responses.front().control);
    badProof["checkpoint"]["votes"][1] = badProof["checkpoint"]["votes"][0];
    badProof = net.sign(badProof, responses.front().from);
    net.nodes[3]->processJsonFromGrpc(badProof.dump()); CHECK_EQ(net.nodes[3]->executedThrough(), 0);
    net.drop = {}; net.deliver(responses.front()); net.flush(); net.maintain();
    net.checkExecuted(bedrock::kCheckpointInterval + 1); CHECK(!net.nodes[3]->inViewChange);
    for (const auto& node : net.nodes) CHECK_EQ(node->balances.at("a"), 100 - bedrock::kCheckpointInterval - 1);
    // The checkpoint's replay table must survive both log pruning and transfer.
    net.request(1); net.flush(); net.checkExecuted(bedrock::kCheckpointInterval + 1);
    ProtocolEnvelope replay;
    for (const auto& p : net.history) if (p.envelope.has_pre_prepare() && p.envelope.pre_prepare().requests_size() && p.envelope.pre_prepare().requests(0).request_id() == 1) { replay = p.envelope; break; }
    replay.mutable_pre_prepare()->set_sequence(bedrock::kCheckpointInterval + 2);
    replay.mutable_pre_prepare()->set_view(1); replay.mutable_pre_prepare()->set_message_sender_id(1); net.sign(replay, 1);
    for (auto& node : net.nodes) node->processProtocolEnvelope(replay);
    net.flush(); net.checkExecuted(bedrock::kCheckpointInterval + 2);
    for (const auto& node : net.nodes) CHECK_EQ(node->balances.at("a"), 100 - bedrock::kCheckpointInterval - 1);
}

void lostMessages(Fixture& f) {
    for (const std::string protocol : {"PBFT", "SBFT"}) {
        Network net(f, protocol);
        net.drop = [](const Packet& p) { return p.to == 3 && p.control.empty(); };
        net.request(1); net.flush();
        if (protocol == "SBFT") net.slowPath();
        CHECK_EQ(net.nodes[0]->executedThrough(), 1); CHECK_EQ(net.nodes[3]->executedThrough(), 0);
        // A recovering replica must catch up even after it independently
        // starts a view change that the healthy replicas do not need to join.
        net.nodes[3]->startViewChange(1, "lost replies");
        net.drop = {}; net.maintain(); net.checkExecuted(1);

        Network unicast(f, protocol);
        unicast.drop = [](const Packet& p) { return p.to == 3 && p.type() == "PrePrepare"; };
        unicast.request(1, false); unicast.flush();
        if (protocol == "SBFT") unicast.slowPath();
        CHECK_EQ(unicast.nodes[0]->executedThrough(), 1);
        CHECK_EQ(unicast.nodes[3]->executedThrough(), 0);
        // A vote or certificate received before the proposal is sufficient
        // to detect a gap, even without a locally buffered client request.
        unicast.drop = {}; unicast.maintain(); unicast.checkExecuted(1);
    }
}

void batching(Fixture& fixture) {
    for (const std::string protocol : {"PBFT", "SBFT"}) {
        Network net(fixture, protocol, 4, [](auto& o) { o.batchMaxRequests = 2; });
        net.request(1, true, false); net.request(2, true, false); net.request(3, true, false);
        CHECK(net.queue.empty()); // Arrival never flushes, even at the count limit.
        net.nodes[0]->proposalTick();
        ProtocolEnvelope batch;
        for (const auto& packet : net.queue) if (packet.envelope.has_pre_prepare()) batch = packet.envelope;
        CHECK_EQ(batch.pre_prepare().requests_size(), 2);
        CHECK_EQ(batch.pre_prepare().requests(0).request_id(), 1u);
        CHECK_EQ(batch.pre_prepare().requests(1).request_id(), 2u);
        auto reordered = batch;
        reordered.mutable_pre_prepare()->mutable_requests()->SwapElements(0, 1);
        CHECK(bedrock::requestDigest(reordered.pre_prepare()) != batch.digest());
        // A signed proposal with duplicate request identities must be rejected.
        auto duplicate = batch;
        *duplicate.mutable_pre_prepare()->mutable_requests(1) = duplicate.pre_prepare().requests(0);
        duplicate.set_digest(bedrock::requestDigest(duplicate.pre_prepare())); net.sign(duplicate, 0);
        net.nodes[1]->processProtocolEnvelope(duplicate);
        CHECK_EQ(net.nodes[1]->executedThrough(), 0);
        net.flush(true); net.checkExecuted(1);
        for (const auto& node : net.nodes) CHECK_EQ(node->balances.at("a"), 98);
        net.nodes[0]->proposalTick(); net.flush(true); net.checkExecuted(2);
        for (const auto& node : net.nodes) CHECK_EQ(node->balances.at("a"), 97);
        net.nodes[0]->proposalTick(); CHECK(net.queue.empty()); // no empty cadence proposals
        net.request(2); net.flush(); net.checkExecuted(2); // duplicate in an earlier batch

        // Preserve the exact batch during a leader change with a missing replica.
        Network change(fixture, protocol);
        change.request(1, true, false); change.request(2, true, false);
        change.drop = [](const Packet& p) { return p.type() == "commit"; };
        change.nodes[0]->proposalTick(); change.flush();
        change.active[0] = false; change.drop = {};
        change.changeView(1);
        if (protocol == "SBFT") change.slowPath();
        change.checkExecuted(1);
        for (int id = 1; id < 4; ++id) CHECK_EQ(change.nodes[id]->balances.at("a"), 98);

        // A checkpoint transfers complete multi-request batches and their replay state.
        Network recovery(fixture, protocol); recovery.active[3] = false;
        for (int batchId = 0; batchId < bedrock::kCheckpointInterval + 1; ++batchId) {
            recovery.request(2 * batchId + 1, true, false);
            recovery.request(2 * batchId + 2, true, false);
            recovery.nodes[0]->proposalTick(); recovery.flush();
            if (protocol == "SBFT") recovery.slowPath();
        }
        recovery.active[3] = true; recovery.maintain();
        recovery.checkExecuted(bedrock::kCheckpointInterval + 1);
        for (const auto& node : recovery.nodes) CHECK_EQ(node->balances.at("a"), 100 - 2 * (bedrock::kCheckpointInterval + 1));
        recovery.request(1); recovery.flush(); recovery.checkExecuted(bedrock::kCheckpointInterval + 1);
    }

    Network limited(fixture, "PBFT", 4, [](auto& o) {
        o.batchMaxBytes = 60; o.maxInflightBatches = 1; o.maxPendingRequests = 3;
    });
    for (int id = 1; id <= 5; ++id) limited.request(id, false, false);
    CHECK_EQ(limited.nodes[0]->pendingRequestCount(), 3u);
    limited.nodes[0]->proposalTick();
    size_t proposals = limited.queue.size();
    limited.nodes[0]->proposalTick(); CHECK_EQ(limited.queue.size(), proposals); // pipeline full
    limited.flush(); limited.checkExecuted(1);
    CHECK_EQ(limited.nodes[0]->balances.at("a"), 99); // byte limit holds one request
    limited.nodes[0]->proposalTick(); limited.flush(); limited.checkExecuted(2);
    limited.nodes[0]->proposalTick(); limited.flush(); limited.checkExecuted(3);
    CHECK_EQ(limited.nodes[0]->balances.at("a"), 97);

    // Ingress itself is bounded before anything acquires the engine lock.
    Network ingress(fixture, "PBFT", 4, [](auto& o) { o.maxPendingRequests = 2; });
    for (int id = 1; id <= 3; ++id) {
        bedrock::ClientRequest r;
        r.set_client_id("ingress"); r.set_request_id(id); r.set_operation("transfer");
        r.mutable_transaction()->set_from("a"); r.mutable_transaction()->set_to("b");
        r.mutable_transaction()->set_amount(1);
        ingress.nodes[0]->submitClientRequest(r);
    }
    ingress.nodes[0]->proposalTick(); ingress.flush(); ingress.checkExecuted(1);
    for (const auto& node : ingress.nodes) CHECK_EQ(node->balances.at("a"), 98);
}

// Timer access only for deterministic protocol tests. No worker threads or
// listening sockets are started by Network.
struct EntityTimerTestAccess {
    static auto& timer(Entity& node) { return node.requestTimer_; }
    static auto& forwarded(Entity& node) { return node.forwardedRequests_; }
    static auto waitSince(Entity& node) { return node.viewChangeWaitSince_; }
    static auto waitGeneration(Entity& node) { return node.viewChangeWaitGeneration_; }
    static auto waitDuration(Entity& node) { return node.viewChangeWaitDuration(); }
    static auto acceptedAt(Entity& node, const std::string& key) { return node.pendingRequests_.at(key).acceptedAt; }
    static void requestExpiry(Entity& node, uint64_t generation) {
        node.running = true;
        node.onRequestTimerExpired(generation);
        node.running = false;
    }
    static void waitExpiry(Entity& node, uint64_t generation) {
        node.running = true;
        node.onViewChangeWaitExpired(generation);
        node.running = false;
    }
    static void ageWait(Entity& node) {
        node.viewChangeWaitSince_ = Entity::Clock::now() - node.viewChangeWaitDuration() - std::chrono::milliseconds(1);
    }
};

void broadcastAdmission(Fixture& fixture) {
    using Access = EntityTimerTestAccess;
    for (const std::string protocol : {"PBFT", "SBFT"}) {
        Network net(fixture, protocol, 4, [](auto& options) {
            options.batchMaxRequests = 1;
            options.maxPendingRequests = 1;
            options.initialElectionTimeoutMs = 4;
        });
        net.request(1, false, false); // Fill the leader, leaving backup pools empty.
        net.request(2, true, false);  // Leader rejects; every backup accepts.
        CHECK_EQ(net.nodes[0]->pendingRequestCount(), 1u);
        std::this_thread::sleep_for(std::chrono::milliseconds(3));
        for (int id = 1; id < 4; ++id) net.nodes[id]->proposalTick();
        net.flush();
        CHECK_EQ(Access::forwarded(*net.nodes[0]).size(), 3u);
        Packet relay;
        for (const auto& packet : net.history)
            if (packet.type() == "ForwardRequests") { relay = packet; break; }
        CHECK(!relay.control.empty());
        // Authentication and request-count validation precede reservation.
        Network invalid(fixture, protocol, 4, [](auto& o) { o.batchMaxRequests = 2; });
        auto forged = json::parse(relay.control);
        forged["signature"] = "invalid";
        invalid.nodes[0]->processJsonFromGrpc(forged.dump());
        CHECK(Access::forwarded(*invalid.nodes[0]).empty());
        auto oversized = bedrock::decodeEnvelope(json::parse(relay.control).at("requests"));
        const auto original = oversized.pre_prepare().requests(0);
        for (int id = 3; id <= 4; ++id) {
            auto* request = oversized.mutable_pre_prepare()->add_requests();
            *request = original; request->set_request_id(id);
        }
        forged["requests"] = bedrock::encodeEnvelope(oversized);
        invalid.nodes[0]->processJsonFromGrpc(invalid.sign(forged, relay.from).dump());
        CHECK(Access::forwarded(*invalid.nodes[0]).empty());
        net.deliver(relay); // One reserved batch per sender, including retransmissions.
        CHECK_EQ(Access::forwarded(*net.nodes[0]).size(), 3u);
        CHECK_EQ(net.nodes[0]->pendingRequestCount(), 1u); // Does not enlarge normal admission.
        net.nodes[0]->proposalTick(); net.flush(); net.checkExecuted(1);
        net.request(3, false, false); // Keep normal ingress busy.
        net.nodes[0]->proposalTick(); net.flush(); net.checkExecuted(2);
        for (int id = 1; id < 4; ++id) CHECK(!Access::timer(*net.nodes[id]).watch());
        net.nodes[0]->proposalTick(); net.flush(); net.checkExecuted(3);
        for (const auto& node : net.nodes) CHECK_EQ(node->balances.at("a"), 97);
        net.nodes[0]->proposalTick(); net.flush(); net.checkExecuted(3); // Relay copies cannot duplicate execution.
        net.changeView(1);
        auto old = json::parse(relay.control);
        // Replay the old-view relay to the new leader with a valid signature.
        old["message_sender_id"] = 2;
        net.nodes[1]->processJsonFromGrpc(net.sign(old, 2).dump());
        CHECK(Access::forwarded(*net.nodes[1]).empty());
    }
}

void watchedRequestTimers(Fixture& fixture) {
    using Access = EntityTimerTestAccess;
    using namespace std::chrono_literals;
    for (const std::string protocol : {"PBFT", "SBFT"}) {
        Network batch(fixture, protocol, 4, [](auto& o) { o.batchMaxRequests = 2; });
        batch.request(30, true, false); batch.request(10, true, false); batch.request(20, true, false);
        const auto before = *Access::timer(*batch.nodes[0]).watch();
        const auto arrival = Access::acceptedAt(*batch.nodes[0], "test-client/20");
        batch.nodes[0]->proposalTick(); batch.flush(); batch.checkExecuted(1);
        auto after = *Access::timer(*batch.nodes[0]).watch();
        CHECK_EQ(after.key, std::string("test-client/20"));
        CHECK_EQ(after.generation, before.generation + 1);
        CHECK(after.since >= before.since);
        CHECK(Access::acceptedAt(*batch.nodes[0], after.key) == arrival);
        Access::requestExpiry(*batch.nodes[0], before.generation);
        CHECK_EQ(batch.nodes[0]->currentView(), 0);
        batch.nodes[0]->proposalTick(); batch.flush(); batch.checkExecuted(2);
        CHECK(!Access::timer(*batch.nodes[0]).watch());

        // A follower watches an accepted sequence even with leader-only client traffic.
        Network unicast(fixture, protocol);
        unicast.request(1, false);
        for (const auto& packet : unicast.queue)
            if (packet.to == 3 && packet.envelope.has_pre_prepare()) { unicast.deliver(packet); break; }
        CHECK_EQ(Access::timer(*unicast.nodes[3]).watch()->key, std::string("seq:1"));
        unicast.flush(); unicast.checkExecuted(1);
        CHECK(!Access::timer(*unicast.nodes[3]).watch());

        // Only replica 3 knows this request. The leader commits other requests
        // successfully, but that progress must not postpone replica 3's watch.
        Network skipped(fixture, protocol);
        const json request{{"type", "Request"}, {"client_id", "ignored"}, {"request_id", 1},
            {"operation", "transfer"}, {"timestamp", "1"},
            {"transaction", {{"from", "a"}, {"to", "b"}, {"amount", 1}}}};
        auto& follower = *skipped.nodes[3];
        follower.processJsonFromGrpc(request.dump());
        auto watched = *Access::timer(follower).watch();
        skipped.request(1, false); skipped.flush(); skipped.checkExecuted(1);
        CHECK(Access::timer(follower).watch()->deadline == watched.deadline);
        CHECK_EQ(Access::timer(follower).watch()->generation, watched.generation);
        AgentTimeouts shorter; shorter.electionMs = 1;
        follower.applyAgentTimeouts(shorter);
        std::this_thread::sleep_for(3ms);
        Access::requestExpiry(follower, Access::timer(follower).watch()->generation);
        CHECK_EQ(follower.currentView(), 1);
        CHECK(follower.inViewChange);
        CHECK(!Access::timer(follower).watch());
        CHECK(!Access::waitSince(follower)); // no NewView countdown until a quorum joins
        skipped.request(2, true, false);
        CHECK(!Access::timer(follower).watch()); // arrivals cannot arm a request watch during view change

        // Hold NewView so quorum-gated waiting and its independent deadline
        // can be checked before installation.
        Network change(fixture, protocol);
        change.request(30, true, false); change.request(10, true, false);
        auto& node = *change.nodes[3];
        const auto accepted = Access::acceptedAt(node, "test-client/30");
        std::vector<Packet> held;
        change.drop = [&](const Packet& packet) {
            if (packet.type() != "NewView") return false;
            held.push_back(packet); return true;
        };
        change.changeView(1);
        CHECK(Access::waitSince(node));
        CHECK(!Access::timer(node).watch());
        const auto waitStart = *Access::waitSince(node);
        const auto oldGeneration = Access::waitGeneration(node);
        AgentTimeouts longer; longer.electionMs = 5000;
        node.applyAgentTimeouts(longer);
        CHECK(*Access::waitSince(node) == waitStart);
        Access::ageWait(node);
        Access::waitExpiry(node, oldGeneration); // superseded recommendation
        CHECK_EQ(node.currentView(), 1);
        CHECK(!held.empty());
        const auto generation = Access::waitGeneration(node);
        for (const auto& packet : held) if (packet.to == 3) { change.deliver(packet); break; }
        CHECK(!node.inViewChange);
        CHECK(!Access::waitSince(node));
        CHECK_EQ(Access::timer(node).watch()->key, std::string("test-client/30"));
        CHECK(Access::acceptedAt(node, "test-client/30") == accepted);
        Access::waitExpiry(node, generation); // already installed view
        CHECK_EQ(node.currentView(), 1);

        // Expiry of a quorum-backed NewView wait advances the view and doubles
        // the next wait. An isolated replica cannot keep advancing alone.
        auto& waiting = *change.nodes[2];
        CHECK(Access::waitSince(waiting));
        const auto duration = Access::waitDuration(waiting);
        Access::ageWait(waiting);
        Access::waitExpiry(waiting, Access::waitGeneration(waiting));
        CHECK_EQ(waiting.currentView(), 2);
        CHECK(Access::waitDuration(waiting) == duration * 2);
        CHECK(!Access::waitSince(waiting));
        change.drop = {}; change.changeView(2);
        change.request(77); change.flush();
        CHECK(Access::waitDuration(waiting) == duration); // normal execution resets backoff

        // Installing an unrelated checkpoint must preserve the ignored head's
        // watch, even as recovered requests and sequence entries are removed.
        Network recovery(fixture, protocol);
        auto& recovering = *recovery.nodes[3];
        recovering.processJsonFromGrpc(request.dump());
        const auto recoveryWatch = *Access::timer(recovering).watch();
        recovery.active[3] = false;
        for (int id = 1; id <= bedrock::kCheckpointInterval + 1; ++id) {
            recovery.request(id); recovery.flush();
            if (protocol == "SBFT") recovery.slowPath();
        }
        recovery.active[3] = true; recovery.maintain();
        recovery.checkExecuted(bedrock::kCheckpointInterval + 1);
        CHECK_EQ(Access::timer(recovering).size(), 1u);
        CHECK(Access::timer(recovering).watch()->deadline == recoveryWatch.deadline);
        CHECK_EQ(Access::timer(recovering).watch()->generation, recoveryWatch.generation);

        // A valid NewView can require checkpoint recovery before installation.
        // Retransmission cannot keep extending that independent deadline.
        Network checkpoint(fixture, protocol); checkpoint.active[3] = false;
        for (int id = 1; id <= bedrock::kCheckpointInterval + 1; ++id) {
            checkpoint.request(id); checkpoint.flush();
            if (protocol == "SBFT") checkpoint.slowPath();
        }
        checkpoint.changeView(1);
        Packet newView;
        for (const auto& packet : checkpoint.history)
            if (packet.to == 3 && packet.type() == "NewView") newView = packet;
        CHECK(!newView.control.empty());
        checkpoint.active[3] = true; checkpoint.deliver(newView);
        auto& behind = *checkpoint.nodes[3];
        CHECK(behind.inViewChange); CHECK(Access::waitSince(behind));
        const auto recoveryWait = *Access::waitSince(behind);
        const auto recoveryGeneration = Access::waitGeneration(behind);
        checkpoint.deliver(newView);
        CHECK(*Access::waitSince(behind) == recoveryWait);
        CHECK_EQ(Access::waitGeneration(behind), recoveryGeneration);
        checkpoint.flush(); checkpoint.maintain();
        CHECK(!behind.inViewChange); CHECK(!Access::waitSince(behind));
        CHECK(!Access::timer(behind).watch());
        Access::waitExpiry(behind, recoveryGeneration);
        CHECK_EQ(behind.currentView(), 1);
    }
}

void replayIntervals() {
    bedrock::ExecutedRequests seen;
    CHECK(seen.insert("client/with/slash/1").second);
    CHECK(seen.insert("client/with/slash/3").second);
    CHECK(!seen.count("client/with/slash/2"));
    CHECK(seen.insert("client/with/slash/2").second);
    CHECK(!seen.insert("client/with/slash/3").second);
    CHECK(seen.insert("other/18446744073709551615").second);
    auto restored = bedrock::ExecutedRequests::restore(json::parse(seen.snapshot().dump()));
    CHECK(restored.count("other/18446744073709551615"));
    CHECK_EQ(restored.snapshot().at("client/with/slash").size(), 1u);
    restored.erase("client/with/slash/2");
    CHECK(!restored.count("client/with/slash/2"));
    CHECK(restored.count("client/with/slash/1")); CHECK(restored.count("client/with/slash/3"));
    CHECK(restored.insert("client/with/slash/2").second);
    CHECK_EQ(restored.snapshot(), seen.snapshot());
}

int main() {
    bedrock::logging::setLevel(bedrock::logging::Level::Error);
    Fixture fixture;
    replayIntervals();
    batching(fixture);
    watchedRequestTimers(fixture);
    broadcastAdmission(fixture);
    normalAndFaults(fixture);
    reorderedPhases(fixture);
    authentication(fixture);
    viewChangeProofs(fixture);
    batchFetch(fixture);
    sbftViewChanges(fixture);
    checkpointRecovery(fixture);
    lostMessages(fixture);
    return testPassed("consensus_test");
}
