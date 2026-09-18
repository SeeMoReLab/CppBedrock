#include "core/FailureSpec.h"
#include "core/Log.h"

#include <libxml/parser.h>
#include <libxml/tree.h>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <fstream>
#include <optional>
#include <sstream>
#include <stdexcept>

namespace bedrock {

namespace {

using ms = std::chrono::milliseconds;

constexpr const char* kLeaderToken = "leader";

std::string trim(const std::string& s) {
    std::size_t b = 0;
    std::size_t e = s.size();
    while (b < e && std::isspace(static_cast<unsigned char>(s[b]))) ++b;
    while (e > b && std::isspace(static_cast<unsigned char>(s[e - 1]))) --e;
    return s.substr(b, e - b);
}

bool equalsIgnoreCase(const std::string& a, const std::string& b) {
    if (a.size() != b.size()) return false;
    for (std::size_t i = 0; i < a.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(a[i])) != std::tolower(static_cast<unsigned char>(b[i]))) return false;
    }
    return true;
}

bool isElement(const xmlNode* node, const char* name) {
    return node && node->type == XML_ELEMENT_NODE && xmlStrcmp(node->name, reinterpret_cast<const xmlChar*>(name)) == 0;
}

const xmlNode* child(const xmlNode* parent, const char* name) {
    if (!parent) return nullptr;
    for (const xmlNode* c = parent->children; c; c = c->next) {
        if (isElement(c, name)) return c;
    }
    return nullptr;
}

std::optional<std::string> childText(const xmlNode* parent, const char* name) {
    const xmlNode* c = child(parent, name);
    if (!c) return std::nullopt;
    xmlChar* raw = xmlNodeGetContent(c);
    std::string text = raw ? reinterpret_cast<const char*>(raw) : "";
    xmlFree(raw);
    return trim(text);
}

std::optional<std::int64_t> childInt(const xmlNode* parent, const char* name) {
    auto text = childText(parent, name);
    if (!text) return std::nullopt;
    try {
        std::size_t consumed = 0;
        const long long v = std::stoll(*text, &consumed);
        if (consumed != text->size()) throw std::invalid_argument("trailing characters");
        return static_cast<std::int64_t>(v);
    } catch (const std::exception&) {
        throw std::runtime_error(std::string("failure spec: <") + name + "> is not an integer: '" + *text + "'");
    }
}

std::optional<double> childFloat(const xmlNode* parent, const char* name) {
    auto text = childText(parent, name);
    if (!text) return std::nullopt;
    try {
        std::size_t consumed = 0;
        const double v = std::stod(*text, &consumed);
        if (consumed != text->size()) throw std::invalid_argument("trailing characters");
        return v;
    } catch (const std::exception&) {
        throw std::runtime_error(std::string("failure spec: <") + name + "> is not a number: '" + *text + "'");
    }
}

ms nonNegativeMs(std::int64_t v) {
    return ms(std::max<std::int64_t>(0, v));
}

ms nonNegativeSecs(double v) {
    if (!(v > 0.0)) return ms(0);
    return ms(static_cast<std::int64_t>(std::llround(v * 1000.0)));
}

ms phaseStart(const xmlNode* phase) {
    if (auto v = childInt(phase, "startAtMs")) return nonNegativeMs(*v);
    if (auto v = childInt(phase, "atTimeMs")) return nonNegativeMs(*v);
    if (auto v = childFloat(phase, "startAt")) return nonNegativeSecs(*v);
    if (auto v = childFloat(phase, "atTime")) return nonNegativeSecs(*v);
    if (auto v = childFloat(phase, "time")) return nonNegativeSecs(*v);
    return ms(0);
}

int parseReplicaId(const std::string& text) {
    try {
        std::size_t consumed = 0;
        const long v = std::stol(text, &consumed);
        if (consumed != text.size() || v < 0) throw std::invalid_argument("not a replica id");
        return static_cast<int>(v);
    } catch (const std::exception&) {
        throw std::runtime_error("failure spec: invalid replica id '" + text + "' in proposalDelay");
    }
}

// proposalDelay may be null (section or element absent): the rule is empty.
ProposalDelayController::Rule parseRule(const xmlNode* proposalDelay) {
    ProposalDelayController::Rule rule;
    if (!proposalDelay) return rule;
    std::optional<ms> defaultDelay;
    if (auto v = childInt(proposalDelay, "delayMs")) defaultDelay = nonNegativeMs(*v);
    const xmlNode* replicas = child(proposalDelay, "replicas");

    bool sawReplicaEntry = false;
    if (replicas) {
        for (const xmlNode* r = replicas->children; r; r = r->next) {
            if (!isElement(r, "replica")) continue;
            sawReplicaEntry = true;
            const std::string idText = childText(r, "id").value_or("");
            if (idText.empty()) continue;
            std::optional<ms> delay;
            if (auto v = childInt(r, "delayMs")) delay = nonNegativeMs(*v);
            else delay = defaultDelay;
            if (!delay) continue;
            if (equalsIgnoreCase(idText, kLeaderToken)) {
                rule.leaderWindowDelay = *delay;
                rule.hasLeaderWindowRule = true;
                continue;
            }
            rule.replicaDelays[parseReplicaId(idText)] = *delay;
        }
    }
    if (sawReplicaEntry || !defaultDelay || !replicas) return rule;

    for (const xmlNode* idNode = replicas->children; idNode; idNode = idNode->next) {
        if (!isElement(idNode, "id")) continue;
        xmlChar* raw = xmlNodeGetContent(idNode);
        const std::string idText = trim(raw ? reinterpret_cast<const char*>(raw) : "");
        xmlFree(raw);
        if (idText.empty()) continue;
        if (equalsIgnoreCase(idText, kLeaderToken)) {
            rule.leaderWindowDelay = *defaultDelay;
            rule.hasLeaderWindowRule = true;
            continue;
        }
        rule.replicaDelays[parseReplicaId(idText)] = *defaultDelay;
    }
    return rule;
}

void appendPhases(std::vector<ProposalDelayController::Phase>& out, ms start, std::size_t order,
                  const xmlNode* proposalDelay, std::optional<ms> nextStart) {
    ProposalDelayController::Rule rule = parseRule(proposalDelay);
    ms interval(0);
    if (proposalDelay) {
        if (auto v = childInt(proposalDelay, "intervalMs")) interval = nonNegativeMs(*v);
        else if (auto v = childFloat(proposalDelay, "interval")) interval = nonNegativeSecs(*v);
    }
    if (interval == ms(0)) {
        out.push_back({start, ms(0), order, rule});
        return;
    }
    std::size_t count = 0;
    if (proposalDelay) {
        if (auto v = childInt(proposalDelay, "count"); v && *v > 0) count = static_cast<std::size_t>(*v);
    }
    std::optional<ms> maxStart;
    if (nextStart && *nextStart > start) maxStart = nextStart;

    std::size_t added = 0;
    ms phaseStart = start;
    while (true) {
        if (maxStart && phaseStart >= *maxStart) break;
        if (count > 0 && added >= count) break;
        out.push_back({phaseStart, interval, order, rule});
        ++added;
        if (!maxStart && count == 0) break;
        phaseStart += interval;
    }
    if (added == 0) out.push_back({start, ms(0), order, rule});
}

struct XmlDoc {
    xmlDocPtr doc{nullptr};
    ~XmlDoc() { if (doc) xmlFreeDoc(doc); }
};

}  // namespace

std::uint64_t ProposalDelayController::nowUnixMs() {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<ms>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

std::unique_ptr<ProposalDelayController> ProposalDelayController::loadFile(const std::string& path,
                                                                           std::uint64_t startUnixMs,
                                                                           const std::string& section) {
    std::ifstream in(path);
    if (!in) throw std::runtime_error("failed to read failure spec " + path);
    std::stringstream buf;
    buf << in.rdbuf();
    return parse(buf.str(), startUnixMs, section);
}

std::unique_ptr<ProposalDelayController> ProposalDelayController::parse(const std::string& xml,
                                                                        std::uint64_t startUnixMs,
                                                                        const std::string& section) {
    static std::once_flag initOnce;
    std::call_once(initOnce, [] { xmlInitParser(); });

    XmlDoc holder;
    holder.doc = xmlReadMemory(xml.data(), static_cast<int>(xml.size()), "failure_spec.xml", nullptr,
                               XML_PARSE_NOBLANKS | XML_PARSE_NONET | XML_PARSE_NOERROR | XML_PARSE_NOWARNING);
    if (!holder.doc) throw std::runtime_error("failed to parse failure spec XML");
    const xmlNode* root = xmlDocGetRootElement(holder.doc);
    if (!isElement(root, "failureSpec")) throw std::runtime_error("failure spec: root element must be <failureSpec>");

    auto ctrl = std::make_unique<ProposalDelayController>();
    ctrl->enabled_ = true;
    ctrl->startUnixMs_ = startUnixMs;
    if (auto v = childInt(root, "warmUpTimeMs")) ctrl->warmUp_ = nonNegativeMs(*v);
    else if (auto v = childFloat(root, "warmUpTime")) ctrl->warmUp_ = nonNegativeSecs(*v);

    struct Raw {
        ms start;
        std::size_t order;
        const xmlNode* proposalDelay;
    };
    std::vector<Raw> raw;
    if (const xmlNode* phases = child(root, "phases")) {
        std::size_t order = 0;
        for (const xmlNode* p = phases->children; p; p = p->next) {
            if (!isElement(p, "phase")) continue;
            const xmlNode* sec = child(p, section.c_str());
            raw.push_back({phaseStart(p), order++, child(sec, "proposalDelay")});
        }
    }
    std::stable_sort(raw.begin(), raw.end(), [](const Raw& a, const Raw& b) {
        return a.start != b.start ? a.start < b.start : a.order < b.order;
    });
    for (std::size_t i = 0; i < raw.size(); ++i) {
        std::optional<ms> next;
        if (i + 1 < raw.size()) next = raw[i + 1].start;
        appendPhases(ctrl->phases_, raw[i].start, raw[i].order, raw[i].proposalDelay, next);
    }
    std::stable_sort(ctrl->phases_.begin(), ctrl->phases_.end(), [](const Phase& a, const Phase& b) {
        return a.startOffset != b.startOffset ? a.startOffset < b.startOffset : a.order < b.order;
    });
    return ctrl;
}

bool ProposalDelayController::elapsedSinceWarmup(ms& out) const {
    const std::uint64_t now = nowUnixMs();
    if (now < startUnixMs_) return false;
    const ms sinceStart(static_cast<std::int64_t>(now - startUnixMs_));
    if (sinceStart < warmUp_) return false;
    out = sinceStart - warmUp_;
    return true;
}

long ProposalDelayController::activePhase(bool haveElapsed, ms elapsed) const {
    if (!haveElapsed) return -1;
    long active = -1;
    for (std::size_t i = 0; i < phases_.size(); ++i) {
        if (elapsed >= phases_[i].startOffset) active = static_cast<long>(i);
        else break;
    }
    return active;
}

std::int64_t ProposalDelayController::pinKey(long phase, ms elapsed, std::int64_t& tick) const {
    tick = 0;
    if (phase < 0 || static_cast<std::size_t>(phase) >= phases_.size()) return phase;
    const Phase& p = phases_[static_cast<std::size_t>(phase)];
    if (p.interval > ms(0) && elapsed >= p.startOffset) {
        tick = (elapsed - p.startOffset).count() / p.interval.count();
    }
    return (static_cast<std::int64_t>(phase) << 32) | tick;
}

void ProposalDelayController::logPhaseChange(long phase) {
    long previous = lastLoggedPhase_.load();
    if (previous == phase) return;
    if (!lastLoggedPhase_.compare_exchange_strong(previous, phase)) return;
    if (phase < 0) {
        LOG_INFO("proposal-delay injection phase changed: inactive");
    } else {
        const Phase& p = phases_[static_cast<std::size_t>(phase)];
        LOG_INFO("proposal-delay injection phase changed: index=" << phase
                 << " start_offset_ms=" << p.startOffset.count()
                 << " interval_ms=" << p.interval.count()
                 << " leader_window=" << (p.rule.hasLeaderWindowRule ? p.rule.leaderWindowDelay.count() : 0)
                 << "ms explicit_replicas=" << p.rule.replicaDelays.size());
    }
}

void ProposalDelayController::observeLeader(int leader, const std::vector<int>& replicaIds) {
    if (!enabled_) return;
    ms elapsed(0);
    const bool have = elapsedSinceWarmup(elapsed);
    const long phase = activePhase(have, elapsed);
    logPhaseChange(phase);
    if (phase < 0) return;
    std::int64_t tick = 0;
    const std::int64_t key = pinKey(phase, elapsed, tick);
    pinLeaderWindow(phase, key, tick, leader, replicaIds);
}

std::chrono::milliseconds ProposalDelayController::pinnedDelay(std::int64_t key, int replicaId) const {
    std::lock_guard<std::mutex> lk(mtx_);
    auto it = pinned_.find(key);
    if (it == pinned_.end()) return ms(0);
    auto r = it->second.find(replicaId);
    return r == it->second.end() ? ms(0) : r->second;
}

std::chrono::milliseconds ProposalDelayController::delayFor(int replicaId) {
    if (!enabled_) return ms(0);
    ms elapsed(0);
    const bool have = elapsedSinceWarmup(elapsed);
    const long phase = activePhase(have, elapsed);
    logPhaseChange(phase);
    if (phase < 0) return ms(0);
    const Rule& rule = phases_[static_cast<std::size_t>(phase)].rule;
    auto explicitDelay = rule.replicaDelays.find(replicaId);
    if (explicitDelay != rule.replicaDelays.end()) return explicitDelay->second;
    if (!rule.hasLeaderWindowRule) return ms(0);
    std::int64_t tick = 0;
    return pinnedDelay(pinKey(phase, elapsed, tick), replicaId);
}

std::chrono::milliseconds ProposalDelayController::delayForProposal(int replicaId, int leader,
                                                                    const std::vector<int>& replicaIds) {
    if (!enabled_) return ms(0);
    observeLeader(leader, replicaIds);
    return delayFor(replicaId);
}

void ProposalDelayController::pinLeaderWindow(long phase, std::int64_t key, std::int64_t tick, int leader,
                                              const std::vector<int>& replicaIds) {
    std::lock_guard<std::mutex> lk(mtx_);
    auto existing = pinned_.find(key);
    if (existing != pinned_.end() && !existing->second.empty()) return;
    if (phase < 0 || static_cast<std::size_t>(phase) >= phases_.size()) return;
    const Phase& p = phases_[static_cast<std::size_t>(phase)];
    if (!p.rule.hasLeaderWindowRule) return;

    std::vector<int> nodes = replicaIds;
    std::sort(nodes.begin(), nodes.end());
    auto pos = std::find(nodes.begin(), nodes.end(), leader);
    if (pos == nodes.end() || nodes.empty()) return;
    const std::size_t leaderPos = static_cast<std::size_t>(pos - nodes.begin());
    const std::size_t windowSize = std::min((nodes.size() - 1) / 3, nodes.size());

    std::map<int, ms> resolved;
    std::ostringstream targets;
    for (std::size_t offset = 0; offset < windowSize; ++offset) {
        const int target = nodes[(leaderPos + offset) % nodes.size()];
        resolved[target] = p.rule.leaderWindowDelay;
        targets << (offset ? "," : "") << target;
    }
    pinned_[key] = std::move(resolved);
    LOG_INFO("resolved leader proposal delay window: phase=" << p.order << " interval_tick=" << tick
             << " leader_replica=" << leader << " delay_ms=" << p.rule.leaderWindowDelay.count()
             << " targets=[" << targets.str() << "]");
}

}  // namespace bedrock
