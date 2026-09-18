#include "core/net/ClientStreams.h"
#include "core/Log.h"

namespace bedrock {

void ClientStreams::close(const std::shared_ptr<Stream>& stream) {
    {
        std::lock_guard<std::mutex> lock(stream->writeMtx);
        stream->closed = true;
        stream->queue.clear();
        stream->context->TryCancel();
    }
    stream->cv.notify_one();
    if (stream->worker.joinable()) stream->worker.join();
}

void ClientStreams::attach(const std::string& clientId, Writer* writer, grpc::ServerContext* context) {
    auto stream = std::make_shared<Stream>();
    stream->writer = writer;
    stream->context = context;
    stream->worker = std::thread([this, stream] {
        for (;;) {
            ClientReply reply;
            {
                std::unique_lock<std::mutex> lock(stream->writeMtx);
                stream->cv.wait(lock, [&] { return stream->closed || !stream->queue.empty(); });
                if (stream->closed) return;
                // At most 1 ms of transport coalescing, independent of the
                // proposal cadence. All entries retain per-request semantics.
                stream->cv.wait_for(lock, std::chrono::milliseconds(1), [&] {
                    return stream->closed || stream->queue.size() >= 128;
                });
                if (stream->closed) return;
                reply = std::move(stream->queue.front());
                stream->queue.pop_front();
                reply.add_request_ids(reply.request_id());
                while (!stream->queue.empty() && reply.request_ids_size() < 128) {
                    const auto& next = stream->queue.front();
                    if (next.result() != reply.result() || next.view() != reply.view() ||
                        next.leader_id() != reply.leader_id()) break;
                    reply.add_request_ids(next.request_id());
                    stream->queue.pop_front();
                }
                reply.set_request_id(0);
            }
            if (!stream->writer->Write(reply)) {
                std::lock_guard<std::mutex> lock(stream->writeMtx);
                dropped_ += stream->queue.size() + 1;
                stream->queue.clear();
                stream->closed = true;
                stream->context->TryCancel();
                return;
            }
        }
    });
    std::shared_ptr<Stream> previous;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto& entry = streams_[clientId];
        previous = std::move(entry);
        entry = stream;
        attached_[writer] = stream;
    }
    if (previous) {
        // The old RPC handler owns joining its writer, avoiding concurrent
        // joins when reconnect races with detach.
        std::lock_guard<std::mutex> lock(previous->writeMtx);
        if (!previous->closed) {
            previous->closed = true;
            previous->context->TryCancel();
        }
        previous->cv.notify_one();
    }
}

void ClientStreams::detach(const std::string& clientId, Writer* writer) {
    std::shared_ptr<Stream> stream;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto owned = attached_.find(writer);
        if (owned == attached_.end()) return;
        stream = owned->second;
        attached_.erase(owned);
        auto it = streams_.find(clientId);
        if (it != streams_.end() && it->second->writer == writer) streams_.erase(it);
    }
    close(stream);
}

bool ClientStreams::reply(const std::string& clientId, const ClientReply& reply) {
    std::shared_ptr<Stream> stream;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = streams_.find(clientId);
        if (it == streams_.end()) { ++dropped_; return false; }
        stream = it->second;
    }
    {
        std::lock_guard<std::mutex> lock(stream->writeMtx);
        if (stream->closed) { ++dropped_; return false; }
        if (stream->queue.size() >= 32768) {
            dropped_ += stream->queue.size() + 1;
            stream->queue.clear(); stream->closed = true;
            stream->context->TryCancel(); stream->cv.notify_one();
            LOG_WARN("client reply queue full; closing stream " << clientId);
            return false;
        }
        stream->queue.push_back(reply);
    }
    stream->cv.notify_one();
    return true;
}

size_t ClientStreams::reply(const std::string& clientId, std::vector<ClientReply>& replies) {
    if (replies.empty()) return 0;
    std::shared_ptr<Stream> stream;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = streams_.find(clientId);
        if (it == streams_.end()) { dropped_ += replies.size(); return 0; }
        stream = it->second;
    }
    const size_t count = replies.size();
    {
        std::lock_guard<std::mutex> lock(stream->writeMtx);
        if (stream->closed) { dropped_ += count; return 0; }
        if (stream->queue.size() + count >= 32768) {
            dropped_ += stream->queue.size() + count;
            stream->queue.clear(); stream->closed = true;
            stream->context->TryCancel(); stream->cv.notify_one();
            LOG_WARN("client reply queue full; closing stream " << clientId);
            return 0;
        }
        for (auto& reply : replies) stream->queue.push_back(std::move(reply));
    }
    stream->cv.notify_one();
    return count;
}

size_t ClientStreams::size() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return streams_.size();
}
} // namespace bedrock
