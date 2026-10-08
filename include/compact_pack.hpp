// SPDX-License-Identifier: GPL-3.0-or-later
// EdgePlus - Modern Industrial Edge Data Collector & Gateway Engine
// Copyright (C) 2026 EdgePlus Contributors. All rights reserved.

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
#include <unordered_map>
#include <stdexcept>
#include <cstring>
#include <zlib.h>

#if defined(_WIN32)
#include <winsock2.h>
#else
#include <arpa/inet.h>
#endif

namespace edgeplus {

/**
 * CompactPack: 纯净高效的工业 KLV (Key-Length-Value) 嵌套二进制序列化容器
 * 
 * 特点:
 * 1. 结构:
 *    [uint16 key_len][key bytes][uint32 val_len][val bytes]
 * 2. 纯文本/二进制无歧义表达:
 *    无 JSON 的引号、花括号、转义符语法噪音，Key 和 Value 紧凑排列。
 * 3. 递归嵌套 (Pack 套 Pack):
 *    Sub-pack 序列化为字节流存入父 Pack 的 Value 中，实现任意层级嵌套。
 * 4. 内置 zlib 压缩:
 *    超高压缩比，大幅节省工业网络带宽。
 * 5. 网络传输帧封装:
 *    [uint32 total_len][uint8 flags][payload (raw or gzip)]
 *    flags: 0x00=Raw, 0x01=GzipCompressed
 */
class CompactPack {
public:
    enum Flag : uint8_t {
        FLAG_RAW = 0x00,
        FLAG_GZIP = 0x01
    };

    CompactPack() = default;

    // --- 写入接口 ---

    void Put(std::string_view key, std::string_view val) {
        fields_[std::string(key)] = std::vector<uint8_t>(val.begin(), val.end());
    }

    void Put(std::string_view key, const std::vector<uint8_t>& val) {
        fields_[std::string(key)] = val;
    }

    void Put(std::string_view key, const void* data, size_t len) {
        const auto* p = static_cast<const uint8_t*>(data);
        fields_[std::string(key)] = std::vector<uint8_t>(p, p + len);
    }

    void PutInt64(std::string_view key, int64_t val) {
        Put(key, std::to_string(val));
    }

    void PutDouble(std::string_view key, double val) {
        Put(key, std::to_string(val));
    }

    // 嵌套 Pack (Pack 套 Pack)
    void PutPack(std::string_view key, const CompactPack& sub_pack) {
        auto bytes = sub_pack.Serialize();
        fields_[std::string(key)] = std::move(bytes);
    }

    // --- 读取接口 ---

    bool Has(std::string_view key) const {
        return fields_.find(std::string(key)) != fields_.end();
    }

    std::string GetString(std::string_view key, const std::string& default_val = "") const {
        auto it = fields_.find(std::string(key));
        if (it == fields_.end()) return default_val;
        return std::string(reinterpret_cast<const char*>(it->second.data()), it->second.size());
    }

    int64_t GetInt64(std::string_view key, int64_t default_val = 0) const {
        std::string s = GetString(key);
        if (s.empty()) return default_val;
        try {
            return std::stoll(s);
        } catch (...) {
            return default_val;
        }
    }

    double GetDouble(std::string_view key, double default_val = 0.0) const {
        std::string s = GetString(key);
        if (s.empty()) return default_val;
        try {
            return std::stod(s);
        } catch (...) {
            return default_val;
        }
    }

    const std::vector<uint8_t>* GetBytes(std::string_view key) const {
        auto it = fields_.find(std::string(key));
        if (it == fields_.end()) return nullptr;
        return &it->second;
    }

    // 读取嵌套 Pack
    bool GetPack(std::string_view key, CompactPack& out_pack) const {
        const auto* bytes = GetBytes(key);
        if (!bytes) return false;
        return out_pack.Deserialize(bytes->data(), bytes->size());
    }

    const std::unordered_map<std::string, std::vector<uint8_t>>& GetAllFields() const {
        return fields_;
    }

    void Clear() {
        fields_.clear();
    }

    size_t Size() const {
        return fields_.size();
    }

    // --- 序列化与反序列化 ---

    // 序列化为 KLV 字节流
    std::vector<uint8_t> Serialize() const {
        std::vector<uint8_t> buf;
        for (const auto& [k, v] : fields_) {
            uint16_t klen = htons(static_cast<uint16_t>(k.size()));
            const auto* kp = reinterpret_cast<const uint8_t*>(&klen);
            buf.insert(buf.end(), kp, kp + 2);
            buf.insert(buf.end(), k.begin(), k.end());

            uint32_t vlen = htonl(static_cast<uint32_t>(v.size()));
            const auto* vp = reinterpret_cast<const uint8_t*>(&vlen);
            buf.insert(buf.end(), vp, vp + 4);
            buf.insert(buf.end(), v.begin(), v.end());
        }
        return buf;
    }

    // 从 KLV 字节流解析
    bool Deserialize(const uint8_t* data, size_t size) {
        fields_.clear();
        size_t offset = 0;
        while (offset + 6 <= size) { // 至少有 2字节key_len + 4字节val_len
            uint16_t klen_net;
            std::memcpy(&klen_net, data + offset, 2);
            uint16_t klen = ntohs(klen_net);
            offset += 2;

            if (offset + klen > size) return false;
            std::string key(reinterpret_cast<const char*>(data + offset), klen);
            offset += klen;

            if (offset + 4 > size) return false;
            uint32_t vlen_net;
            std::memcpy(&vlen_net, data + offset, 4);
            uint32_t vlen = ntohl(vlen_net);
            offset += 4;

            if (offset + vlen > size) return false;
            std::vector<uint8_t> val(data + offset, data + offset + vlen);
            offset += vlen;

            fields_[std::move(key)] = std::move(val);
        }
        return offset == size;
    }

    // --- 网络传输帧打包 (带 Gzip 压缩与 5 字节帧头) ---

    // 帧格式: [uint32 total_payload_len (网络序)][uint8 flags][payload]
    std::vector<uint8_t> PackFrame(bool compress = true) const {
        auto raw_bytes = Serialize();
        uint8_t flags = FLAG_RAW;
        std::vector<uint8_t> payload;

        if (compress && raw_bytes.size() > 64) {
            auto comp = GzipCompress(raw_bytes.data(), raw_bytes.size());
            if (!comp.empty() && comp.size() < raw_bytes.size()) {
                payload = std::move(comp);
                flags = FLAG_GZIP;
            } else {
                payload = std::move(raw_bytes);
            }
        } else {
            payload = std::move(raw_bytes);
        }

        uint32_t total_len = htonl(static_cast<uint32_t>(1 + payload.size()));
        std::vector<uint8_t> frame(4 + 1 + payload.size());
        std::memcpy(frame.data(), &total_len, 4);
        frame[4] = flags;
        std::memcpy(frame.data() + 5, payload.data(), payload.size());
        return frame;
    }

    // 从网络帧负载解析 (去掉 4 字节 total_len 之后，以 flags 为开头的缓冲区)
    bool UnpackFramePayload(const uint8_t* payload_with_flag, size_t total_payload_len) {
        if (total_payload_len < 1) return false;
        uint8_t flags = payload_with_flag[0];
        const uint8_t* body = payload_with_flag + 1;
        size_t body_len = total_payload_len - 1;

        if (flags & FLAG_GZIP) {
            auto decomp = GzipDecompress(body, body_len);
            if (decomp.empty() && body_len > 0) return false;
            return Deserialize(decomp.data(), decomp.size());
        } else {
            return Deserialize(body, body_len);
        }
    }

    // --- zlib 压缩工具 ---

    static std::vector<uint8_t> GzipCompress(const uint8_t* src, size_t src_len) {
        if (!src || src_len == 0) return {};
        z_stream strm{};
        // 15 + 16 为 gzip 格式封装
        if (deflateInit2(&strm, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 15 + 16, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
            return {};
        }

        strm.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(src));
        strm.avail_in = static_cast<uInt>(src_len);

        std::vector<uint8_t> out;
        out.resize(deflateBound(&strm, static_cast<uLong>(src_len)) + 32);

        strm.next_out = reinterpret_cast<Bytef*>(out.data());
        strm.avail_out = static_cast<uInt>(out.size());

        int ret = deflate(&strm, Z_FINISH);
        if (ret != Z_STREAM_END) {
            deflateEnd(&strm);
            return {};
        }

        out.resize(strm.total_out);
        deflateEnd(&strm);
        return out;
    }

    static std::vector<uint8_t> GzipDecompress(const uint8_t* src, size_t src_len) {
        if (!src || src_len == 0) return {};
        z_stream strm{};
        // 32 + 15 允许自动识别 zlib 或 gzip 报头
        if (inflateInit2(&strm, 32 + 15) != Z_OK) {
            return {};
        }

        strm.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(src));
        strm.avail_in = static_cast<uInt>(src_len);

        std::vector<uint8_t> out;
        std::vector<uint8_t> chunk(16384);

        int ret = Z_OK;
        while (ret == Z_OK) {
            strm.next_out = reinterpret_cast<Bytef*>(chunk.data());
            strm.avail_out = static_cast<uInt>(chunk.size());

            ret = inflate(&strm, Z_NO_FLUSH);
            if (ret != Z_OK && ret != Z_STREAM_END) {
                inflateEnd(&strm);
                return {};
            }

            size_t produced = chunk.size() - strm.avail_out;
            out.insert(out.end(), chunk.begin(), chunk.begin() + produced);
        }

        inflateEnd(&strm);
        return out;
    }

private:
    std::unordered_map<std::string, std::vector<uint8_t>> fields_;
};

} // namespace edgeplus
