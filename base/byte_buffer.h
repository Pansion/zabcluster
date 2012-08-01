/*
 * byte_buffer.h
 * Copyright 2004--2005, Google Inc.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  1. Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *  2. Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *  3. The name of the author may not be used to endorse or promote products
 *     derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
 * OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef BYTE_BUFFER_H_
#define BYTE_BUFFER_H_

#include <string>
#include "basictypes.h"

extern "C" {
#include <arpa/inet.h>

  inline uint16 HostToNetwork16(uint16 n) {
    return htons(n);
  }

  inline uint32 HostToNetwork32(uint32 n) {
    return htonl(n);
  }

  inline uint64 HostToNetwork64(uint64 n) {
    return hnwap64(n);
  }
  inline uint16 NetworkToHost16(uint16 n) {
    return ntohs(n);
  }

  inline uint32 NetworkToHost32(uint32 n) {
    return ntohl(n);
  }

  inline uint64 NetworkToHost64(uint64 n) {
    return nhwap64(n);
  }
}
template<class T> inline T _min(T a, T b) {
  return (a > b) ? b : a;
}
template<class T> inline T _max(T a, T b) {
  return (a < b) ? b : a;
}

class ByteBuffer {
  public:
    ByteBuffer();
    ByteBuffer(const char* bytes, size_t len);
    ByteBuffer(const char* bytes); // uses strlen
    ~ByteBuffer();

    const char* Data() const {
      return bytes_ + start_;
    }
    size_t Length() {
      return end_ - start_;
    }
    size_t Length() const {
      return end_ - start_;
    }
    size_t Capacity() {
      return size_ - start_;
    }

    bool ReadUInt8(uint8& val);
    bool ReadUInt16(uint16& val);
    bool ReadUInt32(uint32& val);
    bool ReadUInt64(uint64& val);

    bool ReadInt8(int8& val);
    bool ReadInt16(int16& val);
    bool ReadInt32(int32& val);
    bool ReadInt64(int64& val);

    bool ReadString(std::string& val, size_t len); // append to val
    bool ReadBytes(char* val, size_t len);

    void WriteUInt8(uint8 val);
    void WriteUInt16(uint16 val);
    void WriteUInt32(uint32 val);
    void WriteUInt64(uint64 val);

    void WriteInt8(int8 val);
    void WriteInt16(int16 val);
    void WriteInt32(int32 val);
    void WriteInt64(int64 val);

    void WriteString(const std::string& val);
    void WriteBytes(const char* val, size_t len);

    void Resize(size_t size);
    void Shift(size_t size);

  private:
    char* bytes_;
    size_t size_;
    size_t start_;
    size_t end_;

    DISALLOW_COPY_AND_ASSIGN(ByteBuffer);
};

#endif /* BYTE_BUFFER_HXX_ */
