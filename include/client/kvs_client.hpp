//  Copyright 2019 U.C. Berkeley RISE Lab
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

#ifndef INCLUDE_ASYNC_CLIENT_HPP_
#define INCLUDE_ASYNC_CLIENT_HPP_

#include "anna.pb.h"
#include "common.hpp"
#include "requests.hpp"
#include "threads.hpp"
#include "types.hpp"

class State;



using TimePoint = std::chrono::time_point<std::chrono::system_clock>;

struct PendingRequest {
    TimePoint tp_;
    Address worker_addr_;
    KeyRequest request_;
};

class KvsClientInterface {
public:
    virtual string put_async(const Key& key, const string& payload,
                             LatticeType lattice_type) = 0;
    virtual void get_async(const Key& key) = 0;
    virtual vector<KeyResponse> receive_async() = 0;
    virtual zmq::context_t* get_context() = 0;
    virtual set<string> vget_async(const map<Key,Address> keys, unsigned long long lastSeen, bool secondRound, State * state, const string tx_id) = 0;
    virtual map<Key,Address>  get_worker_thread_multi(const set<Key> keys) = 0;
    virtual set<string> vput_async(const map<Key,Address> keys, const map<Key,string> values,
                           LatticeType lattice_type) = 0;
    virtual string get_request_id() = 0;
    virtual set<string> prepare_async(const map<Key,Address> keys, const map<Key,string> values, const string tx_id,
                              LatticeType lattice_type, State * state) = 0;
    virtual set<string> commit_async(const map<Key,Address> keys, unsigned long long commitTime, const string tx_id, State * state) = 0;
    virtual set<Key> get_missing_worker_threads_multi(const set<Key> keys) = 0;
    virtual set<Address> get_all_worker_threads(const Key& key, State* state) = 0;
    virtual set<Address> get_all_worker_threads(const Key& key) = 0;

    virtual string readTx(const set<Key> keys, unsigned long long t_low,unsigned long long t_high) = 0;
    virtual void writeTx(const map<Key,string> keys_values) = 0;
    virtual set<string> vget_async_snapshot(const map<Key,Address> keys, map<Key, unsigned long long> key_t_low, unsigned long long t_high, unsigned long long lastSeen, State* state, const string tx_id) = 0;
    virtual string readSliceTx(const set<Key> keys, map<Key, unsigned long long> key_t_low, unsigned long long t_high, unsigned long long lastSeen) = 0;

};



#endif  // INCLUDE_ASYNC_CLIENT_HPP_
