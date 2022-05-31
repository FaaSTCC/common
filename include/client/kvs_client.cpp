//  Copyright 2019 U.C. Berkeley RISE Lab
//  Copyright 2021 Taras Lykhenko, Rafael Soares
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

#include "transaction.cpp"
#include "state.hpp"


class KvsClient : public KvsClientInterface {
public:
  /**
   * @addrs A vector of routing addresses.
   * @routing_thread_count The number of thread sone ach routing node
   * @ip My node's IP address
   * @tid My client's thread ID
   * @timeout Length of request timeouts in ms
   */
  KvsClient(vector<UserRoutingThread> routing_threads, string ip,
            unsigned tid = 0, unsigned timeout = 10000) :
      routing_threads_(routing_threads),
      ut_(UserThread(ip, tid)),
      context_(zmq::context_t(1)),
      socket_cache_(SocketCache(&context_, ZMQ_PUSH)),
      key_address_puller_(zmq::socket_t(context_, ZMQ_PULL)),
      response_puller_(zmq::socket_t(context_, ZMQ_PULL)),
      log_(spdlog::basic_logger_mt("client_log", "client_log.txt", true)),
      timeout_(timeout) {
    // initialize logger
    log_->flush_on(spdlog::level::info);

    std::hash<string> hasher;
    seed_ = time(NULL);
    seed_ += hasher(ip);
    seed_ += tid;
    log_->info("Random seed is {}.", seed_);

    // bind the two sockets we listen on
    key_address_puller_.bind(ut_.key_address_bind_address());
    response_puller_.bind(ut_.response_bind_address());

    pollitems_ = {
        {static_cast<void*>(key_address_puller_), 0, ZMQ_POLLIN, 0},
        {static_cast<void*>(response_puller_), 0, ZMQ_POLLIN, 0},
    };

    // set the request ID to 0
    rid_ = 0;
  }

  ~KvsClient() {}

public:
  /**
   * Issue an async PUT request to the KVS for a certain lattice typed value.
   */
  string put_async(const Key& key, const string& payload,
                   LatticeType lattice_type) {
    KeyRequest request;
    KeyTuple* tuple = prepare_data_request(request, key);
    request.set_type(RequestType::PUT);
    tuple->set_lattice_type(lattice_type);
    tuple->set_payload(payload);

    try_request(request);
    return request.request_id();
  }



  /**
   * Issue an async GET request to the KVS.
   */
  void get_async(const Key& key) {
    // we issue GET only when it is not in the pending map
    if (pending_get_response_map_.find(key) ==
        pending_get_response_map_.end()) {
      KeyRequest request;
      prepare_data_request(request, key);
      request.set_type(RequestType::GET);

      try_request(request);
    }
  }

  string readTx(const set<Key> keys, unsigned long long t_low,unsigned long long t_high){
    ReadTransaction * rt = new ReadTransaction(keys, this, t_low, t_high);
    return rt->getTxID();
  }

  string readSliceTx(const set<Key> keys, map<Key, unsigned long long> key_t_low, unsigned long long t_high, unsigned long long lastSeen){
    ReadSliceTransaction * rt = new ReadSliceTransaction(keys, this, key_t_low, t_high, lastSeen);
    return rt->getTxID();
  }

  void writeTx(const map<Key,string> keys_values){
    WriteTransaction * rt = new WriteTransaction(keys_values,this);
  }

  set<string> vget_async(const map<Key,Address> keys, unsigned long long lastSeen, bool secondRound, State* state, const string tx_id) {
    // to simplify the process, or to jump the check of pending_get_response_map_
    //if (pending_get_response_map_.find(keys[0])
    //			== pending_get_response_map_.end()) {
    map<Address, KeyRequest> addressRequest;
    map<Key,Address> keysCopy(keys);
    map<Key,Address>::iterator itKeys;
    set<string> result;
    for ( itKeys = keysCopy.begin(); itKeys!= keysCopy.end(); itKeys++ ) {
      auto key = itKeys->first;
      auto address = itKeys->second;
      if(addressRequest.find(address) == addressRequest.end()){
        KeyRequest request;
        request.set_request_id(get_request_id());
        request.set_response_address(ut_.response_connect_address());
        request.set_type(RequestType::GET);
        request.set_tx_id(tx_id);
        request.set_lastseen(lastSeen);
        if(secondRound){
          request.set_readat(lastSeen);
        }
        addressRequest.insert(pair<Address, KeyRequest>(address, request));
      }
      KeyTuple* tp = addressRequest.at(address).add_tuples();
      tp->set_key(key);
    }

    map<Address, KeyRequest>::iterator itRequests;
    for(itRequests = addressRequest.begin(); itRequests != addressRequest.end(); itRequests++){
      result.insert(itRequests->second.request_id());
      pending_tx_state_map_.insert(pair<string,State*>{itRequests->second.request_id(), state});
      try_request(itRequests->second);
    }

    return result;
    //}
    //return "";
  }

  set<string> vget_async_snapshot(const map<Key,Address> keys, map<Key, unsigned long long> key_t_low, unsigned long long t_high, unsigned long long lastSeen, State* state, const string tx_id) {
    // to simplify the process, or to jump the check of pending_get_response_map_
    //if (pending_get_response_map_.find(keys[0])
    //			== pending_get_response_map_.end()) {
    map<Address, KeyRequest> addressRequest;
    map<Key,Address> keysCopy(keys);
    map<Key,Address>::iterator itKeys;
    set<string> result;
    for ( itKeys = keysCopy.begin(); itKeys!= keysCopy.end(); itKeys++ ) {
      auto key = itKeys->first;
      auto address = itKeys->second;
      if(addressRequest.find(address) == addressRequest.end()){
        KeyRequest request;
        request.set_request_id(get_request_id());
        request.set_response_address(ut_.response_connect_address());
        request.set_type(RequestType::GET);
        request.set_tx_id(tx_id);
        request.set_readat(t_high);
        request.set_lastseen(lastSeen);
        addressRequest.insert(pair<Address, KeyRequest>(address, request));
      }
      KeyTuple* tp = addressRequest.at(address).add_tuples();
      tp->set_key(key);
      tp->set_t_low(key_t_low.at(key));
    }

    map<Address, KeyRequest>::iterator itRequests;
    for(itRequests = addressRequest.begin(); itRequests != addressRequest.end(); itRequests++){
      result.insert(itRequests->second.request_id());
      pending_tx_state_map_.insert(pair<string,State*>{itRequests->second.request_id(), state});
      try_request(itRequests->second);
    }

    return result;
    //}
    //return "";
  }

  set<string> vput_async(const map<Key,Address> keys, const map<Key,string> values,
                         LatticeType lattice_type) {

    map<Address, KeyRequest> addressRequest;
    map<Key,Address> keysCopy(keys);
    map<Key,Address>::iterator itKeys;
    set<string> result;
    for ( itKeys = keysCopy.begin(); itKeys!= keysCopy.end(); itKeys++ ) {
      auto key = itKeys->first;
      auto address = itKeys->second;
      if(addressRequest.find(address) == addressRequest.end()){
        KeyRequest request;
        request.set_request_id(get_request_id());
        request.set_response_address(ut_.response_connect_address());
        request.set_type(RequestType::PUT);
        addressRequest.insert(pair<Address, KeyRequest>(address, request));
      }
      KeyTuple* tp = addressRequest.at(address).add_tuples();
      MultiKeyWrenValue p;
      p.set_committime(1);
      p.set_value(values.at(key));
      tp->set_lattice_type(lattice_type);
      tp->set_key(key);
      tp->set_payload(serialize(p));
    }

    map<Address, KeyRequest>::iterator itRequests;
    for(itRequests = addressRequest.begin(); itRequests != addressRequest.end(); itRequests++){
      result.insert(itRequests->second.request_id());
      try_request(itRequests->second);
    }

    return result;

  }

  set<string> prepare_async(const map<Key,Address> keys, const map<Key,string> values, const string tx_id,
                            LatticeType lattice_type, State* state) {

    map<Address, KeyRequest> addressRequest;
    map<Key,Address> keysCopy(keys);
    map<Key,Address>::iterator itKeys;
    set<string> result;
    for ( itKeys = keysCopy.begin(); itKeys!= keysCopy.end(); itKeys++ ) {
      auto key = itKeys->first;
      auto address = itKeys->second;
      if(addressRequest.find(address) == addressRequest.end()){
        KeyRequest request;
        request.set_tx_id(tx_id);
        request.set_lastseen(0);
        request.set_request_id(get_request_id());
        request.set_response_address(ut_.response_connect_address());
        request.set_type(RequestType::PREPARE);
        request.tx_id();
        addressRequest.insert(pair<Address, KeyRequest>(address, request));
      }
      KeyTuple* tp = addressRequest.at(address).add_tuples();
      tp->set_lattice_type(lattice_type);
      tp->set_key(key);
      tp->set_payload(values.at(key));

    }

    map<Address, KeyRequest>::iterator itRequests;
    for(itRequests = addressRequest.begin(); itRequests != addressRequest.end(); itRequests++){
      result.insert(itRequests->second.request_id());
      pending_tx_state_map_.insert(pair<string,State*>{itRequests->second.request_id(), state});
      try_request(itRequests->second);
    }

    return result;

  }

  set<string> commit_async(const map<Key,Address> keys, unsigned long long commitTime, const string tx_id, State* state) {

    map<Address, KeyRequest> addressRequest;
    map<Key,Address> keysCopy(keys);
    map<Key,Address>::iterator itKeys;
    set<string> result;
    for ( itKeys = keysCopy.begin(); itKeys!= keysCopy.end(); itKeys++ ) {
      auto key = itKeys->first;
      auto address = itKeys->second;
      if(addressRequest.find(address) == addressRequest.end()){
        KeyRequest request;
        request.set_lastseen(0);
        request.set_committime(commitTime);
        request.set_tx_id(tx_id);
        request.set_request_id(get_request_id());
        request.set_response_address(ut_.response_connect_address());
        request.set_type(RequestType::COMMIT);
        addressRequest.insert(pair<Address, KeyRequest>(address, request));
      }
      KeyTuple* tp = addressRequest.at(address).add_tuples();
      tp->set_key(key);
    }

    map<Address, KeyRequest>::iterator itRequests;
    for(itRequests = addressRequest.begin(); itRequests != addressRequest.end(); itRequests++){
      result.insert(itRequests->second.request_id());
      pending_tx_state_map_.insert(pair<string,State*>{itRequests->second.request_id(), state});
      try_request(itRequests->second);
    }

    return result;

  }

  vector<KeyResponse> receive_async() {
    vector<KeyResponse> result;
    kZmqUtil->poll(0, &pollitems_);

    if (pollitems_[0].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&key_address_puller_);
      KeyAddressResponse response;
      response.ParseFromString(serialized);
      for(auto address : response.addresses()){

        Key key = address.key();

        if (pending_request_map_.find(key) != pending_request_map_.end()) {
          if (response.error() == AnnaError::NO_SERVERS) {
            log_->error(
                "No servers have joined the cluster yet. Retrying request.");
            pending_request_map_[key].first = std::chrono::system_clock::now();

            query_routing_async(key);
          } else {
            // populate cache
            for (const Address& ip : address.ips()) {
              key_address_cache_[key].insert(ip);
            }

            // handle stuff in pending request map
            for (auto& req : pending_request_map_[key].second) {
              try_request(req);
            }

            // GC the pending request map
            pending_request_map_.erase(key);
          }
        }else{
          for (const Address& ip : address.ips()) {
            key_address_cache_[key].insert(ip);
          }
          if(pending_state_map_.find(key) != pending_state_map_.end()){
            for(State* state : pending_state_map_.at(key)){
              state->handle(KeyResponse());
            }
            pending_state_map_.erase(key);
          }
        }
      }
    }

    if (pollitems_[1].revents & ZMQ_POLLIN) {
      string serialized = kZmqUtil->recv_string(&response_puller_);
      KeyResponse response;
      response.ParseFromString(serialized);
      bool flag = false;
      for (auto tuple : response.tuples()) {
        Key key = tuple.key();

        if (response.type() == RequestType::GET) {
          if (pending_get_response_map_.find(key) !=
              pending_get_response_map_.end()) {
            if (check_tuple(response.tuples(0))) {
              // error no == 2, so re-issue request
              pending_get_response_map_[key].tp_ =
                  std::chrono::system_clock::now();

              try_request(pending_get_response_map_[key].request_);
            } else {
              // error no == 0 or 1
              flag = true;

              pending_get_response_map_.erase(key);

            }
          }
        } else {
          if (pending_put_response_map_.find(key) !=
              pending_put_response_map_.end() &&
              pending_put_response_map_[key].find(response.response_id()) !=
              pending_put_response_map_[key].end()) {
            if (check_tuple(tuple)) {
              // error no == 2, so re-issue request
              pending_put_response_map_[key][response.response_id()].tp_ =
                  std::chrono::system_clock::now();

              try_request(pending_put_response_map_[key][response.response_id()]
                              .request_);
            } else {
              // error no == 0
              flag = true;
              pending_put_response_map_[key].erase(response.response_id());

              if (pending_put_response_map_[key].size() == 0) {
                pending_put_response_map_.erase(key);
              }
            }
          }
        }
      }
      if(flag){
        if(pending_tx_state_map_.find(response.response_id()) != pending_tx_state_map_.end()){
          result = pending_tx_state_map_.at(response.response_id())->handle(response);
        }else{
          result.push_back(response);
        }
      }

    }

    // GC the pending request map
    set<Key> to_remove;
    for (const auto& pair : pending_request_map_) {
      if (std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now() - pair.second.first)
              .count() > timeout_) {
        // query to the routing tier timed out
        for (const auto& req : pair.second.second) {
          result.push_back(generate_bad_response(req));
        }

        to_remove.insert(pair.first);
      }
    }

    for (const Key& key : to_remove) {
      pending_request_map_.erase(key);
    }

    // GC the pending get response map
    to_remove.clear();
    for (const auto& pair : pending_get_response_map_) {
      if (std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now() - pair.second.tp_)
              .count() > timeout_) {
        // query to server timed out
        result.push_back(generate_bad_response(pair.second.request_));
        to_remove.insert(pair.first);
        invalidate_cache_for_worker(pair.second.worker_addr_);
      }
    }

    for (const Key& key : to_remove) {
      pending_get_response_map_.erase(key);
    }

    // GC the pending put response map
    map<Key, set<string>> to_remove_put;
    for (const auto& key_map_pair : pending_put_response_map_) {
      for (const auto& id_map_pair :
          pending_put_response_map_[key_map_pair.first]) {
        if (std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now() -
            pending_put_response_map_[key_map_pair.first][id_map_pair.first]
                .tp_)
                .count() > timeout_) {
          result.push_back(generate_bad_response(id_map_pair.second.request_));
          to_remove_put[key_map_pair.first].insert(id_map_pair.first);
          invalidate_cache_for_worker(id_map_pair.second.worker_addr_);
        }
      }
    }

    for (const auto& key_set_pair : to_remove_put) {
      for (const auto& id : key_set_pair.second) {
        pending_put_response_map_[key_set_pair.first].erase(id);
      }
    }

    return result;
  }

  /**
   * Set the logger used by the client.
   */
  void set_logger(logger log) { log_ = log; }

  /**
   * Clears the key address cache held by this client.
   */
  void clear_cache() { key_address_cache_.clear(); }

  /**
   * Return the ZMQ context used by this client.
   */
  zmq::context_t* get_context() { return &context_; }

  /**
   * Return the random seed used by this client.
   */
  unsigned get_seed() { return seed_; }

private:

  void try_multi_request(KeyRequest& multiRequest) {
    // we only get NULL back for the worker thread if the query to the routing
    // tier timed out, which should never happen.
    map<Address, vector<Key>> address_to_keys;
    for( auto tuple : multiRequest.tuples()){
      Key key = tuple.key();
      Address worker = get_worker_thread(key);
      vector<Key> v;
      if(address_to_keys.find(worker) == address_to_keys.end()){
        address_to_keys.insert(pair<Address,vector<Key> >(worker, vector<Key>()));
      }
      address_to_keys.at(worker).push_back(key);
      if (worker.length() == 0) {
        // this means a key addr request is issued asynchronously
        if (pending_request_map_.find(key) == pending_request_map_.end()) {
          pending_request_map_[key].first = std::chrono::system_clock::now();
        }
        pending_request_map_[key].second.push_back(multiRequest);
        if(pending_request_keys_map_.find(multiRequest.request_id()) == pending_request_keys_map_.end()){
          pending_request_keys_map_.insert(pair<string,vector<Key> >(multiRequest.request_id(), vector<Key>()));
        }
        pending_request_keys_map_.at(multiRequest.request_id()).push_back(key);
      }
    }
    if(pending_request_keys_map_.find(multiRequest.request_id()) == pending_request_keys_map_.end()){
      return;
    }

    /*
    map<Address, vector<Key>>::iterator it;

    for ( it = address_to_keys.begin(); it != address_to_keys.end(); it++ )
    {
        KeyRequest request;
        request.set_request_id(multiRequest.request_id());
        request.set_response_address(ut_.response_connect_address());
        request.set_type(RequestType::GET);
        Address worker = it->first;
        for (int i = 0; i < it->second.size(); i++) {
            Key key = it->second[i];
            KeyTuple* tp = request.add_tuples();
            tp->set_key(key);
            request.mutable_tuples(i)->set_address_cache_size(
                    key_address_cache_[key].size());




            send_request<KeyRequest>(request, socket_cache_[worker]);

            if (request.type() == RequestType::GET) {
                if (pending_get_response_map_.find(key) ==
                    pending_get_response_map_.end()) {
                    pending_get_response_map_[key].tp_ = std::chrono::system_clock::now();
                    pending_get_response_map_[key].request_ = request;
                }

                pending_get_response_map_[key].worker_addr_ = worker;
            } else {
                if (pending_put_response_map_[key].find(request.request_id()) ==
                    pending_put_response_map_[key].end()) {
                    pending_put_response_map_[key][request.request_id()].tp_ =
                            std::chrono::system_clock::now();
                    pending_put_response_map_[key][request.request_id()].request_ = request;
                }
                pending_put_response_map_[key][request.request_id()].worker_addr_ =
                        worker;
            }
        }
    }
    */


  }

  /**
  * A recursive helper method for the get and put implementations that tries
  * to issue a request at most trial_limit times before giving up. It  checks
  * for the default failure modes (timeout, errno == 2, and cache
  * invalidation). If there are no issues, it returns the set of responses to
  * the respective implementations for them to deal with. This is the same as
  * the above implementation of try_multi_request, except it only operates on
  * a single request.
  */
  void try_request(KeyRequest& request) {
    // we only get NULL back for the worker thread if the query to the routing
    // tier timed out, which should never happen.
    Address worker;
    for(auto tuple:request.tuples()) {
      Key key = request.tuples(0).key();
      worker = get_worker_thread(key);
      if (worker.length() == 0) {
        // this means a key addr request is issued asynchronously
        if (pending_request_map_.find(key) == pending_request_map_.end()) {
          pending_request_map_[key].first = std::chrono::system_clock::now();
        }
        pending_request_map_[key].second.push_back(request);
        return;
      }
    }
    if (request.tuples().size() == 1) {
      Key key = request.tuples(0).key();
      request.mutable_tuples(0)->set_address_cache_size(
          key_address_cache_[key].size());

      send_request<KeyRequest>(request, socket_cache_[worker]);

      if (request.type() == RequestType::GET) {
        if (pending_get_response_map_.find(key) ==
            pending_get_response_map_.end()) {
          pending_get_response_map_[key].tp_ = std::chrono::system_clock::now();
          pending_get_response_map_[key].request_ = request;
        }

        pending_get_response_map_[key].worker_addr_ = worker;
      } else {
        if (pending_put_response_map_[key].find(request.request_id()) ==
            pending_put_response_map_[key].end()) {
          pending_put_response_map_[key][request.request_id()].tp_ =
              std::chrono::system_clock::now();
          pending_put_response_map_[key][request.request_id()].request_ = request;
        }
        pending_put_response_map_[key][request.request_id()].worker_addr_ =
            worker;
      }
    }
    else if (request.tuples().size() > 1){
      for(auto tuple:request.tuples()) {
        Key key = tuple.key();
        if (request.type() == RequestType::GET) {
          if (pending_get_response_map_.find(key) ==
              pending_get_response_map_.end()) {
            pending_get_response_map_[key].tp_ = std::chrono::system_clock::now();
            pending_get_response_map_[key].request_ = request;
          }

          pending_get_response_map_[key].worker_addr_ = worker;
        } else {
          if (pending_put_response_map_[key].find(request.request_id()) ==
              pending_put_response_map_[key].end()) {
            pending_put_response_map_[key][request.request_id()].tp_ =
                std::chrono::system_clock::now();
            pending_put_response_map_[key][request.request_id()].request_ = request;
          }
          pending_put_response_map_[key][request.request_id()].worker_addr_ =
              worker;
        }
      }
      for(auto mutable_tuple: * request.mutable_tuples()) {
        mutable_tuple.set_address_cache_size(
            key_address_cache_[*mutable_tuple.mutable_key()].size());
      }
      send_request<KeyRequest>(request, socket_cache_[worker]);

    }
  }


  /**
 * A helper method to check for the default failure modes for a request that
 * retrieves a response. It returns true if the caller method should reissue
 * the request (this happens if errno == 2). Otherwise, it returns false. It
 * invalidates the local cache if the information is out of date.
 */
  bool check_tuple(const KeyTuple& tuple) {
    Key key = tuple.key();
    if (tuple.error() == 2) {
      log_->info(
          "Server ordered invalidation of key address cache for key {}. "
          "Retrying request.",
          key);

      invalidate_cache_for_key(key, tuple);
      return true;
    }

    if (tuple.invalidate()) {
      invalidate_cache_for_key(key, tuple);

      log_->info("Server ordered invalidation of key address cache for key {}",
                 key);
    }

    return false;
  }

  /**
   * When a server thread tells us to invalidate the cache for a key it's
   * because we likely have out of date information for that key; it sends us
   * the updated information for that key, and update our cache with that
   * information.
   */
  void invalidate_cache_for_key(const Key& key, const KeyTuple& tuple) {
    key_address_cache_.erase(key);
  }

  /**
   * Invalidate the key caches for any key that previously had this worker in
   * its cache. The underlying assumption is that if the worker timed out, it
   * might have failed, and so we don't want to rely on it being alive for both
   * the key we were querying and any other key.
   */
  void invalidate_cache_for_worker(const Address& worker) {
    vector<string> tokens;
    split(worker, ':', tokens);
    string signature = tokens[1];
    set<Key> remove_set;

    for (const auto& key_pair : key_address_cache_) {
      for (const string& address : key_pair.second) {
        vector<string> v;
        split(address, ':', v);

        if (v[1] == signature) {
          remove_set.insert(key_pair.first);
        }
      }
    }

    for (const string& key : remove_set) {
      key_address_cache_.erase(key);
    }
  }

  /**
   * Prepare a data request object by populating the request ID, the key for
   * the request, and the response address. This method modifies the passed-in
   * KeyRequest and also returns a pointer to the KeyTuple contained by this
   * request.
   */
  KeyTuple* prepare_data_request(KeyRequest& request, const Key& key) {
    request.set_request_id(get_request_id());
    request.set_response_address(ut_.response_connect_address());

    KeyTuple* tp = request.add_tuples();
    tp->set_key(key);

    return tp;
  }

  /**
   * returns all the worker threads for the key queried. If there are no cached
   * threads, a request is sent to the routing tier. If the query times out,
   * NULL is returned.
   */
  set<Address> get_all_worker_threads(const Key& key) {
    if (key_address_cache_.find(key) == key_address_cache_.end() ||
        key_address_cache_[key].size() == 0) {
      if (pending_request_map_.find(key) == pending_request_map_.end()) {
        query_routing_async(key);
      }
      return set<Address>();
    } else {
      return key_address_cache_[key];
    }
  }

  set<Address> get_all_worker_threads(const Key& key, State* state) {
    if (key_address_cache_.find(key) == key_address_cache_.end() ||
        key_address_cache_[key].size() == 0) {
      if(pending_state_map_.find(key) == pending_state_map_.end()){
        pending_state_map_.insert(pair<Key,vector<State*>>{key,vector<State*>{}});
      }
      pending_state_map_.at(key).push_back(state);
      if (pending_request_map_.find(key) == pending_request_map_.end()) {
        query_routing_async(key);
      }
      return set<Address>();
    } else {
      return key_address_cache_[key];
    }
  }

  /**
   * Similar to the previous method, but only returns one (randomly chosen)
   * worker address instead of all of them.
   */
  Address get_worker_thread(const Key& key) {
    set<Address> local_cache = get_all_worker_threads(key);

    // This will be empty if the worker threads are not cached locally
    if (local_cache.size() == 0) {
      return "";
    }

    return *(next(begin(local_cache), rand_r(&seed_) % local_cache.size()));
  }

  /**
   * returns all the worker threads for the key queried. If there are no cached
   * threads, a request is sent to the routing tier. If the query times out,
   * NULL is returned.
   */
  map<Key,set<Address>> get_all_worker_threads_multi(const set<Key> keys) {
    vector<Key> missing_keys;
    map<Key,set<Address>> key_2_adresses;
    for(Key key:keys) {
      if (key_address_cache_.find(key) == key_address_cache_.end() ||
          key_address_cache_[key].size() == 0) {
        if (pending_request_map_.find(key) == pending_request_map_.end()) {
          missing_keys.push_back(key);
        }
      } else {
        key_2_adresses.insert(pair<string,set<Address>>(key, key_address_cache_[key]));
      }
    }
    if(!missing_keys.empty()) {
      return map<Key,set<Address>>();
    }
    return key_2_adresses;
  }

  set<Key> get_missing_worker_threads_multi(const set<Key> keys) {
    set<Key> missing_keys;
    for(Key key:keys) {
      if (key_address_cache_.find(key) == key_address_cache_.end() ||
          key_address_cache_[key].size() == 0) {
        missing_keys.insert(key);
      }
    }
    return missing_keys;
  }

  /**
   * Similar to the previous method, but only returns one (randomly chosen)
   * worker address instead of all of them.
   */
  map<Key,Address>  get_worker_thread_multi(const set<Key> keys) {
    map<Key,set<Address>> local_cache = get_all_worker_threads_multi(keys);

    // This will be empty if the worker threads are not cached locally
    if (local_cache.size() == 0) {
      return map<Key,Address>();
    }
    map<Key,Address> result;
    map<Key,set<Address>>::iterator it;
    for ( it = local_cache.begin(); it != local_cache.end(); it++ ) {
      result.insert(pair<Key,Address>(it->first, *(next(begin(it->second), rand_r(&seed_) % it->second.size()))));
    }
    return result;
  }

  /**
   * Returns one random routing thread's key address connection address. If the
   * client is running outside of the cluster (ie, it is querying the ELB),
   * there's only one address to choose from but 4 threads.
   */
  Address get_routing_thread() {
    return routing_threads_[rand_r(&seed_) % routing_threads_.size()]
        .key_address_connect_address();
  }

  /**
   * Send a query to the routing tier asynchronously.
   */
  void query_muilti_routing_async(const vector<Key> keys) {
    // define protobuf request objects
    KeyAddressRequest request;

    // populate request with response address, request id, etc.
    request.set_request_id(get_request_id());
    request.set_response_address(ut_.key_address_connect_address());
    for(Key key : keys)
      request.add_keys(key);

    Address rt_thread = get_routing_thread();
    send_request<KeyAddressRequest>(request, socket_cache_[rt_thread]);
  }

  void query_routing_async(const Key& key) {
    // define protobuf request objects
    KeyAddressRequest request;

    // populate request with response address, request id, etc.
    request.set_request_id(get_request_id());
    request.set_response_address(ut_.key_address_connect_address());
    request.add_keys(key);

    Address rt_thread = get_routing_thread();
    send_request<KeyAddressRequest>(request, socket_cache_[rt_thread]);
  }

  /**
   * Generates a unique request ID.
   */
  string get_request_id() {
    if (++rid_ % 10000 == 0) rid_ = 0;
    return ut_.ip() + ":" + std::to_string(ut_.tid()) + "_" +
           std::to_string(rid_++);
  }

  KeyResponse generate_bad_response(const KeyRequest& req) {
    KeyResponse resp;

    resp.set_type(req.type());
    resp.set_response_id(req.request_id());
    resp.set_error(AnnaError::TIMEOUT);

    KeyTuple* tp = resp.add_tuples();
    tp->set_key(req.tuples(0).key());

    if (req.type() == RequestType::PUT) {
      tp->set_lattice_type(req.tuples(0).lattice_type());
      tp->set_payload(req.tuples(0).payload());
    }

    return resp;
  }


private:
  // the set of routing addresses outside the cluster
  vector<UserRoutingThread> routing_threads_;

  // the current request id
  unsigned rid_;

  // the random seed for this client
  unsigned seed_;

  // the IP and port functions for this thread
  UserThread ut_;

  // the ZMQ context we use to create sockets
  zmq::context_t context_;

  // cache for opened sockets
  SocketCache socket_cache_;

  // ZMQ receiving sockets
  zmq::socket_t key_address_puller_;
  zmq::socket_t response_puller_;

  vector<zmq::pollitem_t> pollitems_;

  // cache for retrieved worker addresses organized by key
  map<Key, set<Address>> key_address_cache_;

  // class logger
  logger log_;

  // GC timeout
  unsigned timeout_;

  // keeps track of pending requests due to missing worker address
  map<Key, pair<TimePoint, vector<KeyRequest>>> pending_request_map_;

  map<string, vector<Key>> pending_request_keys_map_;

  map<Key, vector<State*>> pending_state_map_;

  // keeps track of pending get responses
  map<Key, PendingRequest> pending_get_response_map_;

  map<string, State*> pending_tx_state_map_;


  // keeps track of pending put responses
  map<Key, map<string, PendingRequest>> pending_put_response_map_;

};
