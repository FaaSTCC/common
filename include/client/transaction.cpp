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
#include "transaction.hpp"
#include "state.cpp"


class Transaction : public TransactionInterface {
  public:
  KvsClientInterface *_client;
  State *_state;
  set<Key> _keys;
  string tx_id;
  map<Key,Address> address;

  Transaction(set<Key> keys, KvsClientInterface *client)
      : _client(client), _keys(keys), _state{nullptr}, tx_id{client->get_request_id()} {
  }

  void setState(State *st) {
    if (this->_state != nullptr)
      delete this->_state;
    st->set_context(this);
    this->_state = st;
    st->start();
  }


  State *getState() {
    return this->_state;
  }

  KvsClientInterface *getClient() {
    return this->_client;
  }

  string getTxID() {
    return tx_id;
  }

  map<Key,Address> getAddress() {
    return address;
  }

  ~Transaction() {delete this->_state;};


};


class ReadTransaction : public Transaction {
  unsigned long long t_low_;
  unsigned long long t_high_;

public:
  ReadTransaction(set<Key> keys, KvsClientInterface *client, unsigned long long t_low, unsigned long long t_high)
      : Transaction(keys,client), t_low_{t_low}, t_high_{t_high} {
    auto missing_keys = client->get_missing_worker_threads_multi(keys);
    if (missing_keys.size() != 0) {
      setState(new KeyToPartitionState(missing_keys));
    } else {
      this->address = _client->get_worker_thread_multi(_keys);
      setState(new FirstRoundState(keys,t_low));
    }
  }


  void nextState() {
    if (this->_state != nullptr)
      delete this->_state;
    FirstRoundState * st = new FirstRoundState(_keys,t_low_);
    st->set_context(this);
    this->_state = st;
    this->address = _client->get_worker_thread_multi(_keys);
    st->start();
  }

};

class ReadSliceTransaction : public Transaction {
map<Key, unsigned long long> key_t_low_;
unsigned long long t_high_, lastSeen_;

public:
ReadSliceTransaction(set<Key> keys, KvsClientInterface *client, map<Key, unsigned long long> key_t_low, unsigned long long t_high, unsigned long long lastSeen)
: Transaction(keys,client), key_t_low_{key_t_low}, t_high_{t_high}, lastSeen_{lastSeen} {
auto missing_keys = client->get_missing_worker_threads_multi(keys);
if (missing_keys.size() != 0) {
setState(new KeyToPartitionState(missing_keys));
} else {
this->address = _client->get_worker_thread_multi(_keys);
setState(new SliceRoundState(keys, key_t_low, t_high, lastSeen));
}
}


void nextState() {
  if (this->_state != nullptr)
    delete this->_state;
  SliceRoundState * st = new SliceRoundState(_keys, key_t_low_, t_high_, lastSeen_);
  st->set_context(this);
  this->_state = st;
  this->address = _client->get_worker_thread_multi(_keys);
  st->start();
}

};

template<typename TK, typename TV>
set<TK> extract_keys(std::map<TK, TV> const& input_map) {
  set<TK> retval;
  for (auto const& element : input_map) {
    retval.insert(element.first);
  }
  return retval;
}

class WriteTransaction : public Transaction {
  map<Key,string>  key_value_;

public:

  WriteTransaction(map<Key,string> key_value, KvsClientInterface *client)
      : Transaction( extract_keys<Key>(key_value), client), key_value_(key_value){

    auto missing_keys = client->get_missing_worker_threads_multi(_keys);
    if (missing_keys.size() != 0) {
      setState(new KeyToPartitionState(missing_keys));
    } else {
      this->address = _client->get_worker_thread_multi(_keys);
      setState(new PrepareState(_keys, key_value, tx_id));
    }
  }

  void nextState() {
    if (this->_state != nullptr)
      delete this->_state;
    PrepareState * st = new PrepareState(_keys, key_value_, tx_id);
    st->set_context(this);
    this->_state = st;
    this->address = _client->get_worker_thread_multi(_keys);
    st->start();
  }



};








