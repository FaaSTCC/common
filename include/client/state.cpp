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

#include "kvs_client.hpp"
#include "transaction.hpp"
#include "state.hpp"



class CommitState : public State {
  set<string> ids;
  unsigned long long commitTime_;
  vector<KeyResponse> result_;
  set<Key> keys_;
  string txID_;

public:

  CommitState(set<Key> keys, string txID, unsigned long long commitTime) : keys_{keys}, txID_{txID}, commitTime_{commitTime}{
  }

  void start(){
    KvsClientInterface * client = context_->getClient();
    ids = client->commit_async(context_->getAddress(), commitTime_, txID_, this);
  }

  vector<KeyResponse> handle(KeyResponse response) override{
    ids.erase(response.response_id());
    if(ids.size() == 0){
      delete this->context_;
    }
    return vector<KeyResponse>();
  }

};


class PrepareState : public State {
  set<string> ids;
  unsigned long long commitTime = 0;
  vector<KeyResponse> result_;
  set<Key> keys_;
  map<Key,string> key_value_;
  string txID_;
public:

  PrepareState(set<Key> keys, map<Key,string> key_value, string txID) : keys_{keys}, key_value_{key_value}, txID_{txID}{
  }

  void start(){
    KvsClientInterface * client = context_->getClient();
    ids = client->prepare_async(context_->getAddress(), key_value_, txID_, LatticeType::WREN, this);
  }

  vector<KeyResponse> handle(KeyResponse response) override{
    unsigned long long preparedTime = response.preparedtime();
    commitTime = std::max({commitTime, preparedTime});
    ids.erase(response.response_id());
    if(ids.size() == 0){
      context_->setState(new CommitState(keys_, txID_, commitTime));
    }
    return vector<KeyResponse>();
  }

};

class SliceRoundState : public State {
  set<string> ids;
  map<Key, unsigned long long> key_t_low_;
  unsigned long long t_high_, lastSeen_;
  vector<KeyResponse> result_;
  set<Key> keys;
public:

  SliceRoundState(set<Key> keys_, map<Key, unsigned long long> key_t_low, unsigned long long t_high, unsigned long long lastSeen) :  keys{keys_}, key_t_low_{key_t_low}, t_high_{t_high}, lastSeen_{lastSeen}{
  }

  void start(){
    KvsClientInterface * client = context_->getClient();
    ids = client->vget_async_snapshot(context_->getAddress(), key_t_low_, t_high_, lastSeen_, this,context_->getTxID());

  }

  vector<KeyResponse> handle(KeyResponse response) override{
    if (response.error() == 0) {
      result_.push_back(response);
      ids.erase(response.response_id());
    }
    if(ids.size() == 0) {
      delete this->context_;
      return result_;
    }
    return vector<KeyResponse>();
  }


};

class SecondRoundState : public State {
  set<string> ids;
  unsigned long long _time;
  vector<KeyResponse> result_;
  set<Key> keys;
public:

  SecondRoundState(unsigned long long time, vector<KeyResponse> result, set<Key> secondRoundKeys) : _time{time}, result_{result}, keys{secondRoundKeys}{
  }

  void start(){
    KvsClientInterface * client = context_->getClient();
    ids = client->vget_async(context_->getAddress(), _time, true, this,context_->getTxID());

  }

  vector<KeyResponse> handle(KeyResponse response) override{
    if (response.error() == 0) {
      result_.push_back(response);
      ids.erase(response.response_id());
    }
    if(ids.size() == 0) {
      delete this->context_;
      return result_;
    }
    return vector<KeyResponse>();
  }

};

class FirstRoundState : public State {
  set<string> ids;
  vector<KeyResponse> result;
  set<Key> keys_;
  unsigned long long t_low_;
public:

  FirstRoundState(set<Key> keys, unsigned long long t_low) :keys_{keys},t_low_{t_low}{
  }

  void start(){
    KvsClientInterface * client = context_->getClient();

    ids = client->vget_async(context_->getAddress(), t_low_, false, this,context_->getTxID());

  }

  vector<KeyResponse> handle(KeyResponse response) override{
      if (response.error() == 0) {
        result.push_back(response);
        ids.erase(response.response_id());
      }
      if(ids.size() == 0){
        unsigned long long maxCommitTimestamp = 0;
        set<string> secondRoundKeys{};
        for( auto keyResponse : result){
          for( auto tuple : keyResponse.tuples()){
            TimestampValuePair<string> mkcl =
                TimestampValuePair<string>(to_multi_key_wren_payload(
                    deserialize_multi_key_wren(tuple.payload())));
            maxCommitTimestamp = std::max({maxCommitTimestamp,mkcl.timestamp});

          }
        }
        vector<KeyResponse> validResult;
        for( auto keyResponse : result){

          if(keyResponse.lastseen() < maxCommitTimestamp){
            for( auto tuple : keyResponse.tuples()){
              secondRoundKeys.insert(tuple.key());
            }
          }else{
            validResult.push_back(keyResponse);
          }
        }
        if(secondRoundKeys.size() == 0){
          delete this->context_;
          return result;
        }else{
          context_->setState(new SecondRoundState(maxCommitTimestamp, validResult, secondRoundKeys));
        }
      }

      return vector<KeyResponse>();



  }


};

class KeyToPartitionState : public State {
  int waiting = 0;
  set<Key> missing_keys_;
public:
  KeyToPartitionState(set<Key> missing_keys) : missing_keys_{missing_keys} {
  }

  void start() override{
    for(Key key : missing_keys_){
      KvsClientInterface * client = context_->getClient();
      set<Address> workers = client->get_all_worker_threads(key, this);
      if(workers.size()==0){
        waiting ++;
      }
    }
  }

  vector<KeyResponse> handle(KeyResponse keyResponse) override{
    waiting--;
    if(waiting == 0){
      context_->nextState();
    }
    return vector<KeyResponse>{};
  }



};



