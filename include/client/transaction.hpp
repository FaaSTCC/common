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

#ifndef ANNA_TRANSACTION_HPP
#define ANNA_TRANSACTION_HPP


#include <types.hpp>


class State;
class KvsClientInterface;


class TransactionInterface {
  // CurrentState
  // Read transaction has two possible round
  // each state is responsible for each specific round
public:
  virtual void setState(State* st) = 0;

  virtual void nextState() = 0;

  virtual State* getState() = 0;

  virtual KvsClientInterface* getClient() = 0;

  virtual string getTxID() = 0;
  virtual map<Key,Address> getAddress() = 0;

};


#endif //ANNA_TRANSACTION_HPP
