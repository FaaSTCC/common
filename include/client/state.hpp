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

#ifndef ANNA_STATE_HPP
#define ANNA_STATE_HPP

#include <vector>

class TransactionInterface;


class KeyResponse;



class State {
  /**
   * @var Context
   */
protected:
  TransactionInterface * context_;

public:
  virtual ~State() {
  }

  void set_context(TransactionInterface *context) {
    this->context_ = context;
  }

  virtual void start() = 0;

  virtual std::vector<KeyResponse> handle(KeyResponse keyResponse) = 0;

};



#endif //ANNA_STATE_HPP
