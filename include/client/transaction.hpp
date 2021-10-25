//
// Created by Taras Lykhenko on 24/09/2020.
//

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
