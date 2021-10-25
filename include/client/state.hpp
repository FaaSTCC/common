//
// Created by Taras Lykhenko on 24/09/2020.
//

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
