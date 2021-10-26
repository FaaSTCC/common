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
//  Modifications copyright (C) 2021 Taras Lykhenko, Rafael Soares

#ifndef INCLUDE_LATTICES_LWW_PAIR_LATTICE_HPP_
#define INCLUDE_LATTICES_LWW_PAIR_LATTICE_HPP_

#include "core_lattices.hpp"

template <typename T>
struct TimestampValuePair {
  unsigned long long promise{0};
  unsigned long long timestamp{0};
  T value;

  TimestampValuePair<T>() {
    promise = 0;
    timestamp = 0;
    value = T();
  }

  // need this because of static cast
  TimestampValuePair<T>(const unsigned long long& a) {
    promise = 0;
    timestamp = 0;
    value = T();
  }

  TimestampValuePair<T>(const unsigned long long& ts, const T& v) {
    promise = ts;
    timestamp = ts;
    value = v;
  }

  TimestampValuePair<T>(const unsigned long long& ts, const unsigned long long& p, const T& v) {
    promise = p;
    timestamp = ts;
    value = v;
  }

  unsigned size() { return value.size() + sizeof(unsigned long long) + sizeof(unsigned long long); }
};

template <typename T>
class LWWPairLattice : public Lattice<TimestampValuePair<T>> {
 protected:
  void do_merge(const TimestampValuePair<T>& p) {
    if (p.timestamp >= this->element.timestamp) {
      this->element.promise = std::max(this->element.promise, p.promise);
      this->element.timestamp = p.timestamp;
      if(p.value != ""){
        this->element.value = p.value;
      }
    }
  }

 public:
  LWWPairLattice() : Lattice<TimestampValuePair<T>>(TimestampValuePair<T>()) {}
  LWWPairLattice(const TimestampValuePair<T>& p) :
      Lattice<TimestampValuePair<T>>(p) {}
  MaxLattice<unsigned> size() { return {this->element.size()}; }
};

#endif  // INCLUDE_LATTICES_LWW_PAIR_LATTICE_HPP_
