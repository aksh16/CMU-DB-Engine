//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.h
//
// Identification: src/include/buffer/clock_replacer.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
// #include <mutex>  // NOLINT
#include <vector>
#include <unordered_map>
#include <iostream>
#include <algorithm>
#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * ClockReplacer implements the clock replacement policy, which approximates the Least Recently Used policy.
 */
class ClockReplacer : public Replacer {
 public:
  /**
   * Create a new ClockReplacer.
   * @param num_pages the maximum number of pages the ClockReplacer will be required to store
   */
  explicit ClockReplacer(size_t num_pages);

  /**
   * Destroys the ClockReplacer.
   */
  ~ClockReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  // TODO(student): implement me!
	/*Vector to store status of frame.
	(0=false,1=true,-1=irrelevant)
	[0]->Pin status
	[1]->reference status
	[2]->location of frame in replace_list*/
	std::unordered_map<frame_id_t,std::vector<size_t>> frame_map;
	/*List of frames considered for replacement
	-1 indicates empty space in list*/
	std::vector<frame_id_t> replace_list;
	//Clock hand for replace_list
	size_t clock_hand;
	size_t TOTAL_FRAMES;
	frame_id_t *blank_id;
	size_t MAX_FRAMES;
};

}
