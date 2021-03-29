//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"
#include "common/logger.h"
namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
	frame_map.reserve(num_pages);
	clock_hand = 0;
	TOTAL_FRAMES = 0;
	blank_id = nullptr;
	MAX_FRAMES = num_pages;
	replace_list.reserve(num_pages);
	for(size_t i = 0;i < num_pages;i++){
		replace_list.push_back(-1);
	}
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id){
	std::vector<size_t> frame_status(3);
	LOG_INFO("Frame victimisation");
	size_t frames_passed = 0;
	frame_id_t lowest_fid = 2147483647;
	if(TOTAL_FRAMES == 0){
		LOG_INFO("TOTAL_FRAMES == 0");
		frame_id = nullptr;
		return false;
	}
	while(frames_passed != TOTAL_FRAMES){
		if(replace_list[clock_hand] == -1){
			clock_hand = (clock_hand+1)%MAX_FRAMES;
			continue;
		}
		frame_status = frame_map[replace_list[clock_hand]];
		if(frame_status[1] == 1){
			frame_status[1] = 0;
			if(replace_list[clock_hand] < lowest_fid){
				lowest_fid = replace_list[clock_hand];
				LOG_INFO("Lowest fid:%d",lowest_fid);
			}
			clock_hand = (clock_hand+1)%MAX_FRAMES;
			frames_passed++;
		}
		else if(frame_status[1] == 0){
			*frame_id = replace_list[clock_hand];
			LOG_INFO("If condition, frame victimised: %d",replace_list[clock_hand]);
			frame_map.erase(replace_list[clock_hand]);
			replace_list[clock_hand] = -1;
			TOTAL_FRAMES--;
			return true;
		}
	}
	LOG_INFO("Getting lowest_fid:%d",lowest_fid);
	*frame_id = lowest_fid;
	frame_status = frame_map[lowest_fid];
	LOG_INFO("Frame victimised: %d",lowest_fid);
	frame_map.erase(lowest_fid);
	replace_list[frame_status[2]] = -1;
	TOTAL_FRAMES--;
	return true;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
	//If frame is in replacement vector, find it and remove it
	LOG_INFO("Frame pin %d",frame_id);
	std::vector<size_t> frame_status(3);
	size_t dup_clock_hand;
	//If frame is being pinned after it was unpinned
	if(frame_map.find(frame_id) != frame_map.end()){
		frame_status = frame_map[frame_id];
		frame_status[0] = 1;
		frame_status[1] = 1;
		dup_clock_hand = frame_status[2];
		LOG_INFO("Frame Id: %d, Location in replacer: %lu",frame_id,frame_status[2]);
		replace_list[dup_clock_hand] = -1;
		frame_map[frame_id] = frame_status;
		TOTAL_FRAMES--;
	}
	//If frame is being pinned for the first time
	else{
		LOG_INFO("Frame pinned for first time, frame id:%d",frame_id);
		frame_status[0] = 1;
		frame_status[1] = 1;
		frame_status[2] = -1;
		frame_map[frame_id] = frame_status;
	}
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
	LOG_INFO("Frame unpin %d", frame_id);
	size_t dup_clock_hand;
	std::vector<size_t> frame_status(3);
	if(MAX_FRAMES == TOTAL_FRAMES){
		LOG_INFO("Replacer full");
		return;
	}
	//If frame is new, add it to the map and replace list.
	if(frame_map.find(frame_id) == frame_map.end()){
		dup_clock_hand = clock_hand;
		frame_status[0] = 0;
		frame_status[1] = 0;
		while(replace_list[dup_clock_hand] != -1){
			dup_clock_hand = (dup_clock_hand+1)%MAX_FRAMES;
		}
		replace_list[dup_clock_hand] = frame_id;
		frame_status[2] = dup_clock_hand;
		if(dup_clock_hand == clock_hand){
			clock_hand = (clock_hand+1)%MAX_FRAMES;
		}
		LOG_INFO("Frame id: %d unpin frame replacer location:%lu",frame_id,frame_status[2]);
		frame_map[frame_id] = frame_status;
		TOTAL_FRAMES++;
	}
	//If frame is unpinned after getting pinned, add it back to the replace list
	else if(frame_map.find(frame_id) != frame_map.end()){
		dup_clock_hand = clock_hand;
		frame_status = frame_map[frame_id];
		while(replace_list[dup_clock_hand] != -1){
			dup_clock_hand = (dup_clock_hand+1)%MAX_FRAMES;
		}
		//Only a frame that was pinned previously can be unpinned
		if(frame_status[0] == 1){
			replace_list[dup_clock_hand] = frame_id;
			frame_status[0] = 0;
			frame_status[1] = 1;
			frame_status[2] = dup_clock_hand;
			if(dup_clock_hand == clock_hand){
				clock_hand = (clock_hand+1)%MAX_FRAMES;
			}
			frame_map[frame_id] = frame_status;
			TOTAL_FRAMES++;
		}
	}
}

size_t ClockReplacer::Size() {
	LOG_INFO("Frame size: %lu",TOTAL_FRAMES);
	return TOTAL_FRAMES;
}

}
