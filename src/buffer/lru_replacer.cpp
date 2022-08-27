//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages){}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
    std::lock_guard<std::mutex> lock(mu_);
    if(used_frame_.empty())return false;

    // least recently used page delete
    *frame_id = frames_.back();
    used_frame_.erase(*frame_id);
    frames_.pop_back();

    return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(mu_);
    if(used_frame_.count(frame_id)){
        frames_.erase(used_frame_[frame_id]);
        used_frame_.erase(frame_id);
    }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(mu_);
    if(!used_frame_.count(frame_id)){
        frames_.push_front(frame_id);
        used_frame_[frame_id] = frames_.begin();
    }
}

auto LRUReplacer::Size() -> size_t { 
    std::lock_guard<std::mutex> lock(mu_);
    return used_frame_.size();
}

}  // namespace bustub
