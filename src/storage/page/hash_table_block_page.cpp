//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_block_page.cpp
//
// Identification: src/storage/page/hash_table_block_page.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_block_page.h"
#include "storage/index/generic_key.h"
#include "common/logger.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BLOCK_TYPE::KeyAt(slot_offset_t bucket_ind) const {
	KeyType key_at_ind;
	key_at_ind = array_[bucket_ind].first;
	return key_at_ind;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BLOCK_TYPE::ValueAt(slot_offset_t bucket_ind) const {
	ValueType value_at_ind;
	value_at_ind = array_[bucket_ind].second;
	return value_at_ind;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::Insert(slot_offset_t bucket_ind, const KeyType &key, const ValueType &value) {
	if(this->IsReadable(bucket_ind)){
		return false;
	}
	else{
		array_[bucket_ind].first = key;
		array_[bucket_ind].second = value;
		occupied_[bucket_ind] = 1;
		readable_[bucket_ind] = 1;
		return true;
	}
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::Remove(slot_offset_t bucket_ind) {
	if(this->IsReadable(bucket_ind)){
		readable_[bucket_ind] = 0;
	}
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsOccupied(slot_offset_t bucket_ind) const {
	if(occupied_[bucket_ind] == 1)
		return true;
	else
		return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsReadable(slot_offset_t bucket_ind) const {
	if(readable_[bucket_ind] == 1)
		return true;
	else
		return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::SetPageId(bustub::page_id_t page_id) {
	this->page_id_ = page_id;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
page_id_t HASH_TABLE_BLOCK_TYPE::GetPageId() const {
	return page_id_;
}


// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBlockPage<int, int, IntComparator>;
template class HashTableBlockPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBlockPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBlockPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBlockPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBlockPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
