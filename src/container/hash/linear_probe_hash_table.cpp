//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/linear_probe_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
		num_buckets_ = num_buckets;
		header_page_id_ = INVALID_PAGE_ID;
		/*Get header page*/
		header_page_ = reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->NewPage(&header_page_id_,nullptr));
		/*Each block_page can store BLOCK_ARRAY_SIZE of <key,value> pairs. */
		num_slots_ = num_buckets/BLOCK_ARRAY_SIZE;
		/*If sum buckets are leftover after all slots are filled then add one more slot.
		Division might give some remainder which will also need slots in hash table*/
		if(num_buckets%BLOCK_ARRAY_SIZE){
			num_slots_++;
		}
		/*Fill up the hash table with block_pages as per the slots */
		for(size_t i = 0;i < num_slots_;i++){
			page_id_t block_page_id = INVALID_PAGE_ID;
			auto block_page = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(buffer_pool_manager_->NewPage(&block_page_id,nullptr));
			block_page->SetPageId(block_page_id);
			header_page_->AddBlockPageId(block_page_id);
		}
	}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
	size_t hash_value = hash_fn_.GetHash(key);
	/*Table slot index determines the block page where key is located */
	size_t table_slot_index = hash_value%num_slots_;
	page_id_t block_page_id = header_page_->GetBlockPageId(table_slot_index);
	auto block_page =  reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(buffer_pool_manager_->FetchPage(block_page_id,nullptr));
	/*Page slot index determines the slot in page the <key,value> pair should go into*/
	size_t page_slot_index = hash_value%BLOCK_ARRAY_SIZE;
	size_t page_slot_counter = 0;
	size_t table_slot_counter = 0;
	std::vector<ValueType> result_container;
	while(true){
		/*Check if slot has never been occupied*/
		if(!block_page->IsOccupied(page_slot_index)){
			break;
		}
		else if(block_page->IsReadable(page_slot_index)){
			if(!comparator_(block_page->KeyAt(page_slot_index),key)){
				result_container.push_back(block_page->ValueAt(page_slot_index));
			}
		}
		/*If key is not found tin that block page, keep in next block page*/
		page_slot_counter++;
		if(page_slot_counter < BLOCK_ARRAY_SIZE){
			page_slot_index = (page_slot_index+1)%BLOCK_ARRAY_SIZE;
			continue;
		}
		/*If whole block page does not have key, check for it in next available block page*/
		else{
			table_slot_counter++;
			if(table_slot_counter < num_slots_){
				/*Get the next block_page and unpin the current block_page*/
				page_slot_index = 0;
				page_slot_counter = 0;
				table_slot_index = (table_slot_index+1)%num_slots_;
				buffer_pool_manager_->UnpinPage(block_page_id,false);
				block_page_id = header_page_->GetBlockPageId(table_slot_index);
				block_page =  reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(buffer_pool_manager_->FetchPage(block_page_id,nullptr));
				continue;
			}
			else{
				break;
			}
		}
	}
	if(result_container.size() == 0){
		result = nullptr;
		return false;
	}
	else{
		*result = result_container;
		return true;
	}
	return false;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
	size_t hash_value = hash_fn_.GetHash(key);
	/*Table slot index determines the block page the <key,value> pair should go into */
	size_t table_slot_index = hash_value%num_slots_;
	page_id_t block_page_id = header_page_->GetBlockPageId(table_slot_index);
	auto block_page =  reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(buffer_pool_manager_->FetchPage(block_page_id,nullptr));
	/*Page slot index determines the slot in page the <key,value> pair should go into*/
	size_t page_slot_index = hash_value%BLOCK_ARRAY_SIZE;
	size_t page_slot_counter = 0;
	size_t table_slot_counter = 0;
	while(true){
		/*If able to insert successfully, return true*/
		if(block_page->Insert(page_slot_index,key,value)){
			/*If insertion is successful, unpin the page*/
			buffer_pool_manager_->UnpinPage(block_page_id, true);
			return true;
		}
		/*If <k,v> pair already exists, return false since duplicates are not allowed.*/
		else if(!(comparator_(block_page->KeyAt(page_slot_index),key)) && block_page->ValueAt(page_slot_index) == value){
			/*If insertion is not possible, unpin the page*/
			buffer_pool_manager_->UnpinPage(block_page_id,false);
			return false;
		}
		/*If page slot was full, check for next available slot*/
		page_slot_counter++;
		if(page_slot_counter < BLOCK_ARRAY_SIZE){
			page_slot_index = (page_slot_index+1)%BLOCK_ARRAY_SIZE;
			continue;
		}
		/*If whole block page is full, check for next available block page*/
		else{
			table_slot_counter++;
			if(table_slot_counter < num_slots_){
				/*Get the next block_page and unpin the current block_page*/
				page_slot_index = 0;
				page_slot_counter = 0;
				table_slot_index = (table_slot_index+1)%num_slots_;
				buffer_pool_manager_->UnpinPage(block_page_id,false);
				block_page_id = header_page_->GetBlockPageId(table_slot_index);
				block_page =  reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(buffer_pool_manager_->FetchPage(block_page_id,nullptr));
				continue;
			}
			else{
				this->Resize(num_buckets_);
				Insert(transaction, key, value);
				return true;
			}
		}
	}
	return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
	size_t hash_value = hash_fn_.GetHash(key);
	/*Table slot index determines the block page where <key,value> is located */
	size_t table_slot_index = hash_value%num_slots_;
	page_id_t block_page_id = header_page_->GetBlockPageId(table_slot_index);
	auto block_page =  reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(buffer_pool_manager_->FetchPage(block_page_id,nullptr));
	/*Page slot index determines the slot in page the <key,value> pair should go into*/
	size_t page_slot_index = hash_value%BLOCK_ARRAY_SIZE;
	size_t page_slot_counter = 0;
	size_t table_slot_counter = 0;
  	while(true){
		/*Check if page slot was ever occupied, if not then return false*/
		if(!block_page->IsOccupied(page_slot_index)){
			buffer_pool_manager_->UnpinPage(block_page_id,false);
			return false;
		}
		/*If <k,v> is found, delete it frome the table */
		else if(block_page->IsReadable(page_slot_index)){
			if(!(comparator_(block_page->KeyAt(page_slot_index),key)) && block_page->ValueAt(page_slot_index) == value){
				block_page->Remove(page_slot_index);
				buffer_pool_manager_->UnpinPage(block_page_id,false);
				return true;
			}
		}
		/*If <k,v> pair is not found in the given slote, check for next available slot*/
		page_slot_counter++;
		if(page_slot_counter < BLOCK_ARRAY_SIZE){
			page_slot_index = (page_slot_index+1)%BLOCK_ARRAY_SIZE;
			continue;
		}
		/*If <k,v> is not in whole block page, check for next available block page*/
		else{
			table_slot_counter++;
			if(table_slot_counter < num_slots_){
				/*Get the next block_page and unpin the current block_page*/
				page_slot_index = 0;
				page_slot_counter = 0;
				table_slot_index = (table_slot_index+1)%num_slots_;
				buffer_pool_manager_->UnpinPage(block_page_id,false);
				block_page_id = header_page_->GetBlockPageId(table_slot_index);
				block_page =  reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(buffer_pool_manager_->FetchPage(block_page_id,nullptr));
				continue;
			}
			else{
				/*If value is not found in whole table, return false*/
				return false;
			}
		}
	}
	return false;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  	return 0;
}


template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
