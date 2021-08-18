//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_header_page.cpp
//
// Identification: src/storage/page/hash_table_header_page.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_header_page.h"

namespace bustub {
page_id_t HashTableHeaderPage::GetBlockPageId(size_t index) {
	page_id_t block_pg_id = block_pageId_list_[index];
	return block_pg_id;
}

page_id_t HashTableHeaderPage::GetPageId() const {
	return page_id_;
}

void HashTableHeaderPage::SetPageId(bustub::page_id_t page_id) {
	this->page_id_ = page_id;
}

lsn_t HashTableHeaderPage::GetLSN() const {
	return lsn_;
}

void HashTableHeaderPage::SetLSN(lsn_t lsn) {
	this->lsn_ = lsn;
}

void HashTableHeaderPage::AddBlockPageId(page_id_t page_id) {
	/*Add page_id to end of vector. Page-id for ith block is at index i*/
	block_pageId_list_.push_back(page_id);
	block_counter_++;
}

size_t HashTableHeaderPage::NumBlocks() {
	return block_counter_;
}

void HashTableHeaderPage::SetSize(size_t size) {
	this->size_ = size;
	block_pageId_list_.reserve(size);
	// block_pageId_list_.fill(block_pageId_list_.begin(),block_pageId_list_.end(), -1);
}

size_t HashTableHeaderPage::GetSize() const {
	return size_;
}

}  // namespace bustub
