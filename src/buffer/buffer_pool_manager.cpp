//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"
#include <list>
#include <unordered_map>

/*Buffer pool holds the pages in the main memory.
Page table and page data table are used to manage pages currently in buffer pool.
Frames are the location of pages and pages are what actually stores the content
Pages are in a continuous array which can be indexed via frame id.*/

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}
//Frame id from free_list_ is same as page offset of buffer pool from which page can be taken to write/overwrite
Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

	frame_id_t page_frame;
	/*Check if page is already in buffer pool*/
	LOG_INFO("FetchPageImpl:: Fetch page id #%d",page_id);
	if(page_table_.find(page_id) != page_table_.end()){
		LOG_INFO("Page #%d found in buffer pool", page_id);
		frame_id_t page_frame = page_table_[page_id];
		Page *ref_page = &pages_[page_frame];
		if(ref_page->GetPinCount() == 0){
			replacer_->Pin(page_frame);
		}
		ref_page->pin_count_++;
		return ref_page;
	}

	/*Check if free page is available*/
	else if(!free_list_.empty()){
		LOG_INFO("Page #%d found in free list", page_id);
		frame_id_t page_frame = free_list_.front();
		free_list_.pop_front();
		Page *ref_page = &pages_[page_frame];
		ref_page->pin_count_=1;
		replacer_->Pin(page_frame);
		ref_page->page_id_ = page_id;
		char str_page[PAGE_SIZE];
		disk_manager_->ReadPage(page_id,str_page);
		std::strncpy(ref_page->data_,str_page,PAGE_SIZE);
		std::string str(str_page);
		page_data_table[page_id] = str;
		page_table_[page_id] = page_frame;
		return ref_page;
	}

	/*Or else get page from Replacer*/
	else if(replacer_->Victim(&page_frame)){
		LOG_INFO("Page #%d found in replacer", page_id);
		Page *ref_page = &pages_[page_frame];
		page_id_t old_pid = ref_page->GetPageId();
		char str_page[PAGE_SIZE];
		/*If page is dirty, flush it*/
		if(ref_page->IsDirty()){
			// std::strncpy(str_page,ref_page->GetData(),PAGE_SIZE);
			// disk_manager_->WritePage(old_pid,str_page);
			bool flushed = FlushPageImpl(old_pid);
			if(flushed){
				page_table_.erase(old_pid);
				page_data_table.erase(old_pid);
			}
		}
		else{
			page_table_.erase(old_pid);
			page_data_table.erase(old_pid);
		}
		/*Pin the page, add new details to the page and pin the corresponding frame*/
		ref_page->page_id_ = page_id;
		replacer_->Pin(page_frame);
		ref_page->pin_count_=1;
		disk_manager_->ReadPage(page_id,str_page);
		std::strncpy(ref_page->data_,str_page,PAGE_SIZE);
		std::string str(str_page);
		page_data_table[page_id] = str;
		page_table_[page_id] = page_frame;
		return ref_page;
	}

	return nullptr;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
	LOG_INFO("Page to be unpinned #%d",page_id);
	if(page_table_.find(page_id) != page_table_.end()){
		/*If page is pinned by one or more processes*/
		frame_id_t page_frame = page_table_[page_id];
		Page *ref_page = &pages_[page_frame];
		if(ref_page->GetPinCount() >= 1){
			ref_page->pin_count_ = ref_page->pin_count_ - 1;
			char str_page[PAGE_SIZE];
			/*Check if page is dirty*/
			std::strncpy(str_page,ref_page->GetData(),PAGE_SIZE);
			std::string str_pg(str_page);
			LOG_INFO("Data in page #%d is %s",page_id,str_pg.c_str());
			if(str_pg.compare(page_data_table[page_id]) != 0){
				LOG_INFO("Page #%d is dirty", page_id);
				is_dirty=true;
				ref_page->is_dirty_ = true;
			}
			else{
				LOG_INFO("Page #%d is not dirty", page_id);
				is_dirty=false;
				ref_page->is_dirty_ = false;
			}
			/*If page is no longer in use, remove it from buffer pool and
			add it to the replacer.*/
			if(ref_page->GetPinCount() == 0){
				LOG_INFO("Page #%d will be added to replacer and removed from buffer pool", page_id);
				replacer_->Unpin(page_frame);
			}
			return true;
		}
		else{
			is_dirty=false;
			return false;
		}
	}
	if(is_dirty){}
	is_dirty=false;
	return false;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  	/* Make sure you call DiskManager::WritePage! */
	if(page_id == INVALID_PAGE_ID)
		return false;
	else if(page_table_.find(page_id) == page_table_.end())
		return false;
	else{
		frame_id_t page_frame = page_table_[page_id];
		Page *ref_page = &pages_[page_frame];
		disk_manager_->WritePage(page_id,ref_page->GetData());
		LOG_INFO("Page #%d is flushed", page_id);
		return true;
	}
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  	// 0.   Make sure you call DiskManager::AllocatePage!
  	// 1.   If all the pages in the buffer pool are pinned, return nullptr.
  	// 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  	// 3.   Update P's metadata, zero out memory and add P to the page table.
  	// 4.   Set the page ID output parameter. Return a pointer to P.
	bool flag = 0;
	frame_id_t page_frame;
	page_id_t pid = disk_manager_->AllocatePage();
	LOG_INFO("New page #%d from replacer",pid);
	/*Check if there is space for new page in buffer pool*/
	for(auto it = page_table_.begin(); it != page_table_.end(); it++){
		Page *ref_page = &pages_[it->second];
		if(ref_page->GetPinCount() < 1){
			// LOG_INFO("Flag0");
			flag = 0;
			break;
		}
		else{
			// LOG_INFO("Flag1");
			flag = 1;
			continue;
		}
	}
	/*If there is no space, return nullptr*/
	if(flag == 1 && page_table_.size() == pool_size_){
		LOG_INFO("NewPageImpl::No space for new page");
		return nullptr;
	}
	/*Get page from free list*/
	if(!free_list_.empty()){
		frame_id_t page_frame = free_list_.front();
		free_list_.pop_front();
		LOG_INFO("New page #%d from free list",pid);
		Page *ref_page = &pages_[page_frame];
		*page_id = pid;
		/*Set parameters for the page*/
		ref_page->page_id_ = pid;
		ref_page->pin_count_ = 1;
		ref_page->ResetMemory();
		ref_page->is_dirty_ = false;
		/*Put the page in page table and page data table
		Insert empty string for page data table*/
		replacer_->Pin(page_frame);
		page_table_[pid] = page_frame;
		page_data_table[*page_id] = "";
		return ref_page;
	}
	/*Or else get page from Replacer*/
	else if(replacer_->Victim(&page_frame)){
		Page *ref_page = &pages_[page_frame];
		*page_id = pid;
		page_id_t old_pid = ref_page->page_id_;
		/*If page is dirty, flush it*/
		if(ref_page->IsDirty()){
			bool flushed = FlushPageImpl(old_pid);
			LOG_INFO("Page #%d, frame #%d", old_pid,page_frame);
			if(flushed){
				page_table_.erase(old_pid);
				page_data_table.erase(old_pid);
			}
		}
		else{
			page_table_.erase(old_pid);
			page_data_table.erase(old_pid);
		}
		/*Set parameters for the page*/
		ref_page->pin_count_ = 1;
		ref_page->page_id_ = pid;
		ref_page->ResetMemory();
		ref_page->is_dirty_ = false;
		/*Put the page in page table and page data table
		Insert empty string for page data table*/
		page_data_table[*page_id] = "";
		page_table_[pid] = page_frame;
		replacer_->Pin(page_frame);
		return ref_page;
	}
	return nullptr;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
	if(page_table_.find(page_id) != page_table_.end()){
		frame_id_t page_frame = page_table_[page_id];
		Page *ref_page = &pages_[page_frame];
		if(ref_page->GetPinCount() < 1){
			LOG_INFO("Deleting page #%d",page_id);
			ref_page->ResetMemory();
			page_table_.erase(page_id);
			page_data_table.erase(page_id);
			free_list_.push_back(page_frame);
			return true;
		}
		else{
			LOG_INFO("Page #%d cannot be deleted because it is in use",page_id);
			return false;
		}
	}
	return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
	/*Go through the whole page table and flush each page*/
	for(auto it=page_table_.begin();it != page_table_.end();it++){
		FlushPage(it->first);
	}
}

}
