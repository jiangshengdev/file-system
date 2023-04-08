use std::collections::VecDeque;
use std::mem::size_of;
use std::sync::Arc;

use lazy_static::lazy_static;
use spin::Mutex;

use crate::block_device::BlockDevice;
use crate::{nop, BLOCK_SZ};

/// 内存中的缓存块
pub struct BlockCache {
    /// 缓存块数据
    cache: [u8; BLOCK_SZ],

    /// 底层块ID
    block_id: usize,

    /// 底层块设备
    block_device: Arc<dyn BlockDevice>,

    /// 该块是否为脏块
    modified: bool,
}

impl BlockCache {
    /// 从磁盘加载一个新的块缓存
    ///
    /// # Arguments
    ///
    /// * `block_id`: 块ID
    /// * `block_device`: 块设备
    ///
    /// returns: BlockCache 块缓存
    pub fn new(block_id: usize, block_device: Arc<dyn BlockDevice>) -> Self {
        let mut cache = [0u8; BLOCK_SZ];
        block_device.read_block(block_id, &mut cache);
        Self {
            cache,
            block_id,
            block_device,
            modified: false,
        }
    }

    /// 获取缓存块数据内的偏移地址
    ///
    /// # Arguments
    ///
    /// * `offset`: 偏移量
    ///
    /// returns: usize 偏移地址
    fn addr_of_offset(&self, offset: usize) -> usize {
        &self.cache[offset] as *const _ as usize
    }

    pub fn get_ref<T>(&self, offset: usize) -> &T
    where
        T: Sized,
    {
        let type_size = size_of::<T>();
        assert!(offset + type_size <= BLOCK_SZ);
        let addr = self.addr_of_offset(offset);
        nop();
        unsafe { &*(addr as *const T) }
    }

    pub fn get_mut<T>(&mut self, offset: usize) -> &mut T
    where
        T: Sized,
    {
        let type_size = size_of::<T>();
        assert!(offset + type_size <= BLOCK_SZ);
        self.modified = true;
        let addr = self.addr_of_offset(offset);
        nop();
        unsafe { &mut *(addr as *mut T) }
    }

    pub fn read<T, V>(&self, offset: usize, f: impl FnOnce(&T) -> V) -> V {
        let value = self.get_ref(offset);
        f(value)
    }

    pub fn modify<T, V>(&mut self, offset: usize, f: impl FnOnce(&mut T) -> V) -> V {
        let value = self.get_mut(offset);
        f(value)
    }

    pub fn sync(&mut self) {
        if self.modified {
            self.modified = false;
            self.block_device.write_block(self.block_id, &self.cache);
        }
    }
}

impl Drop for BlockCache {
    fn drop(&mut self) {
        self.sync()
    }
}

/// 块缓存的大小
const BLOCK_CACHE_SIZE: usize = 16;

pub struct BlockCacheManager {
    queue: VecDeque<(usize, Arc<Mutex<BlockCache>>)>,
}

impl BlockCacheManager {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
        }
    }

    /// 获取块缓存
    ///
    /// # Arguments
    ///
    /// * `block_id`: 块ID
    /// * `block_device`: 块设备
    ///
    /// returns: Arc<Mutex<BlockCache, Spin>> 块缓存
    pub fn get_block_cache(
        &mut self,
        block_id: usize,
        block_device: Arc<dyn BlockDevice>,
    ) -> Arc<Mutex<BlockCache>> {
        if let Some(pair) = self.queue.iter().find(|pair| {
            let current_id = pair.0;
            let same = current_id == block_id;
            nop();
            same
        }) {
            let old = pair.1.clone();
            nop();
            old
        } else {
            // 替换
            if self.queue.len() == BLOCK_CACHE_SIZE {
                // 从头到尾
                if let Some((idx, _)) = self.queue.iter().enumerate().find(|(_, pair)| {
                    let count = Arc::strong_count(&pair.1);
                    let free = count == 1;
                    nop();
                    free
                }) {
                    let range = idx..=idx;
                    self.queue.drain(range);
                } else {
                    panic!("Run out of BlockCache!");
                }
            }
            // 将块加载到内存，并推入队列
            let cache = BlockCache::new(block_id, block_device.clone());
            let new = Arc::new(Mutex::new(cache));
            self.queue.push_back((block_id, new.clone()));
            new
        }
    }
}

lazy_static! {
    /// 全局块缓存管理器
    pub static ref BLOCK_CACHE_MANAGER: Mutex<BlockCacheManager> =
        Mutex::new(BlockCacheManager::new());
}

/// 获取块缓存
///
/// # Arguments
///
/// * `block_id`: 块ID
/// * `block_device`: 块设备
///
/// returns: Arc<Mutex<BlockCache, Spin>> 块缓存
pub fn get_block_cache(
    block_id: usize,
    block_device: Arc<dyn BlockDevice>,
) -> Arc<Mutex<BlockCache>> {
    BLOCK_CACHE_MANAGER
        .lock()
        .get_block_cache(block_id, block_device)
}

/// 将所有块缓存同步到块设备
pub fn block_cache_sync_all() {
    let manager = BLOCK_CACHE_MANAGER.lock();
    for (_, cache) in manager.queue.iter() {
        cache.lock().sync();
    }
}
