use std::sync::Arc;

use crate::block_cache::get_block_cache;
use crate::block_device::BlockDevice;
use crate::BLOCK_SZ;

/// 位图块
type BitmapBlock = [u64; 64];

/// 一个块中的比特数
const BLOCK_BITS: usize = BLOCK_SZ * 8;

/// Decompose bits into (block_pos, bits64_pos, inner_pos)
fn decomposition(mut bit: usize) -> (usize, usize, usize) {
    let block_pos = bit / BLOCK_BITS;
    bit %= BLOCK_BITS;
    (block_pos, bit / 64, bit % 64)
}

#[derive(Debug)]
/// 位图
pub struct Bitmap {
    /// 起始块ID
    start_block_id: usize,

    /// 块数
    blocks: usize,
}

impl Bitmap {
    /// 从起始块ID和块数创建一个新的位图
    ///
    /// # Arguments
    ///
    /// * `start_block_id`: 起始块ID
    /// * `blocks`: 块数
    ///
    /// returns: Bitmap 位图
    pub fn new(start_block_id: usize, blocks: usize) -> Self {
        Self {
            start_block_id,
            blocks,
        }
    }

    /// 从块设备中分配一个新的块
    ///
    /// # Arguments
    ///
    /// * `block_device`: 块设备
    ///
    /// returns: Option<usize> 块ID
    pub fn alloc(&self, block_device: &Arc<dyn BlockDevice>) -> Option<usize> {
        for block_id in 0..self.blocks {
            let id = block_id + self.start_block_id;
            let cache = get_block_cache(id, block_device.clone());
            let pos = cache.lock().modify(0, |bitmap_block: &mut BitmapBlock| {
                if let Some((bits64_pos, inner_pos)) = bitmap_block
                    .iter()
                    .enumerate()
                    .find(|(_, bits64)| {
                        let value = **bits64;
                        value != u64::MAX
                    })
                    .map(|(bits64_pos, bits64)| {
                        let inner_pos = bits64.trailing_ones() as usize;
                        (bits64_pos, inner_pos)
                    })
                {
                    // 修改缓存
                    let value = 1u64 << inner_pos;
                    bitmap_block[bits64_pos] |= value;
                    let result = block_id * BLOCK_BITS + bits64_pos * 64 + inner_pos;
                    Some(result)
                } else {
                    None
                }
            });
            if pos.is_some() {
                return pos;
            }
        }
        None
    }

    /// Deallocate a block
    pub fn dealloc(&self, block_device: &Arc<dyn BlockDevice>, bit: usize) {
        let (block_pos, bits64_pos, inner_pos) = decomposition(bit);
        get_block_cache(block_pos + self.start_block_id, Arc::clone(block_device))
            .lock()
            .modify(0, |bitmap_block: &mut BitmapBlock| {
                assert!(bitmap_block[bits64_pos] & (1u64 << inner_pos) > 0);
                bitmap_block[bits64_pos] -= 1u64 << inner_pos;
            });
    }

    /// 获取可分配块的最大数量
    pub fn maximum(&self) -> usize {
        self.blocks * BLOCK_BITS
    }
}
