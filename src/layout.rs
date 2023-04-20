use std::sync::Arc;

use crate::block_cache::get_block_cache;
use crate::block_device::BlockDevice;
use crate::{nop, BLOCK_SZ};

/// 简易文件系统的魔数
const EFS_MAGIC: u32 = 0x3b800001;

/// 直接索引节点的最大数量
const INODE_DIRECT_COUNT: usize = 28;

/// 索引节点名称的最大长度
const NAME_LENGTH_LIMIT: usize = 27;

/// 一级间接索引节点的最大数量
const INODE_INDIRECT1_COUNT: usize = BLOCK_SZ / 4;

/// 二级间接索引节点的最大数量
const INODE_INDIRECT2_COUNT: usize = INODE_INDIRECT1_COUNT * INODE_INDIRECT1_COUNT;

/// 直接索引节点的上界
const DIRECT_BOUND: usize = INODE_DIRECT_COUNT;

/// 一级间接索引节点的上界
const INDIRECT1_BOUND: usize = DIRECT_BOUND + INODE_INDIRECT1_COUNT;

/// 二级间接索引节点的上界
#[allow(unused)]
const INDIRECT2_BOUND: usize = INDIRECT1_BOUND + INODE_INDIRECT2_COUNT;

/// 文件系统超级块
#[repr(C)]
#[derive(Debug)]
pub struct SuperBlock {
    /// 魔数
    magic: u32,

    /// 总块数
    pub total_blocks: u32,

    /// 索引节点位图块数
    pub inode_bitmap_blocks: u32,

    /// 索引节点区域块数
    pub inode_area_blocks: u32,

    /// 数据位图块数
    pub data_bitmap_blocks: u32,

    /// 数据区域块数
    pub data_area_blocks: u32,
}

impl SuperBlock {
    /// 初始化超级块
    ///
    /// # Arguments
    ///
    /// * `total_blocks`: 总块数
    /// * `inode_bitmap_blocks`: 索引节点位图块数
    /// * `inode_area_blocks`: 索引节点区域块数
    /// * `data_bitmap_blocks`: 数据位图块数
    /// * `data_area_blocks`: 数据区域块数
    pub fn initialize(
        &mut self,
        total_blocks: u32,
        inode_bitmap_blocks: u32,
        inode_area_blocks: u32,
        data_bitmap_blocks: u32,
        data_area_blocks: u32,
    ) {
        *self = Self {
            magic: EFS_MAGIC,
            total_blocks,
            inode_bitmap_blocks,
            inode_area_blocks,
            data_bitmap_blocks,
            data_area_blocks,
        }
    }

    /// 使用魔数检查超级块是否有效
    pub fn is_valid(&self) -> bool {
        self.magic == EFS_MAGIC
    }
}

/// 磁盘索引节点的类型
#[derive(PartialEq)]
pub enum DiskInodeType {
    /// 文件
    File,

    /// 目录
    Directory,
}

/// 间接索引块
type IndirectBlock = [u32; BLOCK_SZ / 4];

/// 数据块
type DataBlock = [u8; BLOCK_SZ];

/// 磁盘索引节点
#[repr(C)]
pub struct DiskInode {
    /// 文件大小
    pub size: u32,

    /// 直接索引节点
    pub direct: [u32; INODE_DIRECT_COUNT],

    /// 一级间接索引节点
    pub indirect1: u32,

    /// 二级间接索引节点
    pub indirect2: u32,

    /// 索引节点类型
    type_: DiskInodeType,
}

impl DiskInode {
    /// 初始化一个磁盘索引节点，以及直接索引节点
    /// 间接索引节点只有在需要时才分配
    ///
    /// # Arguments
    ///
    /// * `type_`: 索引节点类型
    pub fn initialize(&mut self, type_: DiskInodeType) {
        self.size = 0;
        self.direct.iter_mut().for_each(|v| *v = 0);
        self.indirect1 = 0;
        self.indirect2 = 0;
        self.type_ = type_;
    }

    /// 这个磁盘索引节点是否是一个目录
    pub fn is_dir(&self) -> bool {
        self.type_ == DiskInodeType::Directory
    }

    /// 返回与当前数据大小对应的块数
    pub fn data_blocks(&self) -> u32 {
        Self::_data_blocks(self.size)
    }

    /// 返回需要的块数
    ///
    /// # Arguments
    ///
    /// * `size`: 字节数
    ///
    /// returns: u32 块数
    fn _data_blocks(size: u32) -> u32 {
        (size + BLOCK_SZ as u32 - 1) / BLOCK_SZ as u32
    }

    /// 返回需要的块数，包括间接索引节点
    ///
    /// # Arguments
    ///
    /// * `size`: 字节数
    ///
    /// returns: u32 块数
    pub fn total_blocks(size: u32) -> u32 {
        let data_blocks = Self::_data_blocks(size) as usize;
        let mut total = data_blocks as usize;
        // indirect1
        if data_blocks > INODE_DIRECT_COUNT {
            total += 1;
        }
        // indirect2
        if data_blocks > INDIRECT1_BOUND {
            total += 1;
            // sub indirect1
            total +=
                (data_blocks - INDIRECT1_BOUND + INODE_INDIRECT1_COUNT - 1) / INODE_INDIRECT1_COUNT;
        }
        total as u32
    }

    /// 获取在新的数据大小下需要分配的数据块数
    ///
    /// # Arguments
    ///
    /// * `new_size`: 新的数据大小
    ///
    /// returns: u32 新增块数
    pub fn blocks_num_needed(&self, new_size: u32) -> u32 {
        assert!(new_size >= self.size);
        Self::total_blocks(new_size) - Self::total_blocks(self.size)
    }

    /// 获取给定内部 ID 的块 ID
    ///
    /// # Arguments
    ///
    /// * `inner_id`: 内部 ID
    /// * `block_device`: 块设备
    ///
    /// returns: u32 块 ID
    pub fn get_block_id(&self, inner_id: u32, block_device: &Arc<dyn BlockDevice>) -> u32 {
        let inner_id = inner_id as usize;
        if inner_id < INODE_DIRECT_COUNT {
            self.direct[inner_id]
        } else if inner_id < INDIRECT1_BOUND {
            get_block_cache(self.indirect1 as usize, Arc::clone(block_device))
                .lock()
                .read(0, |indirect_block: &IndirectBlock| {
                    indirect_block[inner_id - INODE_DIRECT_COUNT]
                })
        } else {
            let last = inner_id - INDIRECT1_BOUND;
            let indirect1 = get_block_cache(self.indirect2 as usize, Arc::clone(block_device))
                .lock()
                .read(0, |indirect2: &IndirectBlock| {
                    indirect2[last / INODE_INDIRECT1_COUNT]
                });
            get_block_cache(indirect1 as usize, Arc::clone(block_device))
                .lock()
                .read(0, |indirect1: &IndirectBlock| {
                    indirect1[last % INODE_INDIRECT1_COUNT]
                })
        }
    }

    /// 扩容当前磁盘索引节点的大小
    ///
    /// # Arguments
    ///
    /// * `new_size`: 新的大小
    /// * `new_blocks`: 新分配的块
    /// * `block_device`: 块设备
    pub fn increase_size(
        &mut self,
        new_size: u32,
        new_blocks: Vec<u32>,
        block_device: &Arc<dyn BlockDevice>,
    ) {
        let mut current_blocks = self.data_blocks();
        self.size = new_size;
        let mut total_blocks = self.data_blocks();
        let mut new_blocks = new_blocks.into_iter();

        // 填充直接索引节点
        while current_blocks < total_blocks.min(INODE_DIRECT_COUNT as u32) {
            self.direct[current_blocks as usize] = new_blocks.next().unwrap();
            current_blocks += 1;
        }

        // 分配一级间接索引节点
        if total_blocks > INODE_DIRECT_COUNT as u32 {
            if current_blocks == INODE_DIRECT_COUNT as u32 {
                self.indirect1 = new_blocks.next().unwrap();
            }
            current_blocks -= INODE_DIRECT_COUNT as u32;
            total_blocks -= INODE_DIRECT_COUNT as u32;
        } else {
            return;
        }

        // 填充一级间接索引节点
        get_block_cache(self.indirect1 as usize, Arc::clone(block_device))
            .lock()
            .modify(0, |indirect1: &mut IndirectBlock| {
                while current_blocks < total_blocks.min(INODE_INDIRECT1_COUNT as u32) {
                    indirect1[current_blocks as usize] = new_blocks.next().unwrap();
                    current_blocks += 1;
                }
            });

        // 分配二级间接索引节点
        if total_blocks > INODE_INDIRECT1_COUNT as u32 {
            if current_blocks == INODE_INDIRECT1_COUNT as u32 {
                self.indirect2 = new_blocks.next().unwrap();
            }
            current_blocks -= INODE_INDIRECT1_COUNT as u32;
            total_blocks -= INODE_INDIRECT1_COUNT as u32;
        } else {
            return;
        }

        // 填充二级间接索引节点从 (a0, b0) -> (a1, b1)
        let mut a0 = current_blocks as usize / INODE_INDIRECT1_COUNT;
        let mut b0 = current_blocks as usize % INODE_INDIRECT1_COUNT;
        let a1 = total_blocks as usize / INODE_INDIRECT1_COUNT;
        let b1 = total_blocks as usize % INODE_INDIRECT1_COUNT;

        // 分配低等级的一级间接索引节点
        get_block_cache(self.indirect2 as usize, Arc::clone(block_device))
            .lock()
            .modify(0, |indirect2: &mut IndirectBlock| {
                while (a0 < a1) || (a0 == a1 && b0 < b1) {
                    if b0 == 0 {
                        indirect2[a0] = new_blocks.next().unwrap();
                    }

                    // 填充当前
                    get_block_cache(indirect2[a0] as usize, Arc::clone(block_device))
                        .lock()
                        .modify(0, |indirect1: &mut IndirectBlock| {
                            indirect1[b0] = new_blocks.next().unwrap();
                        });

                    // 移动到下一个
                    b0 += 1;
                    if b0 == INODE_INDIRECT1_COUNT {
                        b0 = 0;
                        a0 += 1;
                    }
                }
            });
    }

    /// 从当前磁盘索引节点中读取数据
    ///
    /// # Arguments
    ///
    /// * `offset`: 偏移
    /// * `buf`: 缓冲区
    /// * `block_device`: 块设备
    ///
    /// returns: usize 读取的字节数
    pub fn read_at(
        &self,
        offset: usize,
        buf: &mut [u8],
        block_device: &Arc<dyn BlockDevice>,
    ) -> usize {
        let mut start = offset;
        let end = (offset + buf.len()).min(self.size as usize);
        if start >= end {
            return 0;
        }
        let mut start_block = start / BLOCK_SZ;
        let mut read_size = 0usize;
        loop {
            // 计算当前块的结尾
            let mut end_current_block = (start / BLOCK_SZ + 1) * BLOCK_SZ;
            end_current_block = end_current_block.min(end);

            // 读取并更新读取大小
            let block_read_size = end_current_block - start;
            let dst = &mut buf[read_size..read_size + block_read_size];
            get_block_cache(
                self.get_block_id(start_block as u32, block_device) as usize,
                Arc::clone(block_device),
            )
            .lock()
            .read(0, |data_block: &DataBlock| {
                let src = &data_block[start % BLOCK_SZ..start % BLOCK_SZ + block_read_size];
                dst.copy_from_slice(src);
            });
            read_size += block_read_size;

            // 移动到下一个块
            if end_current_block == end {
                break;
            }
            start_block += 1;
            start = end_current_block;
        }
        read_size
    }

    /// 写入数据到当前磁盘索引节点
    /// 大小必须在调用前调整
    ///
    /// # Arguments
    ///
    /// * `offset`: 偏移
    /// * `buf`: 缓冲区
    /// * `block_device`: 块设备
    ///
    /// returns: usize 写入的字节数
    pub fn write_at(
        &mut self,
        offset: usize,
        buf: &[u8],
        block_device: &Arc<dyn BlockDevice>,
    ) -> usize {
        let mut start = offset;
        let end = (offset + buf.len()).min(self.size as usize);
        assert!(start <= end);
        let mut start_block = start / BLOCK_SZ;
        let mut write_size = 0usize;
        loop {
            // 计算当前块的结尾
            let mut end_current_block = (start / BLOCK_SZ + 1) * BLOCK_SZ;
            end_current_block = end_current_block.min(end);

            // 写入并更新写入大小
            let block_write_size = end_current_block - start;
            let cache = get_block_cache(
                self.get_block_id(start_block as u32, block_device) as usize,
                block_device.clone(),
            );
            cache.lock().modify(0, |data_block: &mut DataBlock| {
                let src = &buf[write_size..write_size + block_write_size];
                let dst = &mut data_block[start % BLOCK_SZ..start % BLOCK_SZ + block_write_size];
                dst.copy_from_slice(src);
                nop();
            });
            write_size += block_write_size;

            // 移动到下一个块
            if end_current_block == end {
                break;
            }
            start_block += 1;
            start = end_current_block;
        }
        write_size
    }
}

/// 一个目录条目
#[repr(C)]
pub struct DirEntry {
    name: [u8; NAME_LENGTH_LIMIT + 1],
    inode_number: u32,
}

/// 一个目录条目的大小
pub const DIRENT_SZ: usize = 32;

impl DirEntry {
    /// 创建一个空的目录条目
    pub fn empty() -> Self {
        Self {
            name: [0u8; NAME_LENGTH_LIMIT + 1],
            inode_number: 0,
        }
    }

    /// 根据名称和索引节点号创建一个目录条目
    ///
    /// # Arguments
    ///
    /// * `name`: 文件名
    /// * `inode_number`: 索引节点号
    ///
    /// returns: DirEntry 目录条目
    pub fn new(name: &str, inode_number: u32) -> Self {
        let mut bytes = [0u8; NAME_LENGTH_LIMIT + 1];
        bytes[..name.len()].copy_from_slice(name.as_bytes());
        Self {
            name: bytes,
            inode_number,
        }
    }

    /// 序列化为不可变字节
    pub fn as_bytes(&self) -> &[u8] {
        unsafe { core::slice::from_raw_parts(self as *const _ as usize as *const u8, DIRENT_SZ) }
    }

    /// 序列化为可变字节
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe { core::slice::from_raw_parts_mut(self as *mut _ as usize as *mut u8, DIRENT_SZ) }
    }

    /// 获取条目的名称
    pub fn name(&self) -> &str {
        let len = (0usize..).find(|i| self.name[*i] == 0).unwrap();
        core::str::from_utf8(&self.name[..len]).unwrap()
    }

    /// 获取条目的索引节点号
    pub fn inode_number(&self) -> u32 {
        self.inode_number
    }
}
