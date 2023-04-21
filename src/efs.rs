use std::mem::size_of;
use std::sync::Arc;

use spin::Mutex;

use crate::bitmap::Bitmap;
use crate::block_cache::{block_cache_sync_all, get_block_cache};
use crate::block_device::BlockDevice;
use crate::layout::{DiskInode, DiskInodeType, SuperBlock};
use crate::vfs::Inode;
use crate::{nop, BLOCK_SZ};

#[derive(Debug)]
/// 简易块式文件系统
pub struct EasyFileSystem {
    /// 真实块设备
    pub block_device: Arc<dyn BlockDevice>,

    /// 索引节点位图
    pub inode_bitmap: Bitmap,

    /// 数据位图
    pub data_bitmap: Bitmap,

    /// 索引节点区域起始块ID
    inode_area_start_block: u32,

    /// 数据区域起始块ID
    data_area_start_block: u32,
}

/// 数据块
type DataBlock = [u8; BLOCK_SZ];

impl EasyFileSystem {
    /// 创建指定块大小的简易文件系统
    ///
    /// # Arguments
    ///
    /// * `block_device`: 块设备
    /// * `total_blocks`: 总块数
    /// * `inode_bitmap_blocks`: 索引节点位图块数
    ///
    /// returns: Arc<Mutex<EasyFileSystem, Spin>> 简易文件系统
    pub fn create(
        block_device: Arc<dyn BlockDevice>,
        total_blocks: u32,
        inode_bitmap_blocks: u32,
    ) -> Arc<Mutex<Self>> {
        //region 计算区域的块大小并创建位图

        // 索引节点位图
        let inode_bitmap = Bitmap::new(1, inode_bitmap_blocks as usize);

        // 索引节点比特数
        let inode_num = inode_bitmap.maximum();

        // 128 字节
        let size_of_disk_inode = size_of::<DiskInode>();

        // 索引节点区域块数
        let inode_area_blocks = ((inode_num * size_of_disk_inode + BLOCK_SZ - 1) / BLOCK_SZ) as u32;

        // 索引节点总块数
        let inode_total_blocks = inode_bitmap_blocks + inode_area_blocks;

        // 数据总块数
        let data_total_blocks = total_blocks - 1 - inode_total_blocks;

        // 数据位图块数
        let data_bitmap_blocks = (data_total_blocks + 4096) / 4097;

        // 数据区域块数
        let data_area_blocks = data_total_blocks - data_bitmap_blocks;

        // 数据位图
        let data_bitmap = Bitmap::new(
            (1 + inode_total_blocks) as usize,
            data_bitmap_blocks as usize,
        );

        let mut efs = Self {
            block_device: block_device.clone(),
            inode_bitmap,
            data_bitmap,
            inode_area_start_block: 1 + inode_bitmap_blocks,
            data_area_start_block: 1 + inode_total_blocks + data_bitmap_blocks,
        };
        //endregion

        //region 清空所有块
        for i in 0..total_blocks {
            let cache = get_block_cache(i as usize, block_device.clone());
            cache.lock().modify(0, |data_block: &mut DataBlock| {
                for byte in data_block.iter_mut() {
                    *byte = 0;
                }
            });
        }
        //endregion

        //region 初始化超级块
        let cache = get_block_cache(0, block_device.clone());
        cache.lock().modify(0, |super_block: &mut SuperBlock| {
            super_block.initialize(
                total_blocks,
                inode_bitmap_blocks,
                inode_area_blocks,
                data_bitmap_blocks,
                data_area_blocks,
            );
            nop();
        });
        //endregion

        //region 为根节点创建索引节点
        assert_eq!(efs.alloc_inode(), 0);
        let (root_inode_block_id, root_inode_offset) = efs.get_disk_inode_pos(0);
        let block_cache = get_block_cache(root_inode_block_id as usize, block_device.clone());
        block_cache
            .lock()
            .modify(root_inode_offset, |disk_inode: &mut DiskInode| {
                disk_inode.initialize(DiskInodeType::Directory);
            });
        //endregion

        //region 立即写回
        block_cache_sync_all();
        //endregion

        Arc::new(Mutex::new(efs))
    }

    /// 将一个块设备作为文件系统打开
    ///
    /// # Arguments
    ///
    /// * `block_device`: 块设备
    ///
    /// returns: Arc<Mutex<EasyFileSystem, Spin>> 简易文件系统
    pub fn open(block_device: Arc<dyn BlockDevice>) -> Arc<Mutex<Self>> {
        // 读取超级块
        let cache = get_block_cache(0, block_device.clone());

        let ret = cache.lock().read(0, |super_block: &SuperBlock| {
            // 检查超级块
            assert!(super_block.is_valid(), "Error loading EFS!");

            // 索引节点总块数
            let inode_total_blocks =
                super_block.inode_bitmap_blocks + super_block.inode_area_blocks;

            // 索引节点位图
            let inode_bitmap = Bitmap::new(1, super_block.inode_bitmap_blocks as usize);

            // 数据位图
            let data_bitmap = Bitmap::new(
                (1 + inode_total_blocks) as usize,
                super_block.data_bitmap_blocks as usize,
            );

            // 索引节点区域起始块ID
            let inode_area_start_block = 1 + super_block.inode_bitmap_blocks;

            // 数据区域起始块ID
            let data_area_start_block = 1 + inode_total_blocks + super_block.data_bitmap_blocks;

            // 简易文件系统
            let efs = Self {
                block_device,
                inode_bitmap,
                data_bitmap,
                inode_area_start_block,
                data_area_start_block,
            };

            Arc::new(Mutex::new(efs))
        });

        ret
    }

    /// 获取文件系统的根节点
    ///
    /// # Arguments
    ///
    /// * `efs`: 简易文件系统
    ///
    /// returns: Inode 索引节点
    pub fn root_inode(efs: &Arc<Mutex<Self>>) -> Inode {
        let block_device = efs.lock().block_device.clone();
        // 暂时获得简易文件系统锁
        let (block_id, block_offset) = efs.lock().get_disk_inode_pos(0);
        // 释放简易文件系统锁
        Inode::new(block_id, block_offset, efs.clone(), block_device)
    }

    /// 按ID获取索引节点
    ///
    /// # Arguments
    ///
    /// * `inode_id`: 索引节点ID
    ///
    /// returns: (u32, usize) 索引节点所在块ID和偏移
    pub fn get_disk_inode_pos(&self, inode_id: u32) -> (u32, usize) {
        // 磁盘索引节点字节数
        // 128 字节
        let inode_size = size_of::<DiskInode>();

        // 每个块中的索引节点数
        // 4
        let inodes_per_block = (BLOCK_SZ / inode_size) as u32;

        // 索引节点所在块ID
        let block_id = self.inode_area_start_block + inode_id / inodes_per_block;

        // 索引节点在块中的偏移
        let offset = (inode_id % inodes_per_block) as usize * inode_size;

        (block_id, offset)
    }

    /// 分配一个新索引节点
    pub fn alloc_inode(&mut self) -> u32 {
        self.inode_bitmap.alloc(&self.block_device).unwrap() as u32
    }

    /// 分配一个数据块
    pub fn alloc_data(&mut self) -> u32 {
        self.data_bitmap.alloc(&self.block_device).unwrap() as u32 + self.data_area_start_block
    }

    /// Deallocate a data block
    pub fn dealloc_data(&mut self, block_id: u32) {
        get_block_cache(block_id as usize, Arc::clone(&self.block_device))
            .lock()
            .modify(0, |data_block: &mut DataBlock| {
                data_block.iter_mut().for_each(|p| {
                    *p = 0;
                })
            });
        self.data_bitmap.dealloc(
            &self.block_device,
            (block_id - self.data_area_start_block) as usize,
        )
    }
}
