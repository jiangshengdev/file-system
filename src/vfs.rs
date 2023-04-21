use std::sync::Arc;

use spin::{Mutex, MutexGuard};

use crate::block_cache::{block_cache_sync_all, get_block_cache};
use crate::block_device::BlockDevice;
use crate::efs::EasyFileSystem;
use crate::layout::{DirEntry, DiskInode, DiskInodeType, DIRENT_SZ};
use crate::nop;

/// 简易文件系统之上的虚拟文件系统层
pub struct Inode {
    /// 块ID
    block_id: usize,

    /// 块内偏移
    block_offset: usize,

    /// 文件系统
    fs: Arc<Mutex<EasyFileSystem>>,

    /// 块设备
    block_device: Arc<dyn BlockDevice>,
}

impl Inode {
    /// 创建一个虚拟文件系统索引节点
    ///
    /// # Arguments
    ///
    /// * `block_id`: 块ID
    /// * `block_offset`: 块内偏移
    /// * `fs`: 文件系统
    /// * `block_device`: 块设备
    ///
    /// returns: Inode 索引节点
    pub fn new(
        block_id: u32,
        block_offset: usize,
        fs: Arc<Mutex<EasyFileSystem>>,
        block_device: Arc<dyn BlockDevice>,
    ) -> Self {
        Self {
            block_id: block_id as usize,
            block_offset,
            fs,
            block_device,
        }
    }

    /// 在磁盘索引节点上调用一个函数来读取它
    ///
    /// # Arguments
    ///
    /// * `f`: 回调函数
    ///
    /// returns: V 回调函数的返回值
    fn read_disk_inode<V>(&self, f: impl FnOnce(&DiskInode) -> V) -> V {
        let cache = get_block_cache(self.block_id, self.block_device.clone());
        let ret = cache.lock().read(self.block_offset, f);
        nop();
        ret
    }

    /// 在磁盘索引节点上调用一个函数来修改它
    ///
    /// # Arguments
    ///
    /// * `f`: 回调函数
    ///
    /// returns: V 回调函数的返回值
    fn modify_disk_inode<V>(&self, f: impl FnOnce(&mut DiskInode) -> V) -> V {
        let cache = get_block_cache(self.block_id, self.block_device.clone());
        let ret = cache.lock().modify(self.block_offset, f);
        nop();
        ret
    }

    /// 在磁盘索引节点下通过名称查找索引节点
    ///
    /// # Arguments
    ///
    /// * `name`: 文件名
    /// * `disk_inode`: 磁盘索引节点
    ///
    /// returns: Option<u32> 索引节点ID
    fn find_inode_id(&self, name: &str, disk_inode: &DiskInode) -> Option<u32> {
        // 断言是一个目录
        assert!(disk_inode.is_dir());
        let file_count = (disk_inode.size as usize) / DIRENT_SZ;
        let mut dirent = DirEntry::empty();
        for i in 0..file_count {
            assert_eq!(
                disk_inode.read_at(DIRENT_SZ * i, dirent.as_bytes_mut(), &self.block_device),
                DIRENT_SZ,
            );
            if dirent.name() == name {
                return Some(dirent.inode_number() as u32);
            }
        }
        None
    }

    /// 在当前索引节点下按名称查找索引节点
    pub fn find(&self, name: &str) -> Option<Arc<Inode>> {
        let fs = self.fs.lock();
        self.read_disk_inode(|disk_inode| {
            self.find_inode_id(name, disk_inode).map(|inode_id| {
                let (block_id, block_offset) = fs.get_disk_inode_pos(inode_id);
                Arc::new(Self::new(
                    block_id,
                    block_offset,
                    self.fs.clone(),
                    self.block_device.clone(),
                ))
            })
        })
    }

    /// 扩容磁盘索引节点
    ///
    /// # Arguments
    ///
    /// * `new_size`: 新的大小
    /// * `disk_inode`: 磁盘索引节点
    /// * `fs`: 文件系统
    fn increase_size(
        &self,
        new_size: u32,
        disk_inode: &mut DiskInode,
        fs: &mut MutexGuard<EasyFileSystem>,
    ) {
        if new_size < disk_inode.size {
            return;
        }
        let blocks_needed = disk_inode.blocks_num_needed(new_size);
        let mut v: Vec<u32> = Vec::new();
        for _ in 0..blocks_needed {
            v.push(fs.alloc_data());
        }
        disk_inode.increase_size(new_size, v, &self.block_device);
    }

    /// 在当前索引节点下按名称创建索引节点
    ///
    /// # Arguments
    ///
    /// * `name`: 文件名
    ///
    /// returns: Option<Arc<Inode>> 索引节点
    pub fn create(&self, name: &str) -> Option<Arc<Inode>> {
        let mut fs = self.fs.lock();
        let op = |root_inode: &DiskInode| {
            // 断言根索引节点是一个目录
            assert!(root_inode.is_dir());

            // 当前文件是否已经创建
            self.find_inode_id(name, root_inode)
        };

        // 如果文件已经存在，返回 None
        if self.read_disk_inode(op).is_some() {
            return None;
        }

        // 创建一个新文件
        // 在间接块中分配一个索引节点
        let new_inode_id = fs.alloc_inode();

        // 初始化索引节点
        let (new_inode_block_id, new_inode_block_offset) = fs.get_disk_inode_pos(new_inode_id);
        let cache = get_block_cache(new_inode_block_id as usize, self.block_device.clone());
        cache
            .lock()
            .modify(new_inode_block_offset, |new_inode: &mut DiskInode| {
                new_inode.initialize(DiskInodeType::File);
            });
        self.modify_disk_inode(|root_inode| {
            // 在目录条目中添加文件
            let file_count = (root_inode.size as usize) / DIRENT_SZ;
            let new_size = (file_count + 1) * DIRENT_SZ;
            // 扩容
            self.increase_size(new_size as u32, root_inode, &mut fs);
            // 写入目录条目
            let dirent = DirEntry::new(name, new_inode_id);
            root_inode.write_at(
                file_count * DIRENT_SZ,
                dirent.as_bytes(),
                &self.block_device,
            );
        });

        let (block_id, block_offset) = fs.get_disk_inode_pos(new_inode_id);
        block_cache_sync_all();

        // 返回索引节点
        Some(Arc::new(Self::new(
            block_id,
            block_offset,
            self.fs.clone(),
            self.block_device.clone(),
        )))

        // 由编译器自动释放简易文件系统锁
    }

    /// 列出当前索引节点下的索引节点
    pub fn ls(&self) -> Vec<String> {
        let _fs = self.fs.lock();
        self.read_disk_inode(|disk_inode| {
            let file_count = (disk_inode.size as usize) / DIRENT_SZ;
            let mut v: Vec<String> = Vec::new();
            for i in 0..file_count {
                let mut dirent = DirEntry::empty();
                assert_eq!(
                    disk_inode.read_at(i * DIRENT_SZ, dirent.as_bytes_mut(), &self.block_device),
                    DIRENT_SZ,
                );
                v.push(String::from(dirent.name()));
            }
            v
        })
    }

    /// 从当前索引节点中读取数据
    ///
    /// # Arguments
    ///
    /// * `offset`: 偏移
    /// * `buf`: 缓冲区
    ///
    /// returns: usize 读取的字节数
    pub fn read_at(&self, offset: usize, buf: &mut [u8]) -> usize {
        let _fs = self.fs.lock();
        self.read_disk_inode(|disk_inode| disk_inode.read_at(offset, buf, &self.block_device))
    }

    /// 将数据写入到当前索引节点
    ///
    /// # Arguments
    ///
    /// * `offset`: 偏移
    /// * `buf`: 缓冲区
    ///
    /// returns: usize 写入的字节数
    pub fn write_at(&self, offset: usize, buf: &[u8]) -> usize {
        let mut fs = self.fs.lock();
        let size = self.modify_disk_inode(|disk_inode| {
            self.increase_size((offset + buf.len()) as u32, disk_inode, &mut fs);
            disk_inode.write_at(offset, buf, &self.block_device)
        });
        block_cache_sync_all();
        size
    }

    /// 清空当前索引节点中的数据
    pub fn clear(&self) {
        let mut fs = self.fs.lock();
        self.modify_disk_inode(|disk_inode| {
            let size = disk_inode.size;
            let data_blocks_dealloc = disk_inode.clear_size(&self.block_device);
            assert!(data_blocks_dealloc.len() == DiskInode::total_blocks(size) as usize);
            for data_block in data_blocks_dealloc.into_iter() {
                fs.dealloc_data(data_block);
            }
        });
        block_cache_sync_all();
    }
}
