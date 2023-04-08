use std::any::Any;
use std::fmt::Debug;

/// 块设备的特征
/// 以块为单位读写数据
pub trait BlockDevice: Debug + Send + Sync + Any {
    /// 将数据从块读取到缓冲区
    ///
    /// # Arguments
    ///
    /// * `block_id`: 块ID
    /// * `buf`: 缓冲区
    fn read_block(&self, block_id: usize, buf: &mut [u8]);

    /// 将数据从缓冲区写入到块
    ///
    /// # Arguments
    ///
    /// * `block_id`: 块ID
    /// * `buf`: 缓冲区
    fn write_block(&self, block_id: usize, buf: &[u8]);
}
