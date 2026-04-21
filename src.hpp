#include "fstream.h"
#include <vector>
#include <algorithm>
#include <cstring>

// 磁盘事件类型：正常、故障、更换
enum class EventType {
  NORMAL,  // 正常：所有磁盘工作正常
  FAILED,  // 故障：指定磁盘发生故障（文件被删除）
  REPLACED // 更换：指定磁盘被更换（文件被清空）
};

class RAID5Controller {
private:
  std::vector<sjtu::fstream *> drives_; // 磁盘文件对应的 fstream 对象
  int blocks_per_drive_;               // 每个磁盘的块数
  int block_size_;                     // 每个块的大小
  int num_disks_;                      // 磁盘数
  int failed_drive_id_;                // 损坏或正在更换的磁盘 ID (-1 表示正常)
  bool is_degraded_;                   // 是否处于降级模式

  void ComputeParity(const std::vector<char*>& data_blocks, char* parity_block) {
    std::memset(parity_block, 0, block_size_);
    for (int i = 0; i < num_disks_ - 1; ++i) {
      for (int j = 0; j < block_size_; ++j) {
        parity_block[j] ^= data_blocks[i][j];
      }
    }
  }

  void RecoverBlock(int drive_id, int stripe_id, char* result) {
    std::vector<char> buffer(block_size_);
    std::memset(result, 0, block_size_);
    for (int i = 0; i < num_disks_; ++i) {
      if (i == drive_id) continue;
      drives_[i]->seekg(stripe_id * block_size_);
      drives_[i]->read(buffer.data(), block_size_);
      for (int j = 0; j < block_size_; ++j) {
        result[j] ^= buffer[j];
      }
    }
  }

public:
  RAID5Controller(std::vector<sjtu::fstream *> drives, int blocks_per_drive,
                  int block_size = 4096)
      : drives_(drives), blocks_per_drive_(blocks_per_drive),
        block_size_(block_size), num_disks_(drives.size()),
        failed_drive_id_(-1), is_degraded_(false) {}

  /**
   * @brief 启动 RAID5 系统
   * @param event_type_ 磁盘事件类型
   * @param drive_id 发生事件的磁盘编号（如果是 NORMAL 则忽略）
   *
   * 如果是 FAILED，对应的磁盘文件会被删除。此时不可再对该文件进行读写。
   * 如果是 REPLACED，对应的磁盘文件会被清空（但文件依然存在）
   * 如果是 NORMAL，所有磁盘正常工作
   * 注：磁盘被替换之前不一定损坏。
   */
  void Start(EventType event_type_, int drive_id) {
    if (event_type_ == EventType::FAILED) {
      failed_drive_id_ = drive_id;
      is_degraded_ = true;
    } else if (event_type_ == EventType::REPLACED) {
      // 重新构造数据
      failed_drive_id_ = -1;
      is_degraded_ = false;

      std::vector<char> recovered_data(block_size_);
      for (int stripe_id = 0; stripe_id < blocks_per_drive_; ++stripe_id) {
        RecoverBlock(drive_id, stripe_id, recovered_data.data());
        drives_[drive_id]->seekp(stripe_id * block_size_);
        drives_[drive_id]->write(recovered_data.data(), block_size_);
      }
      drives_[drive_id]->flush();
    } else {
      failed_drive_id_ = -1;
      is_degraded_ = false;
    }
  }

  void Shutdown() {
    for (auto drive : drives_) {
      if (drive && drive->is_open()) {
        drive->flush();
        drive->close();
      }
    }
  }

  void ReadBlock(int block_id, char *result) {
    int stripe_id = block_id / (num_disks_ - 1);
    int data_disk_index = block_id % (num_disks_ - 1);

    // RAID5 parity rotation: P is at (num_disks_ - 1 - (stripe_id % num_disks_))
    int parity_disk = num_disks_ - 1 - (stripe_id % num_disks_);
    int actual_drive_id = data_disk_index;
    if (actual_drive_id >= parity_disk) {
      actual_drive_id++;
    }

    if (is_degraded_ && actual_drive_id == failed_drive_id_) {
      RecoverBlock(actual_drive_id, stripe_id, result);
    } else {
      drives_[actual_drive_id]->seekg(stripe_id * block_size_);
      drives_[actual_drive_id]->read(result, block_size_);
    }
  }

  void WriteBlock(int block_id, const char *data) {
    int stripe_id = block_id / (num_disks_ - 1);
    int data_disk_index = block_id % (num_disks_ - 1);

    int parity_disk = num_disks_ - 1 - (stripe_id % num_disks_);
    int actual_drive_id = data_disk_index;
    if (actual_drive_id >= parity_disk) {
      actual_drive_id++;
    }

    if (is_degraded_) {
      if (actual_drive_id == failed_drive_id_) {
        // Data disk is failed, only parity needs update
        // We need old data to update parity, but old data is lost.
        // Actually, we can recover old data using other disks, but it's easier to:
        // New Parity = XOR(all other disks, new data)
        std::vector<char> new_parity(block_size_);
        std::memset(new_parity.data(), 0, block_size_);
        for (int i = 0; i < block_size_; ++i) {
          new_parity[i] = data[i];
        }

        std::vector<char> buffer(block_size_);
        for (int i = 0; i < num_disks_; ++i) {
          if (i == actual_drive_id || i == parity_disk) continue;
          drives_[i]->seekg(stripe_id * block_size_);
          drives_[i]->read(buffer.data(), block_size_);
          for (int j = 0; j < block_size_; ++j) {
            new_parity[j] ^= buffer[j];
          }
        }

        if (parity_disk != failed_drive_id_) {
          drives_[parity_disk]->seekp(stripe_id * block_size_);
          drives_[parity_disk]->write(new_parity.data(), block_size_);
          drives_[parity_disk]->flush();
        }
      } else if (parity_disk == failed_drive_id_) {
        // Parity disk is failed, just write data
        drives_[actual_drive_id]->seekp(stripe_id * block_size_);
        drives_[actual_drive_id]->write(data, block_size_);
        drives_[actual_drive_id]->flush();
      } else {
        // Neither is failed, update both
        // New Parity = Old Parity XOR Old Data XOR New Data
        std::vector<char> old_data(block_size_);
        std::vector<char> old_parity(block_size_);

        drives_[actual_drive_id]->seekg(stripe_id * block_size_);
        drives_[actual_drive_id]->read(old_data.data(), block_size_);

        drives_[parity_disk]->seekg(stripe_id * block_size_);
        drives_[parity_disk]->read(old_parity.data(), block_size_);

        for (int i = 0; i < block_size_; ++i) {
          old_parity[i] ^= (old_data[i] ^ data[i]);
        }

        drives_[actual_drive_id]->seekp(stripe_id * block_size_);
        drives_[actual_drive_id]->write(data, block_size_);
        drives_[actual_drive_id]->flush();

        drives_[parity_disk]->seekp(stripe_id * block_size_);
        drives_[parity_disk]->write(old_parity.data(), block_size_);
        drives_[parity_disk]->flush();
      }
    } else {
      // Normal mode, update both
      std::vector<char> old_data(block_size_);
      std::vector<char> old_parity(block_size_);

      drives_[actual_drive_id]->seekg(stripe_id * block_size_);
      drives_[actual_drive_id]->read(old_data.data(), block_size_);

      drives_[parity_disk]->seekg(stripe_id * block_size_);
      drives_[parity_disk]->read(old_parity.data(), block_size_);

      for (int i = 0; i < block_size_; ++i) {
        old_parity[i] ^= (old_data[i] ^ data[i]);
      }

      drives_[actual_drive_id]->seekp(stripe_id * block_size_);
      drives_[actual_drive_id]->write(data, block_size_);
      drives_[actual_drive_id]->flush();

      drives_[parity_disk]->seekp(stripe_id * block_size_);
      drives_[parity_disk]->write(old_parity.data(), block_size_);
      drives_[parity_disk]->flush();
    }
  }

  int Capacity() {
    // 返回磁盘阵列能写入的块的数量（你无需改动此函数）
    return (num_disks_ - 1) * blocks_per_drive_;
  }
};
