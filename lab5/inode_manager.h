// inode layer interface.

#ifndef inode_h
#define inode_h

#include <stdint.h>
#include <fstream>
#include <vector>
#include "extent_protocol.h" // TODO: delete it

#define DISK_SIZE  1024*1024*16
#define BLOCK_SIZE 512
#define BLOCK_NUM  (DISK_SIZE/BLOCK_SIZE)

typedef uint32_t blockid_t;

// helper function -----------------------------------------


// disk layer -----------------------------------------
class disk {
private:
    unsigned char blocks[BLOCK_NUM][BLOCK_SIZE];

public:
    disk();
    void read_block(uint32_t id, char *buf);
    void write_block(uint32_t id, const char *buf);
};

// block layer -----------------------------------------
typedef struct superblock {
    uint32_t size;
    uint32_t nblocks;
    uint32_t ninodes;
} superblock_t;

class block_manager {
private:
    disk *d;
    std::map<uint32_t, int>using_blocks;

    int valid_bnum(uint32_t bnum);
    int buf_not_null(char *buf);

public:
    block_manager();
    struct superblock sb;

    uint32_t alloc_block();
    void free_block(uint32_t id);
    void read_block(uint32_t id, char *buf);
    void write_block(uint32_t id, const char *buf);
};

// inode layer -----------------------------------------

struct log_entry {
    enum { create = 0, update, deletee, commit } kind;
    union {
        struct {uint32_t inum, type;} create;
        struct {uint32_t inum; int old_size, new_size; char *old_buf, *new_buf;} update;
        struct {uint32_t inum, type;} deletee;
    } u;
};

class log_manager {
private:
    std::string filename;

    std::fstream logfile;
    std::vector<int> previous_checkpoints;

    void log(const std::string &entry);
    log_entry next_log();

public:
    log_manager();
    ~log_manager();
    void create_log(uint32_t inum, uint32_t type);
    void update_log(uint32_t inum, int old_size, const char *old_buf, int new_size, const char *new_buf);
    void delete_log(uint32_t inum, uint32_t type);
    void commit();
    std::vector<log_entry> rollback();
    std::vector<log_entry> forward();
};


#define INODE_NUM 1024
// Inodes per block.
#define IPB 1  // (BLOCK_SIZE / sizeof(struct inode))
// Block containing inode i
#define IBLOCK(i, nblocks) ((nblocks) / BPB + (i) / IPB + 3) // inode id starts from 1!!
// Bitmap bits per block
#define BPB (BLOCK_SIZE * 8)  // block id starts from 1!!
// Block containing bit for block b
#define BBLOCK(b) ((b) / BPB + 2)

#define NDIRECT 32
#define NINDIRECT (BLOCK_SIZE / sizeof(uint))
#define MAXFILE (NDIRECT + NINDIRECT)
#define MAXFILESIZE ((NDIRECT - 1 + NINDIRECT) * BLOCK_SIZE)

typedef struct inode {
    // short type;
    unsigned int type;
    unsigned int size;
    unsigned int atime;
    unsigned int mtime;
    unsigned int ctime;
    blockid_t    blocks[NDIRECT + 1]; // Data block addresses
} inode_t;

class inode_manager {
private:
    block_manager *bm;
    log_manager lm;

    int valid_inum(uint32_t inum);
    int valid_type(uint32_t type);
    int valid_size(int size);

    struct inode* get_inode(uint32_t inum);
    void put_inode(uint32_t inum, struct inode *ino);

    int _write_file(uint32_t inum, const char *buf, int size);

    void redo(const log_entry &entry);
    void undo(const log_entry &entry);

public:
    inode_manager();
    uint32_t alloc_inode(uint32_t type);
    void free_inode(uint32_t inum);
    void read_file(uint32_t inum, char **buf, int *size);
    void write_file(uint32_t inum, const char *buf, int size);
    void remove_file(uint32_t inum);
    void getattr(uint32_t inum, extent_protocol::attr& a);
    void commit();
    void rollback();
    void forward();
};

#endif // ifndef inode_h
