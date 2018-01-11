#include <time.h>
#include <cstdio>
#include <sstream>

#include "inode_manager.h"

#ifndef VERBOSE
#define VERBOSE 1
#endif

#ifndef TEST
#define TEST 1
#endif

// disk layer -----------------------------------------

disk::disk() {
    bzero(blocks, sizeof(blocks));
}

void disk::read_block(blockid_t id, char *buf) {
    memcpy(buf, blocks[id - 1], BLOCK_SIZE);
}

void disk::write_block(blockid_t id, const char *buf) {
    memcpy(blocks[id - 1], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager() {
    d = new disk();

    // format the disk
    sb.size    = BLOCK_SIZE * BLOCK_NUM;
    sb.nblocks = BLOCK_NUM;
    sb.ninodes = INODE_NUM;

    // mark superblock, bitmap, inode table as used
    char buf[BLOCK_SIZE];
    memset(buf, ~0, BLOCK_SIZE);
    int last_bnum = IBLOCK(INODE_NUM, sb.nblocks);

    // blocks in bitmap to be filled with all ones
    for (int bitmap_bnum = BBLOCK(1); bitmap_bnum < BBLOCK(last_bnum);
         bitmap_bnum++) {
        write_block(bitmap_bnum, buf);
    }

    // the last block in bitmap to partially fill with ones
    // set whole bytes to ones
    memset(buf, 0,  BLOCK_SIZE);
    int remaining_bits_num = last_bnum - (BBLOCK(last_bnum) - BBLOCK(1)) * BPB;
    memset(buf, ~0, remaining_bits_num / 8);

    // set trailing bits to ones
    char last_byte = 0;

    for (int pos = 0; pos < remaining_bits_num % 8; ++pos) {
        last_byte = last_byte | ((char)1 << (7 - pos));
    }
    buf[remaining_bits_num / 8] = last_byte;

    // write last block
    write_block(BBLOCK(last_bnum), buf);
}

int block_manager::valid_bnum(blockid_t bnum) {
    if ((bnum <= 0) || (bnum > BLOCK_NUM)) {
        printf("bm: block id out of range: %d\n", bnum);
        return 0;
    }
    return 1;
}

// Allocate a free disk block.
blockid_t block_manager::alloc_block() {
    int  bitmap_bnum, pos;
    bool free_block_found;

    // search from first block after inode table
    for (bitmap_bnum = BBLOCK(IBLOCK(INODE_NUM, sb.nblocks) + 1); bitmap_bnum <= BBLOCK(BLOCK_NUM); bitmap_bnum++) {
        free_block_found = false;

        // read bitmap
        char bitmap[BLOCK_SIZE];
        read_block(bitmap_bnum, bitmap);

        // read every bit
        for (pos = 0; pos < BPB; ++pos) {
            char byte = bitmap[pos / 8];
            char bit  = byte & ((char)1 << (7 - pos % 8));

            if (bit == 0) { // free block found!
                // mark as used, and write to block
                bitmap[pos / 8] = byte | ((char)1 << (7 - pos % 8));
                write_block(bitmap_bnum, bitmap);
                free_block_found = true;
                break;
            }
        }

        if (free_block_found)
            break;
    }

    if (free_block_found) {
        blockid_t bnum = (bitmap_bnum - BBLOCK(1)) * BPB + pos + 1;
        return bnum;
    } else {
        printf("bm: no empty block available\n");
        return 0;
    }
}

void block_manager::free_block(blockid_t bnum) {
    if (!valid_bnum(bnum))
        return;

    // get bitmap
    char bitmap[BLOCK_SIZE];
    read_block(BBLOCK(bnum), bitmap);

    // set coresponding bit to zero, and write back
    int  byte_pos_in_bitmap = (bnum - 1) % BPB; // starts from 0
    char byte = bitmap[byte_pos_in_bitmap / 8];
    bitmap[((bnum - 1) % BPB) / 8] = byte & ~((char)1 << (7 - byte_pos_in_bitmap % 8));
    
    // update bitmap
    write_block(BBLOCK(bnum), bitmap);
}

void block_manager::read_block(blockid_t bnum, char *buf) {
    if (!valid_bnum(bnum))
        return;

    d->read_block(bnum, buf);
}

void block_manager::write_block(blockid_t bnum, const char *buf) {
    if (!valid_bnum(bnum))
        return;

    d->write_block(bnum, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager() {
    bm = new block_manager();
    uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);

    if (root_dir != 1) {
        printf("im: alloc first inode %d, should be 1\n", root_dir);
        exit(0);
    }
}

// return 1 when inum is valid
int inode_manager::valid_inum(uint32_t inum) {
    if ((inum <= 0) || (inum > INODE_NUM)) {
        printf("im: inum out of range %d\n", inum);
        return 0;
    }

    return 1;
}

int inode_manager::valid_type(uint32_t type) {
    if (type == 0) {
        printf("im: invalid type %u\n", type);
        return 0;
    }
    return 1;
}

int inode_manager::valid_size(int size) {
    if ((size < 0) || ((unsigned)size > MAXFILESIZE)) {
        printf("im: file size out of range %d\n", size);
        return 0;
    }
    return 1;
}

/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode * inode_manager::get_inode(uint32_t inum) {
    if (!valid_inum(inum))
        return NULL;
    
    struct inode *ino, *ino_disk;
    char buf[BLOCK_SIZE];

    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);

    ino_disk = (struct inode *)buf + (inum - 1) % IPB;

    if (ino_disk->type == 0) {
        printf("im: inode %d not exist\n", inum);
        return NULL;
    }

    ino  = (struct inode *)malloc(sizeof(struct inode));
    *ino = *ino_disk;

    return ino;
}

void inode_manager::put_inode(uint32_t inum, struct inode *ino) {
    if (!valid_inum(inum))
        return;

    // change ctime
    ino->ctime = (unsigned int)time(NULL);

    // then write to the right block
    char buf[BLOCK_SIZE];
    struct inode *ino_disk;

    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
    ino_disk  = (struct inode *)buf + (inum - 1) % IPB;
    *ino_disk = *ino;
    bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

/* Create a new file and return its inum. */
uint32_t inode_manager::alloc_inode(uint32_t type) {
    if (!valid_type(type))
        return 0;

    struct inode *ino;
    char buf[BLOCK_SIZE];
    uint32_t inum;

    // find a free inode in inode table
    for (inum = 1; inum <= INODE_NUM; inum++) {
        // directly read from block
        bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);

        // note that inode id starts from 1
        ino = (struct inode *)buf + (inum - 1) % IPB;

        if (ino->type == 0) // find an empty inode
            break;
    }

    if (inum > INODE_NUM) {
        printf("im: no empty inode available\n");
        return 0;
    }

    // initialize empty inode
    ino->type = type;
    ino->size = 0;
    unsigned int now = (unsigned int)time(NULL);
    ino->atime = now;
    ino->mtime = now;
    ino->ctime = now;

    // save inode
    bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);

    #if VERBOSE
    printf("im: allocate inode %d\n", inum);
    #endif

    // log
    lm.create_log(inum, type);

    return inum;
}

/* Return alloced file data, buf_out should be freed by caller. */
void inode_manager::read_file(uint32_t inum, char **buf_out, int *size) {
    #if VERBOSE
    printf("im: read file %d\n", inum);
    #endif

    // invalid input
    if (!valid_inum(inum))
        return;

    // get inode
    struct inode *ino = get_inode(inum);
    if (ino == NULL)
        return;

    // alocate memory for reading
    *buf_out = (char *)malloc(ino->size);

    char block_buf[BLOCK_SIZE];
    blockid_t indirect_block_buf[BLOCK_SIZE / sizeof(blockid_t)];
    int block_num = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;

    // read direct entry
    for (int i = 0; i < (block_num > NDIRECT ? NDIRECT : block_num); i++) {
        bm->read_block(ino->blocks[i], block_buf);

        if (i == block_num - 1) { // only copy partial data from last block
            memcpy(*buf_out + i * BLOCK_SIZE,
                   block_buf,
                   ino->size - i * BLOCK_SIZE);
        } else {
            memcpy(*buf_out + i * BLOCK_SIZE, block_buf, BLOCK_SIZE);
        }
    }

    // read indirect entry, if needed
    if (block_num > NDIRECT) {
        bm->read_block(ino->blocks[NDIRECT], (char *)indirect_block_buf);

        for (int i = 0; i < block_num - NDIRECT; i++) {
            bm->read_block(indirect_block_buf[i], block_buf);

            if (i == block_num - NDIRECT - 1) { // only copy partial data from
                                                // last block
                memcpy(*buf_out + (i + NDIRECT) * BLOCK_SIZE,
                       block_buf,
                       ino->size - (i + NDIRECT) * BLOCK_SIZE);
            } else {
                memcpy(*buf_out + (i + NDIRECT) * BLOCK_SIZE,
                       block_buf,
                       BLOCK_SIZE);
            }
        }
    }

    // report file size
    *size = ino->size;

    // update atime
    ino->atime = (unsigned int)time(NULL);
    put_inode(inum, ino);
    free(ino);
}

/* alloc/free blocks if needed */
void inode_manager::write_file(uint32_t inum, const char *buf, int size) {
    // logging
    char *old;
    int old_size;
    read_file(inum, &old, &old_size);

    #if VERBOSE
    printf("im: write file %d\n", inum);
    #endif

    if (_write_file(inum, buf, size)) {  // log on success
        lm.update_log(inum, old_size, old, size, buf);
    }
}

// return 1 on success
int inode_manager::_write_file(uint32_t inum, const char *buf, int size) {
    // invalid input
    if (!valid_inum(inum) || !valid_size(size))
        return 0;

    // get block containing inode inum
    struct inode *ino = get_inode(inum);
    if (!ino || ino->type == 0)
        return 0;

    // prepare to write
    blockid_t indirect_block_buf[BLOCK_SIZE / sizeof(blockid_t)];
    int block_num_old = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
    int block_num_new = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;

    if (block_num_new <= block_num_old) { // write a smaller file
        // write to old direct block
        for (int i = 0; i < (block_num_new > NDIRECT ? NDIRECT : block_num_new);
             i++) {
            if (i == block_num_new - 1) { // add zero padding when writing the
                                          // last block
                char padding[BLOCK_SIZE];
                bzero(padding, BLOCK_SIZE);
                memcpy(padding, buf + i * BLOCK_SIZE, size - i * BLOCK_SIZE);
                bm->write_block(ino->blocks[i], padding);
            } else {
                bm->write_block(ino->blocks[i], buf + i * BLOCK_SIZE);
            }
        }

        // read indirect block entry, if needed
        if (block_num_new > NDIRECT) {
            bm->read_block(ino->blocks[NDIRECT], (char *)indirect_block_buf);

            // write to old indirect block, if needed
            for (int i = 0; i < block_num_new - NDIRECT; i++) {
                if (i == block_num_new - NDIRECT - 1) { // add zero padding when
                                                        // writing the last
                                                        // block
                    char padding[BLOCK_SIZE];
                    bzero(padding, BLOCK_SIZE);
                    memcpy(padding, buf + (i + NDIRECT) * BLOCK_SIZE,
                           size - (i + NDIRECT) * BLOCK_SIZE);
                    bm->write_block(indirect_block_buf[i], padding);
                } else {
                    bm->write_block(indirect_block_buf[i],
                                    buf + (i + NDIRECT) * BLOCK_SIZE);
                }
            }
        }

        // free unused direct blocks, if any
        for (int i = block_num_new;
             i < (block_num_old > NDIRECT ? NDIRECT : block_num_old); i++) {
            bm->free_block(ino->blocks[i]);
        }

        // free unused indirect blocks, if any
        if (block_num_old > NDIRECT) {
            for (int i = (block_num_new > NDIRECT ? block_num_new - NDIRECT : 0);
                 i < block_num_old - NDIRECT; i++) {
                bm->free_block(indirect_block_buf[i]);
            }

            // free indirect entry, if needed
            if (block_num_new <= NDIRECT) {
                bm->free_block(ino->blocks[NDIRECT]);
            }
        }
    } else { // write a bigger file
        // write to old direct blocks
        for (int i = 0; i < (block_num_old > NDIRECT ? NDIRECT : block_num_old);
             i++) {
            bm->write_block(ino->blocks[i], buf + i * BLOCK_SIZE);
        }

        // alloc and write to remaining direct blocks, if any
        for (int i = block_num_old;
             i < (block_num_new > NDIRECT ? NDIRECT : block_num_new); i++) {
            uint32_t bnum = bm->alloc_block();
            ino->blocks[i] = bnum;

            if (i == block_num_new - 1) { // add zero padding when writing the
                                          // last block
                char padding[BLOCK_SIZE];
                bzero(padding, BLOCK_SIZE);
                memcpy(padding, buf + i * BLOCK_SIZE, size - i * BLOCK_SIZE);
                bm->write_block(ino->blocks[i], padding);
            } else {
                bm->write_block(ino->blocks[i], buf + i * BLOCK_SIZE);
            }
        }

        // read indirect entry, alloc first if needed
        if (block_num_new > NDIRECT) {
            if (block_num_old <= NDIRECT) {
                uint32_t bnum = bm->alloc_block();
                ino->blocks[NDIRECT] = bnum;
            } else {
                bm->read_block(ino->blocks[NDIRECT], (char *)indirect_block_buf);
            }

            // write to old indirect blocks, if any
            for (int i = 0;
                 i < (block_num_old > NDIRECT ? block_num_old - NDIRECT : 0);
                 i++) {
                bm->write_block(indirect_block_buf[i],
                                buf + (i + NDIRECT) * BLOCK_SIZE);
            }

            // alloc and write to remaining indirect block, if needed
            for (int i = (block_num_old > NDIRECT ? block_num_old - NDIRECT : 0);
                 i < block_num_new - NDIRECT; i++) {
                uint32_t bnum = bm->alloc_block();
                indirect_block_buf[i] = bnum;

                if (i == block_num_new - NDIRECT - 1) { // add zero padding when
                                                        // writing the last
                                                        // block
                    char padding[BLOCK_SIZE];
                    bzero(padding, BLOCK_SIZE);
                    memcpy(padding, buf + (i + NDIRECT) * BLOCK_SIZE,
                           size - (i + NDIRECT) * BLOCK_SIZE);
                    bm->write_block(indirect_block_buf[i], padding);
                } else {
                    bm->write_block(indirect_block_buf[i],
                                    buf + (i + NDIRECT) * BLOCK_SIZE);
                }
            }

            // save indirect block entry, if needed
            if (block_num_new > NDIRECT) {
                bm->write_block(ino->blocks[NDIRECT], (char *)indirect_block_buf);
            }
        }
    }

    // update size and mtime
    unsigned int now = (unsigned int)time(NULL);
    ino->size  = size;
    ino->mtime = now;
    ino->ctime = now;
    put_inode(inum, ino);
    free(ino);

    return 1;
}

void inode_manager::remove_file(uint32_t inum) {
    #if VERBOSE
    printf("im: remove file %d\n", inum);
    #endif

    if (!valid_inum(inum))
        return;
        
    // get inode information
    struct inode *ino = get_inode(inum);
    if (ino == NULL)
        return;

    // logging
    char *old, new_empty[1];
    new_empty[0] = '\0';
    int old_size;
    read_file(inum, &old, &old_size);

    lm.update_log(inum, old_size, old, 0, new_empty);
    lm.delete_log(inum, ino->type);
    free(old);

    // free inode first
    free_inode(inum);

    // free block used
    int block_num = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;

    // free direct entry
    for (int i = 0; i < (block_num > NDIRECT ? NDIRECT : block_num); i++) {
        bm->free_block(ino->blocks[i]);
    }

    // free indirect entry, if any
    if (block_num > NDIRECT) {
        blockid_t indirect_block_buf[BLOCK_SIZE / sizeof(blockid_t)];
        bm->read_block(ino->blocks[NDIRECT], (char *)indirect_block_buf);

        for (int i = 0; i < block_num - NDIRECT; i++) {
            bm->free_block(indirect_block_buf[i]);
        }
    }

    free(ino);
}

void inode_manager::free_inode(uint32_t inum) {
    if (!valid_inum(inum))
        return;

    // get old inode
    struct inode *ino = get_inode(inum);
    if (ino == NULL)
        return;

    // set inode free and write to block, and free malloced space
    ino->type = 0;
    put_inode(inum, ino);
    free(ino);
}

void inode_manager::getattr(uint32_t inum, extent_protocol::attr& a) {
    if (!valid_inum(inum))
        return;

    struct inode *ino = get_inode(inum);
    if (ino == NULL)
        return;

    a.type  = ino->type;
    a.atime = ino->atime;
    a.mtime = ino->mtime;
    a.ctime = ino->ctime;
    a.size  = ino->size;

    free(ino);
}

void inode_manager::commit() {
    #if VERBOSE
    printf("im: commit\n");
    #endif
    lm.commit();
}

void inode_manager::rollback() {
    #if VERBOSE
    printf("im: rollback\n");
    #endif

    std::vector<log_entry> entries = lm.rollback();

    #if VERBOSE
    printf("im: %lu logs to undo\n", entries.size());
    #endif

    for (size_t i = entries.size(); i > 0; --i) {
        undo(entries[i - 1]);
    }
}

void inode_manager::forward() {
    #if VERBOSE
    printf("im: forward\n");
    #endif

    std::vector<log_entry> entries = lm.forward();

    #if VERBOSE
    printf("im: %lu logs to redo\n", entries.size());
    #endif

    for (size_t i = 0; i < entries.size(); ++i) {
        redo(entries[i]);
    }
}

void inode_manager::redo(const log_entry &entry) {
    switch (entry.kind) {
        case log_entry::create: {
            #if VERBOSE
            printf("im: redo create, inum: %d, type: %d\n", entry.u.create.inum, entry.u.create.type);
            #endif

            struct inode ino;
            ino.type = entry.u.create.type;
            ino.size = 0;
            unsigned int now = (unsigned int)time(NULL);
            ino.atime = now;
            ino.mtime = now;
            ino.ctime = now;

            put_inode(entry.u.create.inum, &ino);
            break;
        }
        case log_entry::update: {
            #if VERBOSE
            printf("im: redo update, inum: %d, new_size: %d\n", entry.u.update.inum, entry.u.update.new_size);
            #endif

            _write_file(entry.u.update.inum, entry.u.update.new_buf, entry.u.update.new_size);
            break;
        }
        case log_entry::deletee: {
            #if VERBOSE
            printf("im: redo delete, inum: %d, type: %d\n", entry.u.deletee.inum, entry.u.deletee.type);
            #endif

            free_inode(entry.u.deletee.inum);
            break;
        }
        case log_entry::commit:
        default:
            printf("im: unexpected log entry to redo %d\n", entry.kind);
    }
}

void inode_manager::undo(const log_entry &entry) {
    switch (entry.kind) {
        case log_entry::create: {
            #if VERBOSE
            printf("im: undo create, inum: %d, type: %d\n", entry.u.create.inum, entry.u.create.type);
            #endif
            free_inode(entry.u.create.inum);
            break;
        }
        case log_entry::update: {
            #if VERBOSE
            printf("im: undo update, inum: %d, old_size: %d\n", entry.u.update.inum, entry.u.update.old_size);
            #endif
            _write_file(entry.u.update.inum, entry.u.update.old_buf, entry.u.update.old_size);
            break;
        }
        case log_entry::deletee: {
            #if VERBOSE
            printf("im: undo delete, inum: %d, type: %d\n", entry.u.deletee.inum, entry.u.deletee.type);
            #endif

            struct inode ino;
            ino.type = entry.u.deletee.type;
            ino.size = 0;
            unsigned int now = (unsigned int)time(NULL);
            ino.atime = now;
            ino.mtime = now;
            ino.ctime = now;
            put_inode(entry.u.deletee.inum, &ino);

            break;
        }
        case log_entry::commit:
        default:
            printf("im: unexpected log entry to undo %d\n", entry.kind);
    }
}

// Log Manager -----------------------------------------

log_manager::log_manager() {
    filename = "disk.log";
    logfile.open(filename.c_str(), std::fstream::in | std::fstream::out | std::fstream::trunc);
}

log_manager::~log_manager() {
    logfile.close();
}

void log_manager::log(const std::string &entry) {
    if (logfile.peek() != EOF) {  // writing to disk after some rollbacks
        // create a temp file
        std::ofstream newdata("temp", std::ios::out);
        int size = logfile.tellp();
        char buf[size];
    	logfile.seekp(0);
    	logfile.read(buf, size);
    	newdata.write(buf, size);
        newdata.close();

        // remove old log and use newly created one as new log
        logfile.close();
        std::rename("temp", filename.c_str());
        logfile.open(filename.c_str(), std::fstream::in | std::fstream::out | std::fstream::app);

        #if VERBOSE
        printf("lm: clean trailing logs\n");
        #endif
    } else {
        logfile.clear();
    }

    logfile << entry;
    logfile.flush();
}

void log_manager::create_log(uint32_t inum, uint32_t type) {
    std::stringstream ss;
    ss << "create " << inum << ' ' << type << '\n';
    
    #if VERBOSE
    printf("lm: new create log, inum: %d, type: %d\n", inum, type);
    #endif
    log(ss.str());
}

void log_manager::update_log(uint32_t inum, int old_size, const char *old_buf, int new_size, const char *new_buf) {
    std::stringstream ss;
    ss << "update " << inum << ' ' << old_size << ' ' << new_size << ' ';
    
    ss.write(old_buf, old_size);
    ss.write(new_buf, new_size);
    ss << '\n';

    #if VERBOSE
    printf("lm: new update log, inum: %d, old_size: %d, new_size: %d\n", inum, old_size, new_size);
    #endif
    log(ss.str());
}

void log_manager::delete_log(uint32_t inum, uint32_t type) {
    std::stringstream ss;
    ss << "delete " << inum << ' ' << type << '\n';
    
    #if VERBOSE
    printf("lm: new delete log, inum: %d, type: %d\n", inum, type);
    #endif
    log(ss.str());
}

// buf needs to be freed by user
log_entry log_manager::next_log() {
    #if VERBOSE
    int cursor = logfile.tellp();
    #endif

    log_entry entry;

    std::string log_type = "init";
    logfile >> log_type;

    if (log_type == "create") {
        entry.kind = log_entry::create;
        logfile >> entry.u.create.inum >> entry.u.create.type;
        #if VERBOSE
        printf("lm: reading create log at %d, inum: %d, type: %d\n", cursor, entry.u.create.inum, entry.u.create.type);
        #endif
    } else if (log_type == "update") {
        entry.kind = log_entry::update;

        logfile >> entry.u.update.inum >> entry.u.update.old_size >> entry.u.update.new_size;
        logfile.get();
        
        entry.u.update.old_buf = (char*)malloc(entry.u.update.old_size);
        entry.u.update.new_buf = (char*)malloc(entry.u.update.new_size);

        logfile.read(entry.u.update.old_buf, entry.u.update.old_size);
        logfile.read(entry.u.update.new_buf, entry.u.update.new_size);

        #if VERBOSE
        printf("lm: reading update log at %d, inum: %d, old_size: %d, new_size: %d\n", cursor, entry.u.update.inum, entry.u.update.old_size, entry.u.update.new_size);
        #endif
    } else if (log_type == "delete") {
        entry.kind = log_entry::deletee;
        logfile >> entry.u.deletee.inum >> entry.u.deletee.type;
        #if VERBOSE
        printf("lm: reading delete log at %d, inum: %d, type: %d\n", cursor, entry.u.deletee.inum, entry.u.deletee.type);
        #endif
    } else if (log_type == "commit") {
        entry.kind = log_entry::commit;
        #if VERBOSE
        printf("lm: reading commit log at %d\n", cursor);
        #endif
    } else {
        printf("lm: unexpected log type %s\n", log_type.c_str());
        entry.kind = log_entry::commit;
        #if VERBOSE
        printf("lm: reading unexpected log at %d, cursor at: %d", cursor, (int)logfile.tellp());
        exit(-1);
        #endif
    }

    logfile.get();  // skip trailing newline
    return entry;
}

void log_manager::commit() {
    #if VERBOSE
    printf("lm: new commit log\n");
    #endif
    log("commit\n");
    previous_checkpoints.push_back(logfile.tellp());
}

std::vector<log_entry> log_manager::rollback() {
    std::vector<log_entry> entries;

    if (previous_checkpoints.size() == 0) {
        printf("lm: previous commit not exists\n");
        return entries;
    }

    int curr_pos = logfile.tellp();
    int prev_ckp = previous_checkpoints.back();

    if (curr_pos > prev_ckp) {  // some writes need to be undone
        // go to last checkpoint
        logfile.seekp(previous_checkpoints.back());

        // read all writes since checkpoint
        while (logfile.tellp() < curr_pos) {
            entries.push_back(next_log());
        }

        logfile.seekp(previous_checkpoints.back());

        return entries;
    } else if (curr_pos == prev_ckp) {  // rollback after just commit
        if (previous_checkpoints.size() == 1) {
            printf("lm: cannot rollback further\n");
            return entries;
        }

        // skip last commit
        logfile.seekp(-7, std::ios_base::cur);  // 7 == length of "commit\n"
        previous_checkpoints.pop_back();
        int curr_pos = logfile.tellp();
        int prev_ckp = previous_checkpoints.back();

        // see if there is any write needs to be undone
        if (curr_pos > prev_ckp) {
            return rollback();
        } else {
            return entries;
        }
    }
}

std::vector<log_entry> log_manager::forward() {
    std::vector<log_entry> entries;

    if (logfile.peek() == EOF) {  // if no log entry available
        printf("lm: cannot forward further\n");
        logfile.clear();
        return entries;
    }

    log_entry entry;
    do {
        entry = next_log();
        if (entry.kind != log_entry::commit) {
            entries.push_back(entry);
        } else {
            previous_checkpoints.push_back(logfile.tellp());
            break;
        }
    } while (logfile.peek() != EOF);
    
    if (logfile.eof()) {
        logfile.clear();
    }

    return entries;
}
