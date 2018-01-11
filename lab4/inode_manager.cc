#include <time.h>

#include "inode_manager.h"

#ifndef VERBOSE
#define VERBOSE 0
#endif

// disk layer -----------------------------------------

disk::disk() {
    bzero(blocks, sizeof(blocks));
}

void disk::read_block(blockid_t id, char *buf) {
    if ((id <= 0) || (id > BLOCK_NUM)) {
        printf("read_block: block id out of range: %d\n", id);
        return;
    }

    if (buf == NULL) {
        printf("read_block: buf is NULL\n");
        return;
    }

    memcpy(buf, blocks[id - 1], BLOCK_SIZE);
}

void disk::write_block(blockid_t id, const char *buf) {
    if ((id <= 0) || (id > BLOCK_NUM)) {
        printf("write_block: block id out of range: %d\n", id);
        return;
    }

    if (buf == NULL) {
        printf("read_block: buf is NULL\n");
        return;
    }

    memcpy(blocks[id - 1], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t block_manager::alloc_block() {
    if (VERBOSE) {
        printf("alloc_block: try to allocate free block\n");
    }

    char bitmap[BLOCK_SIZE];
    int  bitmap_bnum, pos;
    bool free_block_found;

    // search from first block after inode table
    for (bitmap_bnum = BBLOCK(IBLOCK(INODE_NUM, sb.nblocks) + 1);
         bitmap_bnum <= BBLOCK(BLOCK_NUM); bitmap_bnum++) {
        free_block_found = false;

        // read bitmap
        d->read_block(bitmap_bnum, bitmap);

        // read every bit
        for (pos = 0; pos < BPB; ++pos) {
            char byte = bitmap[pos / 8];
            char bit  = byte & ((char)1 << (7 - pos % 8));

            if (bit == 0) { // free block found!
                // mark as used, and write to block
                bitmap[pos / 8] = byte | ((char)1 << (7 - pos % 8));
                d->write_block(bitmap_bnum, bitmap);
                free_block_found = true;
                break;
            }
        }

        if (free_block_found) {
            break;
        }
    }

    if (free_block_found) {
        blockid_t bnum = (bitmap_bnum - BBLOCK(1)) * BPB + pos + 1;

        if (VERBOSE) {
            printf("alloc_block: bnum of allocated block: %d\n", bnum);
        }
        return bnum;
    } else {
        printf("alloc_block: no empty block available\n");
        return 0;
    }
}

void block_manager::free_block(uint32_t id) {
    if (VERBOSE) {
        printf("free_block: try to free block %d\n", id);
    }

    // invalid block id
    if ((id <= 0) || (id > BLOCK_NUM)) {
        printf("free_block: block id out of range: %d\n", id);
        return;
    }

    // get bitmap
    char bitmap[BLOCK_SIZE];
    d->read_block(BBLOCK(id), bitmap);

    // set coresponding bit to zero, and write back
    int  byte_pos_in_bitmap = (id - 1) % BPB; // starts from 0
    char byte               = bitmap[byte_pos_in_bitmap / 8];
    bitmap[((id - 1) % BPB) / 8] = byte &
                                   ~((char)1 << (7 - byte_pos_in_bitmap % 8));
    d->write_block(BBLOCK(id), bitmap);
}

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
        d->write_block(bitmap_bnum, buf);
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
    d->write_block(BBLOCK(last_bnum), buf);
}

void block_manager::read_block(uint32_t id, char *buf) {
    if ((id <= 0) || (id > BLOCK_NUM)) {
        printf("read_block: block id out of range: %d\n", id);
        return;
    }

    if (buf == NULL) {
        printf("read_block: buf is NULL\n");
        return;
    }

    d->read_block(id, buf);
}

void block_manager::write_block(uint32_t id, const char *buf) {
    if ((id <= 0) || (id > BLOCK_NUM)) {
        printf("write_block: block id out of range: %d\n", id);
        return;
    }

    if (buf == NULL) {
        printf("write_block: buf is NULL\n");
        return;
    }

    d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager() {
    bm = new block_manager();
    uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);

    if (root_dir != 1) {
        printf("inode_manager: alloc first inode %d, should be 1\n",
               root_dir);
        exit(0);
    }
}

/* Create a new file.
* Return its inum. */
uint32_t inode_manager::alloc_inode(uint32_t type) {
    if (type == 0) {
        printf("alloc_inode: invalid type %u\n", type);
        return 0;
    }

    struct inode *ino;
    char buf[BLOCK_SIZE];
    uint32_t inum;

    // find a free inode in inode table
    for (inum = 1; inum <= INODE_NUM; inum++) {
        // directly read from block
        bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);

        // note that inode id starts from 1
        ino = (struct inode *)buf + (inum - 1) % IPB;

        if (ino->type == 0) { // find an empty inode
            break;
        }
    }

    if (inum > INODE_NUM) {
        printf("alloc_inode: no empty inode available\n");
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

    return inum;
}

void inode_manager::free_inode(uint32_t inum) {
    if ((inum <= 0) || (inum > INODE_NUM)) {
        printf("free_inode: inum out of range %d\n", inum);
        return;
    }

    // get old inode
    struct inode *ino = get_inode(inum);

    if (ino == NULL) {
        printf("free_inode: inode not exist: %d\n", inum);
        return;
    }

    // set inode free and write to block, and free malloced space
    ino->type = 0;
    put_inode(inum, ino);
    free(ino);
}

/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode * inode_manager::get_inode(uint32_t inum) {
    if ((inum <= 0) || (inum > INODE_NUM)) {
        printf("get_inode: inum out of range %d\n", inum);
        return NULL;
    }

    struct inode *ino, *ino_disk;
    char buf[BLOCK_SIZE];

    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);

    ino_disk = (struct inode *)buf + (inum - 1) % IPB;

    if (ino_disk->type == 0) {
        printf("get_inode: inode not exist\n");
        return NULL;
    }

    ino  = (struct inode *)malloc(sizeof(struct inode));
    *ino = *ino_disk;

    return ino;
}

void inode_manager::put_inode(uint32_t inum, struct inode *ino) {
    if ((inum <= 0) || (inum > INODE_NUM)) {
        printf("put_inode: inum out of range %d\n", inum);
        return;
    }

    if (ino == NULL) {
        printf("put_inode: inode pointed is NULL\n");
        return;
    }

    // change ctime first
    unsigned int now = (unsigned int)time(NULL);
    ino->ctime = now;

    // then write to the right block
    char buf[BLOCK_SIZE];
    struct inode *ino_disk;

    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
    ino_disk  = (struct inode *)buf + (inum - 1) % IPB;
    *ino_disk = *ino;
    bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

// #define MIN(a, b) ((a) < (b) ? (a) : (b))

/* Get all the data of a file by inum.
 * Return alloced data, should be freed by caller. */
void inode_manager::read_file(uint32_t inum, char **buf_out, int *size) {
    // invalid input
    if ((inum <= 0) || (inum > INODE_NUM)) {
        printf("read_file: inum out of range: %d\n", inum);
        return;
    }

    if (buf_out == NULL) {
        printf("read_file: buf_out pointer is NULL\n");
        return;
    }

    if (size == NULL) {
        printf("read_file: size pointer is NULL\n");
        return;
    }

    // get inode
    struct inode *ino = get_inode(inum);

    if (ino == NULL) {
        printf("read_file: inode not exist: %d\n", inum);
        return;
    }

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
    unsigned int now = (unsigned int)time(NULL);
    ino->atime = now;
    put_inode(inum, ino);
    free(ino);
}

/* alloc/free blocks if needed */
void inode_manager::write_file(uint32_t inum, const char *buf, int size) {
    // invalid input
    if ((inum <= 0) || (inum > INODE_NUM)) {
        printf("write_file: inum out of range: %d\n", inum);
        return;
    }

    if (buf == NULL) {
        printf("write_file: buf pointer is NULL\n");
        return;
    }

    if ((size < 0) || ((unsigned)size > MAXFILESIZE)) {
        printf("write_file: size out of range %d\n", size);
        return;
    }

    // get block containing inode inum
    struct inode *ino = get_inode(inum);

    // invalid (empty) inode
    if (ino->type == 0) { // find an empty inode
        printf("write_file: inode not exist: %d\n", inum);
        return;
    }

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
}

void inode_manager::getattr(uint32_t inum, extent_protocol::attr& a) {
    if ((inum <= 0) || (inum > INODE_NUM)) {
        printf("getattr: inum out of range %d\n", inum);
        return;
    }

    struct inode *ino = get_inode(inum);

    if (ino == NULL) {
        printf("getattr: inode not exist %d\n", inum);
        return;
    }

    a.type  = ino->type;
    a.atime = ino->atime;
    a.mtime = ino->mtime;
    a.ctime = ino->ctime;
    a.size  = ino->size;

    free(ino);
}

void inode_manager::remove_file(uint32_t inum) {
    if ((inum <= 0) || (inum > INODE_NUM)) {
        printf("remove_file: inum out of range %d\n", inum);
        return;
    }
    // get inode information
    struct inode *ino = get_inode(inum);

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
