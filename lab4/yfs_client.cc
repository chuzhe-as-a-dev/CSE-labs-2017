// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst) {
    ec = new extent_client(extent_dst);
    lc = new lock_client(lock_dst);

    // XYB: init root dir
    if (ec->put(1, "") != extent_protocol::OK) printf("   error init root dir\n");
}

void yfs_client::_acquire(inum inum) {
    lc->acquire(inum);
}

void yfs_client::_release(inum inum) {
    lc->release(inum);
}

bool yfs_client::isfile(inum inum) {
    _acquire(inum);
    bool result = _isfile(inum);
    _release(inum);
    return result;
}

bool yfs_client::_isfile(inum inum) {
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("   isfile: error getting attr\n");
        return false;
    }

    if (a.type != extent_protocol::T_FILE) {
        printf("   isfile: %lld is not a file\n", inum);
        return false;
    }

    return true;
}

bool yfs_client::isdir(inum inum) {
    _acquire(inum);
    bool result = _isdir(inum);
    _release(inum);
    return result;
}

bool yfs_client::_isdir(inum inum) {
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("   isdir: error getting attr\n");
        return false;
    }

    if (a.type != extent_protocol::T_DIR) {
        printf("   isdir: %lld is not a dir\n", inum);
        return false;
    }

    return true;
}

int yfs_client::getfile(inum inum, fileinfo& fin) {
    _acquire(inum);
    int result = _getfile(inum, fin);
    _release(inum);
    return result;
}

int yfs_client::_getfile(inum inum, fileinfo& fin) {
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        return IOERR;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size  = a.size;
    printf("   getfile %llu, size: %llu\n", inum, fin.size);

    return OK;
}

int yfs_client::getdir(inum inum, dirinfo& din) {
    _acquire(inum);
    int result = _getdir(inum, din);
    _release(inum);
    return result;
}

int yfs_client::_getdir(inum inum, dirinfo& din) {
    printf("   getdir %llu\n", inum);
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        return IOERR;
    }

    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

    return OK;
}

int yfs_client::getslink(inum inum, slinkinfo& sin) {
    _acquire(inum);
    int result = _getslink(inum, sin);
    _release(inum);
    return result;
}

int yfs_client::_getslink(inum inum, slinkinfo& sin) {
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        return IOERR;
    }

    sin.atime = a.atime;
    sin.mtime = a.mtime;
    sin.ctime = a.ctime;
    sin.size  = a.size;
    printf("   getslink %llu, size: %llu\n", inum, sin.size);

    return OK;
}

bool yfs_client::_has_duplicate(inum parent, const char *name) {
    bool exist;
    inum old_inum;

    return _lookup(parent, name, exist, old_inum) != extent_protocol::OK ? true : exist;
}

bool yfs_client::_add_entry_and_save(inum parent, const char *name, inum inum) {
    std::list<dirent> entries;

    if (_readdir(parent, entries) != OK) {
        return false;
    }

    dirent entry;
    entry.name = name;
    entry.inum = inum;
    entries.push_back(entry);

    if (_writedir(parent, entries) != OK) {
        return false;
    }

    return true;
}

int yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum& ino_out) {
    _acquire(parent);
    int result = _mkdir(parent, name, mode, ino_out);
    _release(parent);
    return result;
}

int yfs_client::_mkdir(inum parent, const char *name, mode_t mode, inum& ino_out) {
    // on exist, return EXIST
    if (_has_duplicate(parent, name)) {
        return EXIST;
    }

    // create file first
    if (ec->create(extent_protocol::T_DIR, ino_out) != extent_protocol::OK) {
        printf("   mkdir: fail to create directory %s\n", name);
        return IOERR;
    }

    // write back
    if (_add_entry_and_save(parent, name, ino_out) == false) {
        return IOERR;
    }

    return OK;
}

int yfs_client::lookup(inum parent, const char *name, bool& found, inum& ino_out) {
    _acquire(parent);
    int result = _lookup(parent, name, found, ino_out);
    _release(parent);
    return result;
}

int yfs_client::_lookup(inum parent, const char *name, bool& found, inum& ino_out) {
    // read directory entries
    std::list<dirent> entries;

    if (_readdir(parent, entries) != OK) {
        return IOERR;
    }

    // check if name exists
    found = false;

    for (std::list<dirent>::iterator it = entries.begin(); it != entries.end(); ++it) {
        if (it->name == name) {
            found   = true;
            ino_out = it->inum;
        }
    }

    return OK;
}

int yfs_client::readdir(inum dir, std::list<dirent>& list) {
    _acquire(dir);
    int result = _readdir(dir, list);
    _release(dir);
    return result;
}

int yfs_client::_readdir(inum dir, std::list<dirent>& list) {
    // get content
    std::string content;

    if (ec->get(dir, content) != extent_protocol::OK) {
        return IOERR;
    }

    // read entries
    list.clear();
    std::istringstream ist(content);
    dirent entry;

    while (std::getline(ist, entry.name, '\0')) {
        ist >> entry.inum;
        list.push_back(entry);
    }

    return OK;
}

int yfs_client::_writedir(inum dir, std::list<dirent>& entries) {
    // prepare content
    std::ostringstream ost;

    for (std::list<dirent>::iterator it = entries.begin(); it != entries.end();
         ++it) {
        ost << it->name;
        ost.put('\0');
        ost << it->inum;
    }

    // write to file
    if (ec->put(dir, ost.str()) != extent_protocol::OK) {
        printf("   writedir: fail to write directory\n");
        return IOERR;
    }

    return OK;
}

// Only support set size of attr
int yfs_client::setattr(inum ino, size_t size) {
    _acquire(ino);
    int result = _setattr(ino, size);
    _release(ino);
    return result;
}

int yfs_client::_setattr(inum ino, size_t size) {
    // keep off invalid input
    if (ino <= 0) {
        printf("   setattr: invalid inode number %llu\n", ino);
        return IOERR;
    }

    if (size < 0) {
        printf("   setattr: size cannot be negative %lu\n", size);
        return IOERR;
    }

    // read old content
    std::string content;

    if (ec->get(ino, content) != extent_protocol::OK) {
        printf("   setattr: fail to read content\n");
        return IOERR;
    }

    // just return if no resize should be applied
    if (size == content.size()) {
        return OK;
    }

    // resize and write back
    content.resize(size);

    if (ec->put(ino, content) != extent_protocol::OK) {
        printf("   setattr: failt to write content\n");
        return IOERR;
    }

    return OK;
}

int yfs_client::create(inum parent, const char *name, mode_t mode, inum& ino_out) {
    _acquire(parent);
    int result = _create(parent, name, mode, ino_out);
    _release(parent);
    return result;
}

int yfs_client::_create(inum parent, const char *name, mode_t mode, inum& ino_out) {
    // on exist, return EXIST
    if (_has_duplicate(parent, name)) {
        return EXIST;
    }

    // create file first
    if (ec->create(extent_protocol::T_FILE, ino_out) != extent_protocol::OK) {
        printf("   create: fail to create file\n");
        return IOERR;
    }

    // write back
    if (_add_entry_and_save(parent, name, ino_out) == false) {
        return IOERR;
    }

    return OK;
}

int yfs_client::read(inum ino, size_t size, off_t off, std::string& data) {
    _acquire(ino);
    int result = _read(ino, size, off, data);
    _release(ino);
    return result;
}

int yfs_client::_read(inum ino, size_t size, off_t off, std::string& data) {
    // keep off invalid input
    if (ino <= 0) {
        printf("   read: invalid inode number %llu\n", ino);
        return IOERR;
    }

    if (size < 0) {
        printf("   read: size cannot be negative %lu\n", size);
        return IOERR;
    }

    if (off < 0) {
        printf("   read: offset cannot be negative %li\n", off);
        return IOERR;
    }

    // return IOERR if offset is beyond file size
    extent_protocol::attr a;

    if (ec->getattr(ino, a) != extent_protocol::OK) {
        printf("   read: error getting attr\n");
        return IOERR;
    }

    if (off >= a.size) {
        printf("   read: offset %li beyond file size %u\n", off, a.size);
        return IOERR;
    }

    // read the file and get desired data
    std::string content;

    if (ec->get(ino, content) != extent_protocol::OK) {
        printf("   read: fail to read file\n");
        return IOERR;
    }

    data = content.substr(off, size);
    return OK;
}

int yfs_client::write(inum ino, size_t size, off_t off, const char *data, size_t& bytes_written) {
    _acquire(ino);
    int result = _write(ino, size, off, data, bytes_written);
    _release(ino);
    return result;
}

int yfs_client::_write(inum ino, size_t size, off_t off, const char *data, size_t& bytes_written) {
    // keep off invalid input
    if (ino <= 0) {
        printf("   write: invalid inode number %llu\n", ino);
        return IOERR;
    }

    if (size < 0) {
        printf("   write: size cannot be negative %lu\n", size);
        return IOERR;
    }

    if (off < 0) {
        printf("   write: offset cannot be negative %li\n", off);
        return IOERR;
    }

    // read the file
    std::string content;

    if (ec->get(ino, content) != extent_protocol::OK) {
        printf("   write: fail to read file\n");
        return IOERR;
    }

    // replace old data with new data, then write back
    if ((unsigned)off >= content.size()) {
        content.resize(off, '\0');
        content.append(data, size);
    } else {
        content.replace(off, off + size <= content.size() ? size : content.size() - off, data, size);
    }

    if (ec->put(ino, content) != extent_protocol::OK) {
        printf("   write: fail to write file\n");
        return IOERR;
    }

    return OK;
}

int yfs_client::unlink(inum parent, const char *name) {
    _acquire(parent);
    bool found;
    inum ino;

    if (_lookup(parent, name, found, ino) != OK) {
        _release(parent);
        return IOERR;
    }

    if (!found) {
        _release(parent);
        return IOERR;
    }
    _acquire(ino);
    int result = _unlink(parent, name);
    _release(ino);
    _release(parent);
    return result;
}

int yfs_client::_unlink(inum parent, const char *name) {
    printf("   unlink: try to unlink %s from parent %llu\n", name, parent);

    // invalid inode number
    if (parent <= 0) {
        printf("   unlink: invalid inode number %llu\n", parent);
        return IOERR;
    }

    // read entries
    std::list<dirent> entries;

    if (_readdir(parent, entries) != OK) {
        return IOERR;
    }

    // locate target inode number
    std::list<dirent>::iterator it;

    for (it = entries.begin(); it != entries.end(); ++it) {
        if (it->name == name) {
            break;
        }
    }

    if (it == entries.end()) {
        printf("   unlink: no such file or directory %s\n", name);
        return IOERR;
    }

    if (!_isfile(it->inum)) {
        return IOERR;
    }

    // remove file and entry in directory
    if (ec->remove(it->inum) != extent_protocol::OK) {
        printf("   unlink: fail to remove file %s\n", name);
        return IOERR;
    }

    entries.erase(it);

    if (_writedir(parent, entries) != OK) {
        return IOERR;
    }
    return OK;
}

int yfs_client::symlink(inum parent, const char *link, const char *name, inum& ino_out) {
    _acquire(parent);
    int result = _symlink(parent, link, name, ino_out);
    _release(parent);
    return result;
}

int yfs_client::_symlink(inum parent, const char *link, const char *name, inum& ino_out) {
    // keep off invalid input
    if (parent <= 0) {
        printf("   symlink: invalid inode number %llu\n", parent);
        return IOERR;
    }

    // create file first
    if (ec->create(extent_protocol::T_SLINK, ino_out) != extent_protocol::OK) {
        printf("   symlink: fail to create directory\n");
        return IOERR;
    }

    // write path to file
    if (ec->put(ino_out, link) != extent_protocol::OK) {
        printf("   symlink: fail to write link\n");
        return IOERR;
    }

    // add entry to directory
    if (_add_entry_and_save(parent, name, ino_out) == false) {
        return IOERR;
    }

    return OK;
}

int yfs_client::readslink(inum ino, std::string& path) {
    _acquire(ino);
    int result = _readslink(ino, path);
    _release(ino);
    return result;
}

int yfs_client::_readslink(inum ino, std::string& path) {
    // keep off invalid input
    if (ino <= 0) {
        printf("   readslink: invalid inode number %llu\n", ino);
        return IOERR;
    }

    // read path
    if (ec->get(ino, path) != extent_protocol::OK) {
        printf("   readslink: fail to read path\n");
        return IOERR;
    }

    return OK;
}

int yfs_client::rmdir(inum parent, const char *name) {
    _acquire(parent);
    bool found;
    inum ino;

    if (_lookup(parent, name, found, ino) != OK) {
        _release(parent);
        return IOERR;
    }

    if (!found) {
        _release(parent);
        return IOERR;
    }
    _acquire(ino);
    int result = _rmdir(parent, name);
    _release(ino);
    _release(parent);
    return result;
}

int yfs_client::_rmdir(inum parent, const char *name) {
    printf("   rmdir: try to remove directory %s from parent %llu\n", name, parent);

    // invalid inode number
    if (parent <= 0) {
        printf("   rmdir: invalid inode number %llu\n", parent);
        return IOERR;
    }

    // read entries
    std::list<dirent> entries;

    if (_readdir(parent, entries) != OK) {
        return IOERR;
    }

    // locate target inode number
    std::list<dirent>::iterator it;

    for (it = entries.begin(); it != entries.end(); ++it) {
        if (it->name == name) {
            break;
        }
    }

    if (it == entries.end()) {
        printf("   rmdir: no such file or directory %s\n", name);
        return IOERR;
    }

    if (!_isdir(it->inum)) {
        return IOERR;
    }

    // check if target directory is empty
    std::list<dirent> sub_entries;

    if (_readdir(it->inum, sub_entries) != OK) {
        return IOERR;
    }

    if (sub_entries.size() != 0) {
        printf("   rmdir: target directory %s is not empty\n", name);
        return IOERR;
    }

    // remove file and entry in directory
    if (ec->remove(it->inum) != extent_protocol::OK) {
        printf("   rmdir: fail to remove directory %s\n", name);
        return IOERR;
    }

    entries.erase(it);

    if (_writedir(parent, entries) != OK) {
        return IOERR;
    }

    return OK;
}
