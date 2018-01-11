#ifndef yfs_client_h
#define yfs_client_h

#include <string>

// #include "yfs_protocol.h"
#include "extent_client.h"
#include <vector>


class yfs_client {
    extent_client *ec;

public:

    typedef unsigned long long inum;
    enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST };
    typedef int status;

    struct fileinfo {
        unsigned long long size;
        unsigned long      atime;
        unsigned long      mtime;
        unsigned long      ctime;
    };
    struct dirinfo {
        unsigned long atime;
        unsigned long mtime;
        unsigned long ctime;
    };
    typedef fileinfo slinkinfo;
    struct dirent {
        std::string      name;
        yfs_client::inum inum;
    };

private:

    static std::string filename(inum);
    static inum n2i(std::string);

    int writedir(inum, std::list<dirent>&);
    bool has_duplicate(inum, const char *);
    bool add_entry_and_save(inum, const char *, inum);

public:

    yfs_client();
    yfs_client(std::string,
               std::string);

    bool isfile(inum);
    bool isdir(inum);

    int getfile(inum, fileinfo&);
    int getdir(inum, dirinfo&);
    int getslink(inum, slinkinfo&);

    int setattr(inum, size_t);
    int create(inum, const char *, mode_t, inum&);
    int read(inum, size_t, off_t, std::string&);
    int write(inum, size_t, off_t, const char *, size_t&);
    int unlink(inum, const char *);

    int mkdir(inum, const char *, mode_t, inum&);
    int readdir(inum, std::list<dirent>&);
    int lookup(inum, const char *, bool&, inum&);

    int symlink(inum, const char *, const char *, inum&);
    int readslink(inum, std::string&);
    /** you may need to add symbolic link related methods here.*/
};

#endif // ifndef yfs_client_h
