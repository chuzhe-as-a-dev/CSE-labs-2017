#ifndef yfs_client_h
#define yfs_client_h

#include <string>
#include <set>

#include "lock_protocol.h"
#include "lock_client.h"

//#include "yfs_protocol.h"
#include "extent_client.h"
#include <vector>


#define CA_FILE "./cert/ca.pem:"
#define USERFILE	"./etc/passwd"
#define GROUPFILE	"./etc/group"


class yfs_client {
    extent_client *ec;
    lock_client *lc;

public:
    typedef unsigned long long inum;
    enum xxstatus {
        OK, RPCERR, NOENT, IOERR, EXIST, NOPEM, ERRPEM, EINVA, ECTIM, ENUSE};

    typedef int status;

    struct filestat {
    	unsigned long long size;
    	unsigned long mode;
    	unsigned short uid;
    	unsigned short gid;
    };

    struct fileinfo {
        unsigned long long size;
        unsigned long atime;
        unsigned long mtime;
        unsigned long ctime;
    	unsigned long mode;
    	unsigned short uid;
    	unsigned short gid;
    };
    struct dirinfo {
        unsigned long atime;
        unsigned long mtime;
        unsigned long ctime;
    	unsigned long mode;
    	unsigned short uid;
    	unsigned short gid;
    };
    typedef struct fileinfo slinkinfo;

    struct dirent {
        std::string name;
        yfs_client::inum inum;
    };

 private:
    void _acquire(inum);
    void _release(inum);

    bool _has_duplicate(inum, const char *);
    bool _add_entry_and_save(inum, const char *, inum);

    bool _isfile(inum);
    bool _isdir(inum);

    int _getfile(inum, fileinfo &);
    int _getdir(inum, dirinfo &);
    int _getslink(inum, slinkinfo&);

    int _setattr(inum, size_t);
    int _setattr(inum, filestat, unsigned long);
    int _lookup(inum, const char *, bool &, inum &);
    int _create(inum, const char *, mode_t, inum &);
    int _writedir(inum, std::list<dirent>&);
    int _readdir(inum, std::list<dirent> &);
    int _write(inum, size_t, off_t, const char *, size_t &);
    int _read(inum, size_t, off_t, std::string &);
    int _unlink(inum,const char *);
    int _mkdir(inum , const char *, mode_t , inum &);
    int _symlink(inum, const char *, const char *, inum&);
    int _readslink(inum, std::string&);
    int _rmdir(inum, const char *);

 public:
    yfs_client();
    yfs_client(std::string, std::string, const char*);

    bool isfile(inum);
    bool isdir(inum);

    int getfile(inum, fileinfo &);
    int getdir(inum, dirinfo &);
    int getslink(inum, slinkinfo&);

    int setattr(inum, size_t);
    int setattr(inum, filestat, unsigned long);
    int lookup(inum, const char *, bool &, inum &);
    int create(inum, const char *, mode_t, inum &);
    int readdir(inum, std::list<dirent> &);
    int write(inum, size_t, off_t, const char *, size_t &);
    int read(inum, size_t, off_t, std::string &);
    int unlink(inum,const char *);
    int mkdir(inum , const char *, mode_t , inum &);

    int verify(const char* cert_file, unsigned short*);
    int symlink(inum, const char *, const char *, inum&);
    int readslink(inum, std::string&);
    int rmdir(inum, const char *);

    // version control
    int commit();
    int rollback();
    int forward();
};

#endif
