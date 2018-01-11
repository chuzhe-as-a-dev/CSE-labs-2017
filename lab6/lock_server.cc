// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server()
    : nacquire (0) {
    pthread_mutex_init(&mutex, NULL);
}

lock_protocol::status lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r) {
	printf("stat request from clt %d\n", clt);
	r = nacquire;
	return lock_protocol::OK;
}

lock_protocol::status lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r) {
    printf("[INFO] about to get mutex\n");
    pthread_mutex_lock(&mutex);

    // wait until lock is free
    while (locks.find(lid) != locks.end()) {
        pthread_cond_wait(&conds[lid], &mutex);
    }

    // now target lock is free and mutex is held
    locks.insert(lid);  // acquire lock
    printf("[INFO] client %d acquired lock %llu\n", clt, lid);

    // craete condition variable if needed
    if (conds.find(lid) == conds.end()) {
        pthread_cond_t cond;
        pthread_cond_init(&cond, NULL);
        conds[lid] = cond;
    }

    pthread_mutex_unlock(&mutex);
    printf("[INFO] just released mutex\n");
    return lock_protocol::OK;
}

lock_protocol::status lock_server::release(int clt, lock_protocol::lockid_t lid, int &r) {
    printf("[INFO] about to get mutex\n");
    pthread_mutex_lock(&mutex);

    // if target lock is not existing
    if (locks.find(lid) == locks.end()) {
        printf("[ERROR] client %d tries to release un held lock %llu\n", clt, lid);
        pthread_mutex_unlock(&mutex);
        return lock_protocol::NOENT;
    }

    locks.erase(lid);  // release lock
    printf("[INFO] client %d released lock %llu\n", clt, lid);
    pthread_cond_signal(&conds[lid]);  // signal other threads that want this lock

    pthread_mutex_unlock(&mutex);
    printf("[INFO] just released mutex\n");
    return lock_protocol::OK;
}
