#ifndef KEY_VALUE_STORE_H
#define KEY_VALUE_STORE_H
#define TABLE_SIZE 26

#include <stddef.h>
#include <pthread.h>
#include "src/common/constants.h"


typedef struct Subscribers{
    char *sub_clients;
    int fd_notif;
    int ativo; // 1 se a subscrição está ativa, 0 caso contrário
    struct Subscribers *next;
} Subscribers;

typedef struct KeyNode {
    char *key;
    char *value;
    Subscribers *subs;
    struct KeyNode *next;
} KeyNode;

typedef struct Chaves_subscritas{
    char *key;
    int active; // 1 if the session is active, 0 otherwise
    struct Chaves_subscritas *next;
} Chaves_subscritas;

// Struct for the clients
typedef struct Client{
    char *id;
    int request_fd;
    int response_fd;
    int notification_fd;
    int active; // 1 if the session is active, 0 otherwise
    struct Client* next;
    Chaves_subscritas *sub_keys;
} Client;

typedef struct HashTable {
    KeyNode *table[TABLE_SIZE];
    pthread_rwlock_t tablelock;
} HashTable;

typedef struct {
    Client* clients[MAX_SESSION_COUNT];
    int in; // Buffer insertion index
    int out; // Buffer extraction index
} Buffer;

/// Creates a new KVS hash table.
/// @return Newly created hash table, NULL on failure
struct HashTable *create_hash_table();

int hash(const char *key); 

// Writes a key value pair in the hash table.
// @param ht The hash table.
// @param key The key.
// @param value The value.
// @return 0 if successful.
int write_pair(HashTable *ht, const char *key, const char *value);

// Reads the value of a given key.
// @param ht The hash table.
// @param key The key.
// return the value if found, NULL otherwise.
char* read_pair(HashTable *ht, const char *key);

/// Deletes a pair from the table.
/// @param ht Hash table to read from.
/// @param key Key of the pair to be deleted.
/// @return 0 if the node was deleted successfully, 1 otherwise.
int delete_pair(HashTable *ht, const char *key);

/// Frees the hashtable.
/// @param ht Hash table to be deleted.
void free_table(HashTable *ht);

int sub_key(HashTable *ht, const char * key, const char * client_id, int fd_notif);
int unsub_key(HashTable *ht, const char * key, const char * client_id);
int iniciar_subscricao(Client *client, const char* key);
int apagar_subscricao(Chaves_subscritas *sub_keys, const char* key);
int remove_subs(HashTable *ht, const char *client_id, const char *key);

#endif  // KVS_H
