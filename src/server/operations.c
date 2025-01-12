#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "constants.h"
#include "io.h"
#include "kvs.h"
#include "operations.h"
#include "src/common/io.h"

static struct HashTable *kvs_table = NULL;

/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

int kvs_init() {
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);
  kvs_table = NULL;
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE],
              char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  pthread_rwlock_wrlock(&kvs_table->tablelock);

  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write key pair (%s,%s)\n", keys[i], values[i]);
    }
  }

  pthread_rwlock_unlock(&kvs_table->tablelock);
  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  
  pthread_rwlock_rdlock(&kvs_table->tablelock);

  write_str(fd, "[");
  for (size_t i = 0; i < num_pairs; i++) {
    char *result = read_pair(kvs_table, keys[i]);
    char aux[MAX_STRING_SIZE];
    if (result == NULL) {
      snprintf(aux, MAX_STRING_SIZE, "(%s,KVSERROR)", keys[i]);
    } else {
      snprintf(aux, MAX_STRING_SIZE, "(%s,%s)", keys[i], result);
    }
    write_str(fd, aux);
    free(result);
  }
  write_str(fd, "]\n");
  
  pthread_rwlock_unlock(&kvs_table->tablelock);
  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  pthread_rwlock_wrlock(&kvs_table->tablelock);

  int aux = 0;
  for (size_t i = 0; i < num_pairs; i++) {
    if (delete_pair(kvs_table, keys[i]) != 0) {
      if (!aux) {
        write_str(fd, "[");
        aux = 1;
      }
      char str[MAX_STRING_SIZE];
      snprintf(str, MAX_STRING_SIZE, "(%s,KVSMISSING)", keys[i]);
      write_str(fd, str);
    }
  }
  if (aux) {
    write_str(fd, "]\n");
  }

  pthread_rwlock_unlock(&kvs_table->tablelock);
  return 0;
}

void kvs_show(int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return;
  }
  
  pthread_rwlock_rdlock(&kvs_table->tablelock);
  char aux[MAX_STRING_SIZE];
  
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i]; // Get the next list head
    while (keyNode != NULL) {
      snprintf(aux, MAX_STRING_SIZE, "(%s, %s)\n", keyNode->key, keyNode->value);
      write_str(fd, aux);
      keyNode = keyNode->next; // Move to the next node of the list
    }
  }

  pthread_rwlock_unlock(&kvs_table->tablelock);
}

int kvs_backup(size_t num_backup,char* job_filename , char* directory) {
  pid_t pid;
  char bck_name[50];
  snprintf(bck_name, sizeof(bck_name), "%s/%s-%ld.bck", directory, strtok(job_filename, "."),
           num_backup);

  pthread_rwlock_rdlock(&kvs_table->tablelock);
  pid = fork();
  pthread_rwlock_unlock(&kvs_table->tablelock);
  if (pid == 0) {
    // functions used here have to be async signal safe, since this
    // fork happens in a multi thread context (see man fork)
    int fd = open(bck_name, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    for (int i = 0; i < TABLE_SIZE; i++) {
      KeyNode *keyNode = kvs_table->table[i]; // Get the next list head
      while (keyNode != NULL) {
        char aux[MAX_STRING_SIZE];
        aux[0] = '(';
        size_t num_bytes_copied = 1; // the "("
        // the - 1 are all to leave space for the '/0'
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied,
                                        keyNode->key, MAX_STRING_SIZE - num_bytes_copied - 1);
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied,
                                        ", ", MAX_STRING_SIZE - num_bytes_copied - 1);
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied,
                                        keyNode->value, MAX_STRING_SIZE - num_bytes_copied - 1);
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied,
                                        ")\n", MAX_STRING_SIZE - num_bytes_copied - 1);
        aux[num_bytes_copied] = '\0';
        write_str(fd, aux);
        keyNode = keyNode->next; // Move to the next node of the list
      }
    }
    exit(1);
  } else if (pid < 0) {
    return -1;
  }
  return 0;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}

// Find client in linked list
Client* find_client(const char* key, Client* head){
    Client* current = head;

    while (current != NULL){
      if (strcmp(current->id, key) == 0){
        return current;
      }
      current = current -> next;
    }
    return NULL;
}

void add_client(Client** head, Client* new_client) {
    // If the list is empty, the new client becomes the first element
    if (*head == NULL) {
        *head = new_client;
        new_client->next = NULL;
    } else {
        // Traverse to the end of the list and add the new client
        Client* current = *head;
        while (current->next != NULL) {
            current = current->next;
        }
        current->next = new_client;
        new_client->next = NULL;
    }
}

void print_everything_at_key(const char* key,HashTable *ht){
    int index = hash(key);
    printf("Dar print das subscrições na chave %s\n", key);
    printf("----------------\n");
    KeyNode* node = ht->table[index];
    while (node != NULL){
      if (strcmp(node->key, key) == 0){
        Subscribers* node_subscritores = node->subs;
        while (node_subscritores != NULL){
          
          printf("Id do cliente: %s , fd notif = %d, ativo = %d\n", node_subscritores->sub_clients, node_subscritores->fd_notif, node_subscritores->ativo);
          node_subscritores = node_subscritores->next;
        }
      }
      node = node->next;
    }
  printf("----------------\n");
}

int subscribe(const char * key, const char * client_id, int fd_resp_pipe, int fd_notif_pipe){
  int op_code = 3;
  int value = sub_key(kvs_table, key, client_id, fd_notif_pipe);
  char buffer[3];

  snprintf(buffer, sizeof(buffer), "%d%d", op_code, value);
  if (write_all(fd_resp_pipe, buffer, sizeof(buffer)) == -1) {
    fprintf(stderr, "Failed to write to the response FIFO while subscribing!");
    return -1;
  }
  //print_everything_at_key(key, kvs_table);
  return value;
}

int unsubscribe(const char * key, const char * client_id, int fd_resp_pipe){
  //print_everything_at_key(key, kvs_table);
  int op_code = 4;
  printf("Entering unsub_key\n");
  int value = unsub_key(kvs_table, key, client_id);
  printf("Leaving unsub_key\n");
  char buffer[3];
  memset(buffer, '\0', sizeof(buffer));
  snprintf(buffer, sizeof(buffer), "%d%d", op_code, value);


  if (write_all(fd_resp_pipe, buffer, sizeof(buffer)) == -1) {
    fprintf(stderr, "Failed to write to the response FIFO while unsubscribing");
    return -1;
  }
  //print_everything_at_key(key, kvs_table);
  return value;
}

int disconnect(Client *client){
  Chaves_subscritas *keyNode;

  keyNode = client->sub_keys;


  while(keyNode != NULL){
    Chaves_subscritas *temp = keyNode;
    keyNode = keyNode->next;
    if(remove_subs(kvs_table, client->id, temp->key) == 1){
      fprintf(stderr, "Error while unsubscribing in hashtable\n");
      return -1;
    }
    free(temp->key);
    free(temp);
  }
  free(client->id);
  free(client->sub_keys); 
  free(client);
  return 0;
}