#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>

#include "kvs.h"
#include "constants.h"

static struct HashTable* kvs_table = NULL;


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
  return 0;
}

int find_in_vector(int * index_seen, int hashed_key, int count){
    for (int i = 0; i < count; i++){
      if (index_seen[i] == hashed_key){
        return 1;
      }
    }
    return 0;
}

int compare_keynodes(const void *a, const void *b) {
    KeyNode node_a = *(KeyNode *)a;
    KeyNode node_b = *(KeyNode *)b;
    return strcmp(node_a.key, node_b.key);
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  int index_seen[num_pairs], counter = 0; 
  //If we don't do this the qsort sorts the keys without the values following them
  KeyNode vector[num_pairs];
  
  for(size_t i = 0; i < num_pairs; i++){
    vector[i].key = malloc(strlen(keys[i])+1);
    strcpy(vector[i].key, keys[i]);

    vector[i].value = malloc(strlen(values[i])+1);
    strcpy(vector[i].value,values[i]);
  }

  qsort(vector, num_pairs, sizeof(KeyNode), compare_keynodes); 
  
  for (size_t i = 0; i < num_pairs; i++) {
    int index = hash(vector[i].key);
    if (!find_in_vector(index_seen, hash(vector[i].key), counter)){
      index_seen[counter++] = index;
      //Only locks the index entry of the kvs table
      pthread_rwlock_wrlock(&kvs_table->locks[index]);
    }

    if (write_pair(kvs_table, vector[i].key, vector[i].value) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", vector[i].key, vector[i].value);
    }
  }

  for (int i = 0; i < counter; i++){
    //Unlocks the section of the kvs table that we previously locked
    pthread_rwlock_unlock(&kvs_table->locks[index_seen[i]]);
  }

  for(size_t i = 0; i < num_pairs; i++){
    free(vector[i].key);
    free(vector[i].value);
  }

  return 0;
}

/*
  Compares strings, used in qsort
*/
int compare_strings(const void *a, const void *b) {
    const char *str1 = (const char *)a;
    const char *str2 = (const char *)b;
    return strcmp(str1, str2);
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd_out) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  qsort(keys, num_pairs, MAX_STRING_SIZE, compare_strings);
  ssize_t bytes_written = 0;
  int index_seen[num_pairs], counter = 0; 
  char *message = "[";

  bytes_written = write(fd_out, message, (size_t)strlen(message));
  if (bytes_written == -1){return 1;}

  for (size_t i = 0; i < num_pairs; i++) {
    int index = hash(keys[i]);
    if (!find_in_vector(index_seen, hash(keys[i]), counter)){
      index_seen[counter++] = index;
      //Only locks the index entry of the kvs table
      pthread_rwlock_rdlock(&kvs_table->locks[index]);
    } 

    char* result = read_pair(kvs_table, keys[i]);
    char to_file[MAX_WRITE_SIZE];

    if (result == NULL) {
      snprintf(to_file, sizeof(to_file), "(%s,KVSERROR)", keys[i]);
      message = malloc(sizeof(char) * (strlen(to_file) + 1));
      strcpy(message, to_file);
      bytes_written = write(fd_out, message, (size_t)strlen(message));
      
      if (bytes_written == -1){
        for (int j = 0; j < counter; j++){
          pthread_rwlock_unlock(&kvs_table->locks[index_seen[j]]);
        }
        free(message);
        return 1;
      }
    } else {
      snprintf(to_file, sizeof(to_file), "(%s,%s)", keys[i], result);
      message = malloc(sizeof(char) * (strlen(to_file) + 1));
      strcpy(message, to_file);
      bytes_written = write(fd_out, message, (size_t)strlen(message));
      if (bytes_written == -1){
        for (int j = 0; j < counter; j++){
          pthread_rwlock_unlock(&kvs_table->locks[index_seen[j]]);
        }
        free(message);
        return 1;
      }
    }    
    free(result);
    free(message);
  }
  for (int j = 0; j < counter; j++){
    //Unlocks the section of the kvs table that we previously locked
    pthread_rwlock_unlock(&kvs_table->locks[index_seen[j]]);
  }
  message = "]\n";
  bytes_written = write(fd_out, message, (size_t)strlen(message));
  if (bytes_written == -1){return 1;}
  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd_out) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  qsort(keys, num_pairs, MAX_STRING_SIZE, compare_strings);
  
  int index_seen[num_pairs], counter = 0, aux = 0;
  char to_file[MAX_WRITE_SIZE];
  char *message;

  for (size_t i = 0; i < num_pairs; i++) {
    int index = hash(keys[i]);

    if (!find_in_vector(index_seen, hash(keys[i]), counter)){
      index_seen[counter++] = index;
      pthread_rwlock_wrlock(&kvs_table->locks[index]);
    } 

    if (delete_pair(kvs_table, keys[i]) != 0) {
      if (!aux) {
        message = "[";
        ssize_t bytes_written = write(fd_out, message, (size_t)strlen(message));

        if (bytes_written == -1) {
          for (int j = 0; j < counter; j++){
            pthread_rwlock_unlock(&kvs_table->locks[index_seen[j]]);
          }
          return 1;
        }
        aux = 1;
      }
      snprintf(to_file, sizeof(to_file), "(%s,KVSMISSING)", keys[i]);
      message = malloc(sizeof(char) * (strlen(to_file) + 1));
      strcpy(message, to_file);
      ssize_t bytes_written = write(fd_out, message, (size_t)strlen(message));

      if (bytes_written == -1) {
        free(message);
        for (int j = 0; j < counter; j++){
          pthread_rwlock_unlock(&kvs_table->locks[index_seen[j]]);
        }
        return 1;
      }
      free(message);
    }
  }

  for (int j = 0; j < counter; j++){
    pthread_rwlock_unlock(&kvs_table->locks[index_seen[j]]);
  }
  
  if (aux) {
    message = "]\n";
    ssize_t bytes_written = write(fd_out, message, (size_t)strlen(message));
    if (bytes_written == -1) {return 1;}
  }
  return 0;
}

void kvs_show(int fd_out) {

  for (int i = 0; i < TABLE_SIZE; i++){
    //locks the whole kvs table to writers
    pthread_rwlock_rdlock(&kvs_table->locks[i]);
  }

  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL) {
      char to_file[1024];
      char * message;
      snprintf(to_file, sizeof(to_file), "(%s, %s)\n", keyNode->key, keyNode->value);
      message = malloc(sizeof(char) * (strlen(to_file) + 1));
      strcpy(message, to_file);

      ssize_t bytes_written = write(fd_out, message, (size_t)strlen(message));

      if (bytes_written == -1) {
        fprintf(stderr,"Could not write everything to .out\n");
      }
      keyNode = keyNode->next; // Move to the next node
      free(message);
    }
  }
  for (int i = 0; i < TABLE_SIZE; i++){
    //Unlocks the whole table
    pthread_rwlock_unlock(&kvs_table->locks[i]);
  }
}

int kvs_backup(int n_backup, char* dir_path, char file_name[MAX_JOB_FILE_NAME_SIZE]) {
    char path[MAX_JOB_FILE_NAME_SIZE], *name, *info;
    ssize_t bytes_written = 0;

    snprintf(path, MAX_JOB_FILE_NAME_SIZE, "%s/%s-%d.bck", dir_path, file_name, n_backup);
    name = malloc(sizeof(char) * (strlen(path) + 1));
    strcpy(name, path); 

    int fd_created = open(name, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if(fd_created == -1){
      free(name);
      return -1;
    }

    for (int i = 0; i < TABLE_SIZE; i++) {
        KeyNode *keyNode = kvs_table->table[i];
        while (keyNode != NULL) {
            char buffer[MAX_WRITE_SIZE + 1];
            snprintf(buffer, sizeof(buffer), "(%s, %s)\n", keyNode->key, keyNode->value);
            info = malloc(sizeof(char) * (strlen(buffer) + 1));
            strcpy(info, buffer);
            bytes_written = write(fd_created, buffer, (size_t)strlen(info));
            if(bytes_written == -1){
              close(fd_created);
              free(name);
              free(info);
              return -1;
            } 
            keyNode = keyNode->next;
            free(info);
        }
    }

    close(fd_created);
    free(name);
    return 0;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}