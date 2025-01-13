#include "kvs.h"
#include "constants.h"
#include "string.h"
#include <stdio.h>
#include <ctype.h>

#include "src/common/io.h"
#include "src/common/constants.h"
#include <stdlib.h>

// Hash function based on key initial.
// @param key Lowercase alphabetical string.
// @return hash.
// NOTE: This is not an ideal hash function, but is useful for test purposes of the project
int hash(const char *key) {
    int firstLetter = tolower(key[0]);
    if (firstLetter >= 'a' && firstLetter <= 'z') {
        return firstLetter - 'a';
    } else if (firstLetter >= '0' && firstLetter <= '9') {
        return firstLetter - '0';
    }
    return -1; // Invalid index for non-alphabetic or number strings
}

struct HashTable* create_hash_table() {
	HashTable *ht = malloc(sizeof(HashTable));
	if (!ht) return NULL;
	for (int i = 0; i < TABLE_SIZE; i++) {
		ht->table[i] = NULL;
	}
	pthread_rwlock_init(&ht->tablelock, NULL);
	return ht;
}

int write_pair(HashTable *ht, const char *key, const char *value) {
    int index = hash(key);
    char key_buffer[MAX_KEY_SIZE];
    char value_buffer[MAX_KEY_SIZE];
    memset(key_buffer, '\0', MAX_STRING_SIZE);
    memset(value_buffer, '\0', MAX_STRING_SIZE);
    
    // CHANGEME quando fazemos isto temos que tambem percorrer os clientes a procura de novas chaves a serem adicionadas
    // Search for the key node
	KeyNode *keyNode = ht->table[index];
    Subscribers *subNode;

    while (keyNode != NULL) {
        // WE found the key we are looking to replace
        if (strcmp(keyNode->key, key) == 0) {
            // overwrite value
            free(keyNode->value); // Overwrite value
            keyNode->value = strdup(value);

            // Se a fila de subscritores nao for nula
            // Ativo condicao
            if ((keyNode->subs != NULL)){ // se entra aqui significa que não é nula, ou
                subNode = keyNode->subs;

                // Percorremos a fila de subscritores
                while(subNode != NULL){
                    if(subNode->ativo == 1){
                        strcpy(key_buffer, key);
                        strcpy(value_buffer, value);
                        if (write_all(subNode->fd_notif, key_buffer, sizeof(char) * MAX_KEY_SIZE) == -1) {
                            fprintf(stderr, "Failed to write to the notification FIFO about writing in subscription!");
                            return -1;
                        }
                        if (write_all(subNode->fd_notif, value_buffer, sizeof(char) * MAX_KEY_SIZE) == -1) {
                            fprintf(stderr, "Failed to write to the notification FIFO about writing in subscription!");
                            return -1;
                        }
                    }   
                    
                    subNode = subNode->next;
                }
            }
            return 0;
        }
        keyNode = keyNode->next; // Move to the next node
    }
    // Key not found, create a new key node
    
    keyNode = malloc(sizeof(KeyNode));
    keyNode->key = strdup(key); // Allocate memory for the key
    keyNode->value = strdup(value); // Allocate memory for the value
    keyNode->next = ht->table[index]; // Link to existing nodes
    ht->table[index] = keyNode; // Place new key node at the start of the list
    return 0;
}

char* read_pair(HashTable *ht, const char *key) {
    int index = hash(key);

	KeyNode *keyNode = ht->table[index];
    KeyNode *previousNode;
    char *value;

    while (keyNode != NULL) {
        if (strcmp(keyNode->key, key) == 0) {
            value = strdup(keyNode->value);
            return value; // Return the value if found
        }
        previousNode = keyNode;
        keyNode = previousNode->next; // Move to the next node
    }

    return NULL; // Key not found
}

int delete_pair(HashTable *ht, const char *key) {
    int index = hash(key);
    char key_buffer[MAX_KEY_SIZE];
    char value_buffer[MAX_KEY_SIZE];
    memset(key_buffer, '\0', MAX_STRING_SIZE);
    memset(value_buffer, '\0', MAX_STRING_SIZE);

    // Search for the key node
    KeyNode *keyNode = ht->table[index];
    KeyNode *prevNode = NULL;
    Subscribers *subNode;
    Subscribers *prevSub;

    while (keyNode != NULL) {
        if (strcmp(keyNode->key, key) == 0) {
            // Key found; delete this node
            if (prevNode == NULL) {
                subNode = keyNode->subs;
                while(subNode != NULL){
                    if(subNode->ativo == 1){
                        strcpy(key_buffer, key);
                        strcpy(value_buffer, "DELETED");
                        if (write_all(subNode->fd_notif, key_buffer, sizeof(char) * MAX_KEY_SIZE) == -1) {
                            fprintf(stderr, "Failed to write to the notification FIFO about writing in subscription!");
                            return -1;
                        }
                        if (write_all(subNode->fd_notif, value_buffer, sizeof(char) * MAX_KEY_SIZE) == -1) {
                            fprintf(stderr, "Failed to write to the notification FIFO about writing in subscription!");
                            return -1;
                        }
                        subNode->ativo = 0;
                    }
                    prevSub = subNode;
                    subNode = prevSub->next;
                }

                // Node to delete is the first node in the list
                ht->table[index] = keyNode->next; // Update the table to point to the next node
            } else {
                subNode = keyNode->subs;
                
                while(subNode != NULL){
                    if(subNode->ativo == 1){
                        strcpy(key_buffer, key);
                        strcpy(value_buffer, "DELETED");
                        if (write_all(subNode->fd_notif, key_buffer, sizeof(char) * MAX_KEY_SIZE) == -1) {
                            fprintf(stderr, "Failed to write to the notification FIFO about writing in subscription!");
                            return -1;
                        }
                        if (write_all(subNode->fd_notif, value_buffer, sizeof(char) * MAX_KEY_SIZE) == -1) {
                            fprintf(stderr, "Failed to write to the notification FIFO about writing in subscription!");
                            return -1;
                        }
                        subNode->ativo = 0;
                    }

                    prevSub = subNode;
                    subNode = prevSub->next;
                }
                // Node to delete is not the first; bypass it
                prevNode->next = keyNode->next; // Link the previous node to the next node
            }
            // Free the memory allocated for the key and value
            free(keyNode->key);
            free(keyNode->value);
            free(keyNode); // Free the key node itself
            return 0; // Exit the function
        }
        prevNode = keyNode; // Move prevNode to current node
        keyNode = keyNode->next; // Move to the next node
    }

    return 1;
}

void free_table(HashTable *ht) {
    for (int i = 0; i < TABLE_SIZE; i++) {
        KeyNode *keyNode = ht->table[i];
        Subscribers *subNode;

        while (keyNode != NULL) {
            subNode = keyNode->subs;

            while(subNode != NULL){
                Subscribers *subTemp = subNode;
                subNode = subNode->next;
                free(subTemp->sub_clients);
                free(subTemp);
            }
            
            KeyNode *temp = keyNode;
            keyNode = keyNode->next;
            free(temp->key);
            free(temp->value);
            free(temp);
        }
    }
    pthread_rwlock_destroy(&ht->tablelock);
    free(ht);
}

int sub_key(HashTable *ht, const char * key, const char * client_id, int fd_notif){
    int index = hash(key);
    
	KeyNode *keyNode = ht->table[index];
    KeyNode *previousNode;
    Subscribers *subNode;

    while (keyNode != NULL) {
        if (strcmp(keyNode->key, key) == 0) {
            subNode = keyNode->subs;

            // If we dont have subscribers inside the certain index of the hashtable
            if (subNode == NULL) {
                // Alocate memory for an entry
                subNode = malloc(sizeof(Subscribers));
                subNode->sub_clients = strdup(client_id);
                subNode->fd_notif = fd_notif;
                subNode->ativo = 1;
                subNode->next = NULL;
                keyNode->subs = subNode;
                
                return 1;
            }

            while (subNode->next != NULL) { // Itero pela lista de subscribers 
                if (strcmp(subNode->sub_clients, client_id) == 0) {
                    subNode->ativo = 1;
                    
                    return 1;
                }
                subNode = subNode->next;
            }

            // Se chegamos aqui significa que o cliente não estava subscrito e vamos inscrevê-lo
            subNode = malloc(sizeof(subNode));
            subNode->sub_clients = strdup(client_id);
            subNode->fd_notif = fd_notif;
            subNode->ativo = 1;
            subNode->next = keyNode->subs;
            keyNode->subs = subNode;
            
            return 1;
        }
        
        previousNode = keyNode;
        keyNode = previousNode->next;
    }
    
    return 1;
}

// Ponto disto é: Ir ao index da hashtable e encontrar a subscricao do cliente dentro dos subscribers e apagar
int unsub_key(HashTable *ht, const char * key, const char * client_id){
    int index = hash(key);

	KeyNode *keyNode = ht->table[index];
    KeyNode *previousNode;
    Subscribers *subNode;
    Subscribers *previousSub = NULL;

    while (keyNode != NULL) {
        if (strcmp(keyNode->key, key) == 0) { // Chave exata encontrada
            subNode = keyNode->subs; // Shortcut para subscribers

            if (subNode == NULL){
                return 0;
            }

            while (subNode != NULL) { // Itero pela lista de subscribers

                if (strcmp(subNode->sub_clients, client_id) == 0) { // Encontro o cliente nos subscribers
                    subNode->ativo = 0;
                
                }
                previousSub = subNode;
                subNode = previousSub->next; 
            }
        }
        previousNode = keyNode;
        keyNode = previousNode->next;
    }

    return 0; //A subscrição não existia
}

int iniciar_subscricao(Client *client, const char* key) {

    Chaves_subscritas *keyNode = client->sub_keys;

    if (keyNode == NULL) {
        client->sub_keys = malloc(sizeof(Chaves_subscritas));
        client->sub_keys->key = malloc(sizeof(char) * MAX_STRING_SIZE);
        strcpy(client->sub_keys->key, key); 
        client->sub_keys->active = 1;
        client->sub_keys->next = NULL; 
        return 0;
    }

    while (keyNode != NULL) {

        if (strcmp(keyNode->key, key) == 0) {
            keyNode->active = 1; 
            return 0;
        }

        if (keyNode->next == NULL) break;
        keyNode = keyNode->next;
    }

    keyNode->next = malloc(sizeof(Chaves_subscritas));
    keyNode->next->key = malloc(sizeof(char) * MAX_STRING_SIZE);
    strcpy(keyNode->next->key, key);
    keyNode->next->active = 1;
    keyNode->next->next = NULL;

    return 0;
}


int apagar_subscricao(Chaves_subscritas *sub_keys, const char* key){

    Chaves_subscritas *keyNode;
    keyNode = sub_keys;

    while(keyNode != NULL){
        if(strcmp(keyNode->key, key) == 0){
            keyNode->active = 0;
            return 0;
        }
        keyNode = keyNode->next;
    }
    return 1;
}

int remove_subs(HashTable *ht, const char *client_id, const char *key){
    int index = hash(key);
    KeyNode *keyNode = ht->table[index];
    Subscribers *subNode;

    while (keyNode != NULL) {
        if (strcmp(keyNode->key, key) == 0) {
            
            subNode = keyNode->subs;
            while(subNode != NULL){
                if(strcmp(subNode->sub_clients, client_id) == 0){
                    subNode->ativo = 0;
                    break;
                }
            }
            return 0;
        }
        keyNode = keyNode->next; // Move to the next node
    }

    return 1;
}
