#include "kvs.h"
#include "constants.h"
#include "string.h"
#include <stdio.h>
#include <ctype.h>

#include "src/common/io.h"
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
    char buffer[MAX_STRING_SIZE+1];
    memset(buffer, '\0', MAX_STRING_SIZE);

    // Search for the key node
	KeyNode *keyNode = ht->table[index];
    Subscribers *subNode;

    while (keyNode != NULL) {
        if (strcmp(keyNode->key, key) == 0) {
            // overwrite value
            free(keyNode->value);
            keyNode->value = strdup(value);
            subNode = keyNode->subs;
            snprintf(buffer, sizeof(buffer), "(%s,%s)", key, value);

            while(subNode != NULL){
                printf("sub: %s\n", subNode->subs);
                printf("Starting to write to the notification FIFO about a key,value named %s\n", buffer);
                if (write_all(subNode->fd_notif, buffer, sizeof(buffer)) == -1) {
                    fprintf(stderr, "Failed to write to the notification FIFO about writing in subscription!\n");
                    return -1;
                }
                subNode = subNode->next;
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
    char buffer[MAX_STRING_SIZE+1];
    memset(buffer, '\0', MAX_STRING_SIZE);

    // Search for the key node
    KeyNode *keyNode = ht->table[index];
    KeyNode *prevNode = NULL;
    Subscribers *subNode;
    Subscribers *prevSub;
    printf("delete pair is wrong\n");
    while (keyNode != NULL) {
        printf("Entrou no while do delete\n");
        if (strcmp(keyNode->key, key) == 0) {
            // Key found; delete this node
            printf("Encontrou a chave no delete!\n");
            if (prevNode == NULL) {
                
                subNode = keyNode->subs;
                snprintf(buffer, sizeof(buffer), "(%s,DELETED)", key);
                printf("%s\n", buffer);
                while(subNode != NULL){
                    if (write_all(subNode->fd_notif, buffer, sizeof(buffer)) == -1) {
                        fprintf(stderr, "Failed to write to the notification FIFO about writing in subscription!");
                        return -1;
                    }
                    prevSub = subNode;
                    subNode = prevSub->next;
                    free(prevSub->subs);
                    free(prevSub);
                }

                // Node to delete is the first node in the list
                ht->table[index] = keyNode->next; // Update the table to point to the next node
            } else {
                subNode = keyNode->subs;
                snprintf(buffer, sizeof(buffer), "(%s,DELETED)", key);
                
                while(subNode != NULL){
                    if (write_all(subNode->fd_notif, buffer, sizeof(buffer)) == -1) {
                        fprintf(stderr, "Failed to write to the notification FIFO about writing in subscription!");
                        return -1;
                    }
                    prevSub = subNode;
                    subNode = prevSub->next;
                    free(prevSub->subs);
                    free(prevSub);
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
                free(subTemp->subs);
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
        printf("key: %s\n", keyNode->key);
        if (strcmp(keyNode->key, key) == 0) {
            printf("Found key\n");
            subNode = keyNode->subs;

            if (subNode == NULL) {
                printf("Subscribing client '%s' to key '%s'\n", client_id, key);
                subNode = malloc(sizeof(subNode));
                subNode->subs = strdup(client_id);
                subNode->fd_notif = fd_notif;
                subNode->next = keyNode->subs;
                keyNode->subs = subNode;
                return 1;
            }
            
            if (strcmp(subNode->subs, client_id) == 0) {
                printf("Client already subscribed\n");
                return 0;
            }

            while (subNode->next != NULL) {
                subNode = subNode->next;
            }

            subNode = malloc(sizeof(subNode));
            subNode->subs = strdup(client_id);
            subNode->fd_notif = fd_notif;
            subNode->next = keyNode->subs;
            keyNode->subs = subNode;
            return 1;
        }
        
        previousNode = keyNode;
        keyNode = previousNode->next;
    }
    return 0;
}

int unsub_key(HashTable *ht, const char * key, const char * client_id){
    int index = hash(key);

	KeyNode *keyNode = ht->table[index];
    KeyNode *previousNode;
    Subscribers *subNode;
    Subscribers *previousSub = NULL;

    while (keyNode != NULL) {

        if (strcmp(keyNode->key, key) == 0) {
            printf("Encontrou a chave\n");
            subNode = keyNode->subs;
            while (subNode != NULL) { // Itero pela lista de subscribers
                if (strcmp(subNode->subs, client_id) == 0) { // Encontro o cliente nos subscribers
                    printf("Encontrou o cliente\n");
                    if (previousSub == NULL) { //Se só tiver um subscrito entra nesta
                        if(subNode->next != NULL){
                            keyNode->subs = subNode->next;
                        }
                        previousSub = subNode;
                    } else {
                        previousSub = subNode;
                        subNode = previousSub->next;
                    }
                    free(previousSub->subs);
                    free(previousSub);
                    return 0; // A subscrição existia e foi apagada
                }
                previousSub = subNode;
                subNode = previousSub->next; 
            }
        }
        previousNode = keyNode;
        keyNode = previousNode->next;
    }

    return 1; //A subscrição não existia
}

int iniciar_subscricao(Client *client, const char* key){

    KeyNode *keyNode;
    keyNode = client->sub_keys;
    
    if (keyNode == NULL){
        client->sub_keys = malloc(sizeof(KeyNode));
        client->sub_keys->key = malloc(sizeof(char) * MAX_STRING_SIZE);
        strcpy(client->sub_keys->key, key); 
        return 0;
    }
    while (keyNode->next != NULL){
        if (strcmp(keyNode->key, key) == 0){
            printf("Client already subscribed this key\n");
        return 0;
        }
        keyNode = keyNode->next;
    }

    keyNode->next = malloc(sizeof(KeyNode));
    keyNode->next = malloc(sizeof(char) * MAX_STRING_SIZE);
    strcpy(keyNode->key, key);

    return 0;
}

int apagar_subscricao(KeyNode *sub_keys, const char* key){
    KeyNode *keyNode;
    KeyNode *prevNode = NULL;

    keyNode = sub_keys;

    while(keyNode != NULL){
        printf("keyNode->key: %s\n", keyNode->key);
        printf("key: %s\n", key);
        if(strcmp(keyNode->key, key) == 0){
            printf("Encontrou a chave\n");
            if(prevNode == NULL){
                if(keyNode->next != NULL){
                    sub_keys = keyNode->next;
                }
                prevNode = keyNode;
            }
            else{
                prevNode = keyNode;
                keyNode = prevNode->next;
            }
            free(prevNode->key);
            free(prevNode->value);
            free(prevNode);
            printf("Supostamente apagou\n");
            return 0;
        }
        prevNode = keyNode;
        keyNode = prevNode->next;
    }
    printf("Não apagou\n");
    return 1;
}

int remove_subs(HashTable *ht, const char *client_id, const char *key){
    int index = hash(key);
    KeyNode *keyNode = ht->table[index];
    Subscribers *subNode;
    Subscribers *prevSub;

    while (keyNode != NULL) {
        if (strcmp(keyNode->key, key) == 0) {
            
            subNode = keyNode->subs;
            while(subNode != NULL){
                if(strcmp(subNode->subs, client_id) == 0){
                    prevSub = subNode;
                    subNode = prevSub->next;
                    free(prevSub->subs);
                    free(prevSub);
                    break;
                }
            }
            return 0;
        }
        keyNode = keyNode->next; // Move to the next node
    }

    return 1;
}
