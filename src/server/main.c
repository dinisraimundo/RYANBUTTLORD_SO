#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h> 
#include <sys/wait.h>
#include <errno.h>

#include "../common/constants.h"
#include "constants.h"
#include "parser.h"
#include "operations.h"
#include "io.h"


// Global variables
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
int concurrent_backups = 0;
int client_counter = 0;

// Struct for the files
typedef struct file_info{
    char path[MAX_JOB_FILE_NAME_SIZE];
    char name[MAX_JOB_FILE_NAME_SIZE];
    char path_out[MAX_JOB_FILE_NAME_SIZE];
} Info; 

// Struct we use in the main function
typedef struct main_info {
    Info * job_files;
    size_t n_file;
    char * dir_path;
    int max_backups;
} Main_info;

// Struct for the clients
typedef struct {
    int id;
    int request_fd;
    int response_fd;
    int notification_fd;
    int active; // 1 if the session is active, 0 otherwise
} Client;

// Struct for host thread
typedef struct {
    int register_fd;         // FIFO file descriptor
    Client *clients;     // Array of clients
} Thread_args;

// Prototypes
Info *get_job_files(DIR *directory, const char *dir_path, size_t *num_files);
void *job_processor(void *arg);
void *register_clients(void *arg);

// Main
int main(int argc, char *argv[]) {
    printf("1");
    if (argc < 4) {
        fprintf(stderr, "Error: Few arguments\n");
        return 1;
    }
 
    // Definitions
    size_t number_of_files = 0, n_file = 0;
    DIR *directory = opendir(argv[1]);
    Info *job_files = get_job_files(directory, argv[1], &number_of_files);
    Client clients[MAX_SESSION_COUNT];
    pthread_t list[atoi(argv[3])];
    int n_thread = 0;
    pthread_t host;

    printf("1");
    // Create fifo directory
    const char *fifo_dir = "/tmp";
    DIR *fifo_dir_fd = opendir(fifo_dir);

    // Init kvs
    if (kvs_init()) {
        fprintf(stderr, "Failed to initialize KVS\n");
        return 1;
    }
    printf("1");
    // Init fifo de registo 
    if (mkfifo(argv[4], 0666) == -1 && errno == EEXIST){
        fprintf(stderr, "Failed to create fifo\n");
        return 1;
    }
    printf("1");
    int register_fifo = open(argv[4], O_RDONLY);
    printf("1");
    Thread_args * args = malloc(sizeof(Thread_args));
    args->register_fd = register_fifo;
    args->clients = clients;
    // Host thread to create clients
    pthread_create(&host, NULL, register_clients, (void*)args);
    
    while (n_file < number_of_files) {
        int count = 0;

        for (n_thread = 0; n_thread < atoi(argv[3]); n_thread++) {
            if (n_file >= number_of_files) break; // If this happens we don't need more threads

            // Allocate memory for each thread data
            Main_info *thread_info = malloc(sizeof(Main_info));
            thread_info->n_file = n_file;
            thread_info->job_files = job_files;
            thread_info->dir_path = malloc(strlen(argv[1]) + 1);
            thread_info->max_backups = atoi(argv[2]);
            strcpy(thread_info->dir_path, argv[1]);

            pthread_create(&list[n_thread], NULL, job_processor, (void *)thread_info);

            count++;
            n_file++;
        }
        
        // Join threads
        for (n_thread = 0; n_thread < count; n_thread++) {
            pthread_join(list[n_thread], NULL);
        }
    }
    
    kvs_terminate();
    closedir(directory);
    if (unlink(argv[4]) == -1) {
        fprintf(stderr, "Failed to remove FIFO");
    }
    closedir(fifo_dir_fd);
    free(args);
    free(job_files);
    return 0;
}

/*
    This function receives a filename and checks if the file is a .job
*/
int is_job_file(const char *filename) {
    const char *ext = strrchr(filename, '.');
    return ext && strcmp(ext, ".job") == 0;
}

/*
    This function verifies if a file is already in an array
*/
int is_filename_processed(const char *filename, char **processed_files, size_t num_files) {
    for (size_t i = 0; i < num_files; i++) {
        if (strcmp(processed_files[i], filename) == 0) {
            return 1; // Filename already processed
        }
    }
    return 0;
}

/*
    Removes the .job from the file and puts .out
*/
void replace_extension_with_out(char *filename) {

    char *ext = strstr(filename, ".job");
    if (ext != NULL && strcmp(ext, ".job") == 0) {
        *ext = '\0';  // Remove the ".job" part
    }

    strcat(filename, ".out");
}

// Obtain all .job files from a directory
Info *get_job_files(DIR *directory, const char *dir_path, size_t *num_files) {
    struct dirent *entry;
    Info *files = malloc(4 * sizeof(Info));
    char **processed_files = malloc(4 * sizeof(char *));
    size_t capacity = 4, files_capacity = 4;
    size_t processed_count = 0;
    *num_files = 0;
    
    while ((entry = readdir(directory)) != NULL) {
        if (is_job_file(entry->d_name)) {

            if (*num_files >= capacity) {
                capacity *= 2;
                files = realloc(files, capacity * sizeof(Info));
            }
            if (processed_count >= files_capacity) {
                files_capacity *= 2;
                processed_files = realloc(processed_files, files_capacity * sizeof(char *));
            }

            char filepath[MAX_JOB_FILE_NAME_SIZE + 1];

            snprintf(filepath, sizeof(filepath), "%s/%s", dir_path, entry->d_name);

            if (is_filename_processed(entry->d_name, processed_files, processed_count)) {
                continue;
            }

            strcpy(files[*num_files].path, filepath);
            replace_extension_with_out(filepath);
            strcpy(files[*num_files].path_out, filepath);

            // Store the filename to avoid duplicates
            processed_files[processed_count] = strdup(entry->d_name);
            processed_count++;

            strcpy(files[*num_files].name, strtok(entry->d_name, "."));
            (*num_files)++;
        }
    }

    for (size_t i = 0; i < processed_count; i++) {
        free(processed_files[i]);
    }
    free(processed_files);
    return files;
}

/*
    This function compares keys in order to sort
*/
int compare_keys(const void *a, const void *b) {
    return strcmp(*(const char **)a, *(const char **)b);
}

/*
    This is the function that will execute every instruction from a file
*/
void *job_processor(void *arg){
    Main_info *info = (Main_info*) arg;
    int n_backup_file = 0, end_file = 0;
    int fd = open(info->job_files[info->n_file].path, O_RDONLY, 0644);
    int fd_out = open(info->job_files[info->n_file].path_out, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    while (1) {
            char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
            char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
            unsigned int delay;
            size_t num_pairs; 

            switch (get_next(fd)) {
                case CMD_WRITE:

                    num_pairs = parse_write(fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                    
                    if (num_pairs == 0) {
                        fprintf(stderr, "Invalid command. See HELP for usage\n");

                        continue;
                    }
                    if (kvs_write(num_pairs, keys, values)) {
                        fprintf(stderr, "Failed to write pair\n");
                    }
                    break;

                case CMD_READ:
                    num_pairs = parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                    if (num_pairs == 0) {
                        fprintf(stderr, "Invalid command. See HELP for usage\n");

                        continue;
                    }
                    if (kvs_read(num_pairs, keys, fd_out)) {
                        fprintf(stderr, "Failed to read pair\n");
                    }
                    break;

                case CMD_DELETE: 
                    num_pairs = parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                    if (num_pairs == 0) {
                        fprintf(stderr, "Invalid command. See HELP for usage\n");
                        continue;
                    }

                    if (kvs_delete(num_pairs, keys, fd_out)) {
                        fprintf(stderr, "Failed to delete pair\n");
                    }
                    break;

                case CMD_SHOW:
                    kvs_show(fd_out);
                    break;

                case CMD_WAIT:
                    if (parse_wait(fd, &delay, NULL) == -1) {
                        fprintf(stderr, "Invalid command. See HELP for usage\n");
                        continue;
                    }

                    if (delay > 0) {
                        const char *message = "Waiting...\n";
                        ssize_t bytes_written =write(fd_out,
                                                         message, (size_t)strlen(message));
                        if (bytes_written == -1) {
                            fprintf(stderr, "Erro a escrever no .out\n");
                        }
                        kvs_wait(delay);
                    }
                    break;

                case CMD_BACKUP:
                    pthread_mutex_lock(&lock);
                    n_backup_file++; // Increments number of backups per file
                    concurrent_backups++; 

                    // Makes sure we are not creating more concurrent backups than we are allowed to
                    if (concurrent_backups <= info->max_backups){
                        int id = fork();

                        if (id == 0){
                            int fd_created = kvs_backup(n_backup_file, info->dir_path, info->job_files[info->n_file].name);
                            if (fd_created) {
                                fprintf(stderr, "Failed to perform backup.\n");
                            }
                            close(fd_created);
                            _exit(0);
                        }
                    } else {
                        wait(NULL);
                        int id = fork();
                        if (id == 0){
                            int fd_created = kvs_backup(n_backup_file, info->dir_path, info->job_files[info->n_file].name);
                            if (fd_created) {
                                fprintf(stderr, "Failed to perform backup.\n");
                            }
                            close(fd_created);
                            _exit(0);
                        }
                    }
                    pthread_mutex_unlock(&lock);
                    break;

                case CMD_INVALID:   
                    fprintf(stderr, "Invalid command. See HELP for usage\n");
                    break;

                case CMD_HELP:
                    printf(
                        "Available commands:\n"
                        "  WRITE [(key,value)(key2,value2),...]\n"
                        "  READ [key,key2,...]\n"
                        "  DELETE [key,key2,...]\n"
                        "  SHOW\n"
                        "  WAIT <delay_ms>\n"
                        "  BACKUP\n"
                        "  HELP\n");
                    break;

                case CMD_EMPTY:
                    break;

                case EOC:
                    close(fd);
                    close(fd_out);
                    end_file = 1;
                    n_backup_file = 0;
                    break;
            }
            if (end_file){break;}
        }
        free(info->dir_path);
        free(info);
        return NULL;
    }

void *register_clients(void *arg){
    printf("1");
    Thread_args * args = (Thread_args*) arg;
    int register_fd = args->register_fd;
    Client *clients = args->clients;

    // To read from client
    int notif_fd, req_fd, resp_fd;

    // Loop to read indefinitely
    while (1){
        printf("cao");
        // Temos de ver se tamos a ler tudo sempre 
        char buffer[MAX_PIPE_PATH_LENGTH];
        ssize_t bytes_read = read(register_fd, buffer, MAX_PIPE_PATH_LENGTH);
        if (bytes_read == -1){
            fprintf(stderr, "Failed to read from fifo\n");
            return NULL;
        }
        if (client_counter >= MAX_SESSION_COUNT){
            fprintf(stderr, "Max number of clients reached\n");
            return NULL;
        }
        // Split the input
        char *token = strtok(buffer, " ");

        // Get notif pipe
        req_fd = atoi(token);
        token = strtok(NULL, " ");
        
        // Get resp pipe
        resp_fd = atoi(token);
        token = strtok(NULL, " ");

        // Get notif pipe
        notif_fd = atoi(token);

        clients[client_counter].active = 1;
        clients[client_counter].request_fd = req_fd;
        clients[client_counter].response_fd = resp_fd;	
        clients[client_counter].notification_fd = notif_fd;

        client_counter++;
    }
}