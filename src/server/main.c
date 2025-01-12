#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <stdio.h>
#include <errno.h>

#include "kvs.h"
#include "constants.h"
#include "parser.h"
#include "operations.h"
#include "io.h"
#include "pthread.h"
#include "src/common/io.h"
#include "src/common/protocol.h"
#include "src/common/constants.h"

struct SharedData {
  DIR* dir;
  char* dir_name;
  pthread_mutex_t directory_mutex;
};

pthread_mutex_t register_clients_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;
                                                                                                   //
size_t active_backups = 0;          // Number of active backups                                   //
size_t max_backups;                // Maximum allowed simultaneous backups                       //
size_t max_threads;               // Maximum allowed simultaneous threads                         //
char register_fifo_name[MAX_PIPE_PATH_LENGTH] = "/tmp/";     // Register FIFO name               //
char* jobs_directory = NULL;        // Jobs directory                                             //
Client *clients; // Array of clients                                           //
int session_count = 0;            // Number of active sessions                                  //

int filter_job_files(const struct dirent* entry) {
    const char* dot = strrchr(entry->d_name, '.');
    if (dot != NULL && strcmp(dot, ".job") == 0) {
        return 1;  // Keep this file (it has the .job extension)
    }
    return 0;
}

static int entry_files(const char* dir, struct dirent* entry, char* in_path, char* out_path) {
  const char* dot = strrchr(entry->d_name, '.');
  if (dot == NULL || dot == entry->d_name || strlen(dot) != 4 || strcmp(dot, ".job")) {
    return 1;
  }

  if (strlen(entry->d_name) + strlen(dir) + 2 > MAX_JOB_FILE_NAME_SIZE) {
    fprintf(stderr, "%s/%s\n", dir, entry->d_name);
    return 1;
  }

  strcpy(in_path, dir);
  strcat(in_path, "/");
  strcat(in_path, entry->d_name);

  strcpy(out_path, in_path);
  strcpy(strrchr(out_path, '.'), ".out");

  return 0;
}

static int run_job(int in_fd, int out_fd, char* filename) {
  size_t file_backups = 0;
  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    switch (get_next(in_fd)) {
      case CMD_WRITE:
        num_pairs = parse_write(in_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_write(num_pairs, keys, values)) {
          write_str(STDERR_FILENO, "Failed to write pair\n");
        }
        break;

      case CMD_READ:
        num_pairs = parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_read(num_pairs, keys, out_fd)) {
          write_str(STDERR_FILENO, "Failed to read pair\n");
        }
        break;

      case CMD_DELETE:
        num_pairs = parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        printf("num_pairs: %ld\n", num_pairs);
        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_delete(num_pairs, keys, out_fd)) {
          write_str(STDERR_FILENO, "Failed to delete pair\n");
        }
        break;

      case CMD_SHOW:
        kvs_show(out_fd);
        break;

      case CMD_WAIT:
        if (parse_wait(in_fd, &delay, NULL) == -1) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay > 0) {
          printf("Waiting %d seconds\n", delay / 1000);
          kvs_wait(delay);
        }
        break;

      case CMD_BACKUP:
        pthread_mutex_lock(&n_current_backups_lock);
        if (active_backups >= max_backups) {
          wait(NULL);
        } else {
          active_backups++;
        }
        pthread_mutex_unlock(&n_current_backups_lock);
        int aux = kvs_backup(++file_backups, filename, jobs_directory);

        if (aux < 0) {
            write_str(STDERR_FILENO, "Failed to do backup\n");
        } else if (aux == 1) {
          return 1;
        }
        break;

      case CMD_INVALID:
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        break;

      case CMD_HELP:
        write_str(STDOUT_FILENO,
            "Available commands:\n"
            "  WRITE [(key,value)(key2,value2),...]\n"
            "  READ [key,key2,...]\n"
            "  DELETE [key,key2,...]\n"
            "  SHOW\n"
            "  WAIT <delay_ms>\n"
            "  BACKUP\n" // Not implemented
            "  HELP\n");

        break;

      case CMD_EMPTY:
        break;

      case EOC:
        printf("EOF\n");
        return 0;
    }
  }
}

void* run_client(void *args){

  Client* client = (Client*) args;
  client->active = 1;
  // Mudar os argumentos para void* args e depois fazer casting para conseguilos 
  // Passei cliente porque era uma estrutura que nos ja temos feito e da jeito por isso ta gg
  // Basicamente meti um loop infinito no registration fifo e sempre que lia informacao sobre um cliente crio logo numa thread esta funcao
  // nao tenho a certeza que funciona mas agora conseguimos fazer isto

  char op[2]; 
  char buffer[MAX_KEY_SIZE];
  int result;
  int intr = 0;

  printf("Entrar no run client\n");

  memset(buffer, '\0', MAX_KEY_SIZE);
  while (1){

    printf("Ler op\n");
    if (read_all(client->request_fd, op, 1, &intr) == -1) {
      if (intr){
        fprintf(stderr, "Reading from request FIFO was interrupted\n");
      } else {
        fprintf(stderr, "Failed to read from request FIFO\n");
      }
      return NULL;
    }
    printf("op: %s\n", op);

    switch(atoi(op)){
      case OP_CODE_CONNECT:
      case OP_CODE_DISCONNECT:
        result = disconnect(client);

        if(result == 1){
          fprintf(stderr, "Failed to disconnect client\n");
        }
        snprintf(buffer, MAX_KEY_SIZE, "%d%d", atoi(op), result);

        if (close(client->request_fd) == -1){
          fprintf(stderr, "Failed to close fifo\n");
        }

        if (close(client->response_fd) == -1){
          fprintf(stderr, "Failed to close fifo\n");
        }

        if (close(client->notification_fd) == -1){
          fprintf(stderr, "Failed to close fifo\n");
        }
        
        if (write_all(client->response_fd, buffer, MAX_KEY_SIZE) == -1) {
          fprintf(stderr, "Failed to read from the request FIFO");
          return NULL;
        }

        break;

      case OP_CODE_SUBSCRIBE:

        if (read_all(client->request_fd, buffer, MAX_KEY_SIZE, &intr) == -1) {
          if (intr){
            fprintf(stderr, "Reading from request FIFO was interrupted\n");
          } else {
            fprintf(stderr, "Failed to read from request FIFO\n");
          }
        }
        printf("começou a subscrição\n");
        result = subscribe(buffer, client->id, client->response_fd, client->notification_fd);

        if (result == 1){
          if (iniciar_subscricao(client, buffer) == 1){
            fprintf(stderr, "Failed to iniciate subscription\n");
          }
        }

        break;

      case OP_CODE_UNSUBSCRIBE:
      
        if (read_all(client->request_fd, buffer, MAX_KEY_SIZE, &intr) == -1) {
          if (intr){
            fprintf(stderr, "Reading from request FIFO was interrupted\n");
          } else {
            fprintf(stderr, "Failed to read from the request FIFO");
          }
          return NULL;
        }

        result = unsubscribe(buffer, client->id, client->response_fd, client->notification_fd);

        if (result == 0){
          if (apagar_subscricao(client->sub_keys, buffer) == 1){
            fprintf(stderr, "Failed to delete subscription\n");
          }
        }
        break;
    }
  }
  return NULL;
}

//frees arguments
static void* get_file(void* arguments) {
  struct SharedData* thread_data = (struct SharedData*) arguments;
  DIR* dir = thread_data->dir;
  char* dir_name = thread_data->dir_name;

  if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to lock directory_mutex\n");
    return NULL;
  }

  struct dirent* entry;
  char in_path[MAX_JOB_FILE_NAME_SIZE], out_path[MAX_JOB_FILE_NAME_SIZE];
  while ((entry = readdir(dir)) != NULL) {
    if (entry_files(dir_name, entry, in_path, out_path)) {
      continue;
    }

    if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to unlock directory_mutex\n");
      return NULL;
    }

    int in_fd = open(in_path, O_RDONLY);
    if (in_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open input file: ");
      write_str(STDERR_FILENO, in_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (out_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open output file: ");
      write_str(STDERR_FILENO, out_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out = run_job(in_fd, out_fd, entry->d_name);

    close(in_fd);
    close(out_fd);

    if (out) {
      if (closedir(dir) == -1) {
        fprintf(stderr, "Failed to close directory\n");
        return 0;
      }

      exit(0);
    }

    if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to lock directory_mutex\n");
      return NULL;
    }
  }

  if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to unlock directory_mutex\n");
    return NULL;
  }

  pthread_exit(NULL);
}

// Function to be executed by the host thread
/*
  The host thread reads from the register fifo and registers clients while opening its fifos
*/
void* get_register(void* arg){

  int intr = 0;

  if (arg != NULL){
    fprintf(stderr, "Invalid argument\n");
    return NULL;
  }
  char buffer[BUFFER_SIZE];
  printf("register fifo = %s\n", register_fifo_name);
  if (mkfifo(register_fifo_name, 0666) == -1 && errno != EEXIST){
      fprintf(stderr, "Failed to create fifo\n");
      return NULL;
  }
  
  while (1){
    int fd = open(register_fifo_name, O_RDONLY);
    if (fd == -1){
      fprintf(stderr, "Failed to open fifo\n");
      return NULL;
    }
    Client* client = (Client*)malloc(sizeof(Client));


    if (read_all(fd, buffer, BUFFER_SIZE, &intr) == -1){
      if (intr == 1){
        fprintf(stderr, "Reading from register FIFO was interrupted\n");
      } else {
        fprintf(stderr, "Failed to read from register fifo\n");
      }
      return NULL;
    }

    // Handle Op-code
    char *token = strtok(buffer, " ");


    if (strcmp(token, "0") != 0){
      fprintf(stderr, "Invalid command\n");
      return NULL;
    }
    // Opens requests pipe for reading
    token = strtok(NULL, " ");

    int fd_req_pipe = open(token, O_RDONLY);
    if (fd_req_pipe == -1){
      fprintf(stderr, "Failed to open fifo\n");
      return NULL;
    }
    client->request_fd = fd_req_pipe;
    // Opens response pipe for writing
    token = strtok(NULL, " ");

    int fd_resp_pipe = open(token, O_WRONLY);
    if (fd_resp_pipe == -1){
      fprintf(stderr, "Failed to open fifo\n");
      close(fd_req_pipe);
      return NULL;
    }
    client->response_fd = fd_resp_pipe;

    // Opens notification pipe for writing
    token = strtok(NULL, " ");
   
    int fd_notif_pipe = open(token, O_WRONLY);
    if (fd_notif_pipe == -1){
      fprintf(stderr, "Failed to open fifo\n");
      close(fd_req_pipe);
      close(fd_resp_pipe);
      return NULL;
    }
    client->notification_fd = fd_notif_pipe;

    // Assigns an id to the client
    token = strtok(NULL, " ");
   
    client->id = token;

    add_client(&clients, client);

    // CHANGEME para fazer com mais clientes: criar uma lista em cima antes do while probably com max session count e gg
    pthread_t client_thread;
    // Create thread for client
    
    pthread_create(&client_thread, NULL, run_client, (void*)client);
    close(fd);
  }

  return NULL;
}

static void dispatch_threads(DIR* dir) {
  pthread_t* threads = malloc(max_threads * sizeof(pthread_t));
  pthread_t host_thread; // Tarefa Anfitriã

  if (threads == NULL) {
    fprintf(stderr, "Failed to allocate memory for threads\n");
    return;
  }

  struct SharedData thread_data = {dir, jobs_directory, PTHREAD_MUTEX_INITIALIZER};

  // Create host thread
  if (pthread_create(&host_thread, NULL, get_register, NULL) != 0){
      fprintf(stderr, "Failed to create host task\n");
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return; 
  }

  for (size_t i = 0; i < max_threads; i++) {
    if (pthread_create(&threads[i], NULL, get_file, (void*)&thread_data) != 0) {
      fprintf(stderr, "Failed to create thread %zu\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  // ler do FIFO de registo

  for (unsigned int i = 0; i < max_threads; i++) {
    if (pthread_join(threads[i], NULL) != 0) {
      fprintf(stderr, "Failed to join thread %u\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }
  if (pthread_join(host_thread, NULL) != 0){
    fprintf(stderr, "Failed to join host thread\n");
    pthread_mutex_destroy(&thread_data.directory_mutex);
    free(threads);
    return;
  }

  if (pthread_mutex_destroy(&thread_data.directory_mutex) != 0) {
    fprintf(stderr, "Failed to destroy directory_mutex\n");
  }

  free(threads);
}


int main(int argc, char** argv) {
  if (argc < 4) {
    write_str(STDERR_FILENO, "Usage: ");
    write_str(STDERR_FILENO, argv[0]);
    write_str(STDERR_FILENO, " <jobs_dir>");
		write_str(STDERR_FILENO, " <max_threads>");
		write_str(STDERR_FILENO, " <max_backups> \n");
    return 1;
  }

  clients = malloc(MAX_SESSION_COUNT * sizeof(Client));

  jobs_directory = argv[1];
  strcat(register_fifo_name, argv[4]);

  char* endptr;
  max_backups = strtoul(argv[3], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_proc value\n");
    return 1;
  }

  max_threads = strtoul(argv[2], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_threads value\n");
    return 1;
  }

	if (max_backups <= 0) {
		write_str(STDERR_FILENO, "Invalid number of backups\n");
		return 0;
	}

	if (max_threads <= 0) {
		write_str(STDERR_FILENO, "Invalid number of threads\n");
		return 0;
	}

  if (kvs_init()) {
    write_str(STDERR_FILENO, "Failed to initialize KVS\n");
    return 1;
  }

  DIR* dir = opendir(argv[1]);
  if (dir == NULL) {
    fprintf(stderr, "Failed to open directory: %s\n", argv[1]);
    return 0;
  }

  dispatch_threads(dir);

  if (closedir(dir) == -1) {
    fprintf(stderr, "Failed to close directory\n");
    return 0;
  }

  while (active_backups > 0) {
    wait(NULL);
    active_backups--;
  }

  kvs_terminate();
  return 0;
}
