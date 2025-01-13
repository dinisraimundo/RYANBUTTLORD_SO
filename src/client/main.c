#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>

#include "parser.h"
#include "src/client/api.h"
#include "src/common/constants.h"
#include "src/common/io.h"

int notif_fifo, req_fifo, resp_fifo; 

void* reads_notifs(void* arg){

  int *fd_notif = (int*) arg;
  int fd_notif_pipe = *fd_notif;
  char key_buffer[MAX_KEY_SIZE];
  char value_buffer[MAX_KEY_SIZE];
  int intr = 0;
  
  while(1){
    if (read_all(fd_notif_pipe, key_buffer, sizeof(char)*MAX_KEY_SIZE, &intr) == -1) {
      if (intr){
        fprintf(stderr, "Reading from the notification FIFO was interrupted\n");
      } else {
        fprintf(stderr, "Failed to read from the notification FIFO\n");
      }
      return NULL;
    }
    if(strcmp(key_buffer, "disconnect_sigma") == 0){

      if (write_all(req_fifo, "9", 1) == -1){
        fprintf(stderr, "Failed to write to fifo\n");
        return NULL;
      }
      kill(getpid(), SIGKILL);
    }

    if (read_all(fd_notif_pipe, value_buffer, sizeof(char)*MAX_KEY_SIZE, &intr) == -1) {
      if (intr){
        fprintf(stderr, "Reading from the notification FIFO was interrupted\n");
      } else {
        fprintf(stderr, "Failed to read from the notification FIFO\n");
      }
      return NULL;
    }

    printf("(%s,%s)\n", key_buffer, value_buffer);
  }
  return NULL;
}


int main(int argc, char* argv[]) {
  if (argc < 3) {
    fprintf(stderr, "Usage: %s <client_unique_id> <register_pipe_path>\n", argv[0]);
    return 1;
  }
  
  pthread_t thread_id;
  char req_pipe_path[256] = "/tmp/req";
  char resp_pipe_path[256] = "/tmp/resp";
  char notif_pipe_path[256] = "/tmp/notif";
  char register_pipe_path[256] = "/tmp/";
  strcat(register_pipe_path, argv[2]);

  char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE] = {0};
  unsigned int delay_ms;
  size_t num;

  strncat(req_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(resp_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(notif_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));

  req_pipe_path[strlen(req_pipe_path)] = '\0';
  resp_pipe_path[strlen(resp_pipe_path)] = '\0';
  notif_pipe_path[strlen(notif_pipe_path)] = '\0';
  int connection = kvs_connect(req_pipe_path, resp_pipe_path, register_pipe_path, notif_pipe_path, &notif_fifo, &req_fifo, &resp_fifo);
  
  if(connection < 0){
    if(connection == -1){
      // Close the notification fifo
      if (close(notif_fifo) == -1){
        fprintf(stderr, "Failed to close notification fifo\n");
        return 1;
      }
      
      // Close the request fifo
      if (close(req_fifo) == -1){
        fprintf(stderr, "Failed to close request fifo\n");
        return 1;
      }

      // Close the response fifo
      if (close(resp_fifo) == -1){
        fprintf(stderr, "Failed to close response fifo\n");
        return 1;
      }

      if (unlink(req_pipe_path) == -1) {
          fprintf(stderr, "Failed to unlink FIFO");
      }
      if (unlink(resp_pipe_path) == -1) {
          fprintf(stderr, "Failed to unlink FIFO");
      }
      if (unlink(notif_pipe_path) == -1) {
          fprintf(stderr, "Failed to unlink FIFO");
      }
    }
  
    printf("Server returned 1 for operation: connect\n");
    return 1;
  }
  else{
    printf("Server returned 0 for operation: connect\n");
  }

  if (pthread_create(&thread_id, NULL, reads_notifs, (void*)&notif_fifo) != 0) {
    fprintf(stderr, "Failed to create thread");
    return -1;
  }
  
  while (1) {
    switch (get_next(STDIN_FILENO)) {
      case CMD_DISCONNECT:
        pthread_cancel(thread_id);
        pthread_join(thread_id, NULL);
        // debug this bs
        if (kvs_disconnect(req_pipe_path, resp_pipe_path, notif_pipe_path, req_fifo, resp_fifo, notif_fifo) != 0) {
          fprintf(stderr, "Failed to disconnect to the server\n");
          return -1;
        }
        // TODO: end notifications thread
        return 0;

      case CMD_SUBSCRIBE:
        num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
        if (num == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }
         
        if (kvs_subscribe(keys[0], req_fifo, resp_fifo) == -1) {
            fprintf(stderr, "Command subscribe failed\n");
        }

        break;

      case CMD_UNSUBSCRIBE:
        num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
        if (num == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }
         
        if (kvs_unsubscribe(keys[0], req_fifo, resp_fifo) == -1) {
            fprintf(stderr, "Command unsubscribe failed\n");
        }

        break;

      case CMD_DELAY:
        if (parse_delay(STDIN_FILENO, &delay_ms) == -1) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay_ms > 0) {
            printf("Waiting...\n");
            delay(delay_ms);
        }
        break;

      case CMD_INVALID:
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        break;

      case CMD_EMPTY:
        break;

      case EOC:
        // input should end in a disconnect, or it will loop here forever
        break;
    }
  }
  if (pthread_join(thread_id, NULL) != 0) {
    fprintf(stderr, "Failed to join thread");
    return 1;
  }
}
