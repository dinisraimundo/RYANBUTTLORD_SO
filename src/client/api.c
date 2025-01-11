#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <fcntl.h> 
#include <sys/wait.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "api.h"
#include "src/common/constants.h"
#include "src/common/protocol.h"

int kvs_connect(char const* req_pipe_path, char const* resp_pipe_path, char const* server_pipe_path,
                char const* notif_pipe_path, int* notif_fifo, int* req_fifo, int* resp_fifo) {

  // Create fifos
  int register_fifo;

  // Create the request fifo
  if (mkfifo(req_pipe_path, 0666) == -1){
    fprintf(stderr, "Failed to create fifo\n");
    return -1;
  }

  // Create the response fifo
  if (mkfifo(resp_pipe_path, 0666) == -1){
    fprintf(stderr, "Failed to create fifo\n");
    return -1;
  }

  // Create the notification fifo
  if (mkfifo(notif_pipe_path, 0666) == -1){
    fprintf(stderr, "Failed to create fifo\n");
    return -1;
  }
 

  // Open the register fifo
  if ((register_fifo = open(server_pipe_path, O_WRONLY)) == -1){
    fprintf(stderr, "Failed to open fifo\n");
    return -1;
  }

  // Open the request fifo
  if ((*req_fifo = open(req_pipe_path, O_WRONLY)) == -1){
    fprintf(stderr, "Failed to open fifo\n");
    return -1;
  }
 
  // Open the response fifo
  if ((*resp_fifo = open(resp_pipe_path, O_RDONLY)) == -1){
    fprintf(stderr, "Failed to open fifo\n");
    return -1;
  }

  // Open the notification fifo
  if ((*notif_fifo = open(notif_pipe_path, O_RDONLY)) == -1){
    fprintf(stderr, "Failed to open fifo\n");
    return -1;
  }

  // Get the client id
  char last_char = req_pipe_path[strlen(req_pipe_path) - 1];

  // Send the Op-code, client id and each fifos fd to the server
  // CHANGEME - Change the buffer size
  char buffer[BUFFER_SIZE + 10];
  sprintf(buffer, "0 %s %s %s %c", req_pipe_path, resp_pipe_path, notif_pipe_path, last_char);
  if (write_all(register_fifo, buffer, BUFFER_SIZE + 10) == -1){
    fprintf(stderr, "Failed to write to fifo\n");
    return -1;
  }

  close(register_fifo);
  return 0;
}
 
int kvs_disconnect(char const* req_pipe_path, char const* resp_pipe_path, char const* notif_pipe_path,
                    int fd_req_pipe, int fd_resp_pipe, int fd_notif_pipe) {
  char buffer[3];
  int op_code, result;

  memset(buffer, '\0', 3);
  strcpy(buffer, "2");

  // Se não tivermos fechado os fds primeiro temos de os trazer para aqui e fachá-los para posteriormente dar unlink
  // close pipes and unlink pipe files
  printf("Disconnecting the following pipes from the server: %s%s%s\n", req_pipe_path, resp_pipe_path, notif_pipe_path);

  if (write_all(fd_req_pipe, buffer, sizeof(char)* 2) == -1) {
    perror("Failed to write to request FIFO");
    return -1;
  }

  if (read_all(fd_resp_pipe, buffer, sizeof(char)*3) == -1) {
    perror("Failed to read from response FIFO");
    return -1;
  }

  op_code = atoi(buffer[0]);
  result = atoi(buffer[1]);

  if(op_code != 2){
    fprintf(stderr, "Op_code errado no kvs_subscribe\n");
  }

  if(result == 0){
    // Close the request fifo
    if (close(fd_req_pipe) == -1){
      fprintf(stderr, "Failed to close fifo\n");
      return 1;
    }

    // Close the response fifo
    if (close(fd_resp_pipe) == -1){
      fprintf(stderr, "Failed to close fifo\n");
      return 1;
    }

    // Close the notification fifo
    if (close(fd_notif_pipe) == -1){
      fprintf(stderr, "Failed to close fifo\n");
      return 1;
    }

    if (unlink(req_pipe_path) == -1) {
        perror("Failed to unlink FIFO");
    }
    if (unlink(resp_pipe_path) == -1) {
        perror("Failed to unlink FIFO");
    }
    if (unlink(notif_pipe_path) == -1) {
        perror("Failed to unlink FIFO");
    }
  }
  return 0;
}

int kvs_subscribe(const char* key, int fd_req_pipe, int fd_resp_pipe) {
  // send subscribe message to request pipe and wait for response in response pipe
  int op_code, result;
  char buffer[KEY_OPCODE];

  memset(buffer, '\0', KEY_OPCODE);
  strcpy(buffer, "3");
  strcat(buffer, key);

  if (write_all(fd_req_pipe, buffer, sizeof(buffer)) == -1) {
    perror("Failed to write to request FIFO");
    return -1;
  }
  
  if (read_all(fd_resp_pipe, buffer, sizeof(char)*3) == -1) {
    perror("Failed to read from response FIFO");
    return -1;
  }

  op_code = atoi(buffer[0]);
  result = atoi(buffer[1]);

  if(op_code != 3){
    fprintf(stderr, "Op_code errado no kvs_subscribe\n");
  }

  printf("%d\n", fd_resp_pipe);

  printf("Server returned %d for operation: subscribe\n", result);

  return result;
}

int kvs_unsubscribe(const char* key, int fd_req_pipe, int fd_resp_pipe) {
  int op_code, result;
  char buffer[KEY_OPCODE];
  memset(buffer, '\0', KEY_OPCODE);
  strcpy(buffer, "4");
  strcat(buffer, key);

  if (write_all(fd_req_pipe, buffer, sizeof(buffer)) == -1) {
    perror("Failed to write to request FIFO");
    return -1;
  }

  if (read_all(fd_resp_pipe, buffer, sizeof(buffer)) == -1) {
    perror("Failed to read from response FIFO");
    return -1;
  }

  op_code = atoi(buffer[0]);
  result = atoi(buffer[1]);

  if(op_code != 4){
    fprintf(stderr, "Op_code errado no kvs_unsubscribe\n");
  }

  printf("%d\n", fd_resp_pipe);
  
  printf("Server returned %d for operation: unsubscribe\n", result);

  return result;
}


