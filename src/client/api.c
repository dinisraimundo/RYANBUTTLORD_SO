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
#include "src/common/io.h"
#include "src/common/constants.h"
#include "src/common/protocol.h"

int kvs_connect(const char* req_pipe_path, char const* resp_pipe_path, char const* server_pipe_path,
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
    fprintf(stderr, "Failed to open register FIFO\n");
    return -1;
  }

  char* client_id = (char*)req_pipe_path + 8;
  
  // Send the Op-code, client id and each fifos fd to the server
  // CHANGEME - Change the buffer size
  char buffer[BUFFER_SIZE];
  memset(buffer, '\0', BUFFER_SIZE);
  sprintf(buffer, "0 %s %s %s %s", req_pipe_path, resp_pipe_path, notif_pipe_path, client_id);
  if (write_all(register_fifo, buffer, BUFFER_SIZE) == -1){
    fprintf(stderr, "Failed to write to fifo\n");
    return -1;
  }
  // maybe semaforo aqui
  close(register_fifo);
  // Open the request fifo
  if ((*req_fifo = open(req_pipe_path, O_WRONLY)) == -1){
    fprintf(stderr, "Failed to open requests FIFO\n");
    return -1;
  }

  // Open the response fifo
  if ((*resp_fifo = open(resp_pipe_path, O_RDONLY)) == -1){
    fprintf(stderr, "Failed to open response FIFO\n");
    return -1;
  }

  // Open the notification fifo
  if ((*notif_fifo = open(notif_pipe_path, O_RDONLY)) == -1){
    fprintf(stderr, "Failed to open notifications FIFO\n");
    return -1;
  }

  // Get the client id


  return 0;
}
 
int kvs_disconnect(char const* req_pipe_path, char const* resp_pipe_path, char const* notif_pipe_path,
                    int fd_req_pipe, int fd_resp_pipe, int fd_notif_pipe) {
  int op_code, result;
  char buffer[KEY_OPCODE];

  memset(buffer, '\0', KEY_OPCODE);
  strcpy(buffer, "2");

  // Se não tivermos fechado os fds primeiro temos de os trazer para aqui e fachá-los para posteriormente dar unlink
  // close pipes and unlink pipe files

  int intr = 0;

  if (write_all(fd_req_pipe, buffer, sizeof(buffer)) == -1) {
    fprintf(stderr, "Failed to write to request FIFO\n");
    return -1;
  }

  if (read_all(fd_resp_pipe, buffer, sizeof(char)*3, &intr) == -1) {
    if (intr){
      fprintf(stderr, "Reading from response FIFO was interrupted\n");  

    } else {
      fprintf(stderr, "Failed to read from response FIFO\n");
    }
    return -1;
  }

  char op_code_str[2];
  char result_str[2];

  op_code_str[0] = buffer[0];
  op_code_str[1] = '\0'; 

  // Copy the second character as the result
  result_str[0] = buffer[1];
  result_str[1] = '\0';

  op_code = atoi(op_code_str);
  result = atoi(result_str);

  // CHANGEME - que isto
  if (op_code != 2){
    fprintf(stderr, "Op_code errado no kvs_subscribe\n");
  }

  printf("Server returned %d for operation: disconnect\n", result);

  if (result == 0){
    // Close the notification fifo
    if (close(fd_notif_pipe) == -1){
      fprintf(stderr, "Failed to close fifo\n");
      return 1;
    }
    
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
  return 0;
}

int kvs_subscribe(const char* key, int fd_req_pipe, int fd_resp_pipe) {
  // send subscribe message to request pipe and wait for response in response pipe
  int op_code, result;
  char buffer[KEY_OPCODE];

  memset(buffer, '\0', KEY_OPCODE);
  strcpy(buffer, "3");
  strcat(buffer, key);
  int intr = 0;

  if (write_all(fd_req_pipe, buffer, sizeof(buffer)) == -1) {
    perror("Failed to write to request FIFO");
    return -1;
  }
  
  if (read_all(fd_resp_pipe, buffer, sizeof(char)*3, &intr) == -1) {
    if (intr){
      fprintf(stderr, "Reading from response FIFO was interrupted\n");  

    } else {
      fprintf(stderr, "Failed to read from response FIFO\n");
    }
    return -1;
  }

  char op_code_str[2];
  char result_str[2];

  op_code_str[0] = buffer[0];
  op_code_str[1] = '\0'; 

  // Copy the second character as the result
  result_str[0] = buffer[1];
  result_str[1] = '\0';
  
  op_code = atoi(op_code_str);
  result = atoi(result_str);

  if(op_code != 3){
    fprintf(stderr, "Op_code errado no kvs_subscribe\n");
  }

  printf("Server returned %d for operation: subscribe\n", result);

  return result;
}

int kvs_unsubscribe(const char* key, int fd_req_pipe, int fd_resp_pipe) {
  int op_code, result;
  char buffer[KEY_OPCODE];
  memset(buffer, '\0', KEY_OPCODE);
  strcpy(buffer, "4");
  strcat(buffer, key);
  int intr = 0;
  
  if (write_all(fd_req_pipe, buffer, sizeof(buffer)) == -1) {
    perror("Failed to write to request FIFO");
    return -1;
  }

  if (read_all(fd_resp_pipe, buffer, sizeof(char)*3, &intr) == -1) {
    if (intr){
      fprintf(stderr, "Reading from response FIFO was interrupted\n");  

    } else {
      fprintf(stderr, "Failed to read from response FIFO\n");
    }

    return -1;
  }

  char op_code_str[2];
  char result_str[2];

  op_code_str[0] = buffer[0];
  op_code_str[1] = '\0'; 

  // Copy the second character as the result
  result_str[0] = buffer[1];
  result_str[1] = '\0';
  
  op_code = atoi(op_code_str);
  result = atoi(result_str);

  if(op_code != 4){
    fprintf(stderr, "Op_code errado no kvs_unsubscribe\n");
  }

  
  printf("Server returned %d for operation: unsubscribe\n", result);

  return result;
}


