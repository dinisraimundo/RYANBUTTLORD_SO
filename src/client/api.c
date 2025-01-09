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
                char const* notif_pipe_path) { // Retirei dos parametros o int* notif_pipe porque não uso
  // Create fifos
  int register_fifo, notif_fifo, req_fifo, resp_fifo;

  // Create the request fifo
  if (mkfifo(req_pipe_path, 0777) == -1){
    fprintf(stderr, "Failed to create fifo\n");
    return 1;
  }

  // Create the response fifo
  if (mkfifo(resp_pipe_path, 0777) == -1){
    fprintf(stderr, "Failed to create fifo\n");
    return 1;
  }
  // Create the notification fifo
  if (mkfifo(notif_pipe_path, 0777) == -1){
    fprintf(stderr, "Failed to create fifo\n");
    return 1;
  }

  // Open fifos
 fprintf(stderr, "cadela");
 fprintf(stderr, "server pipe path = %s\n", server_pipe_path);
 
  // Open the register fifo
  if ((register_fifo = open(server_pipe_path, O_WRONLY)) == -1){
    fprintf(stderr, "Failed to open fifo\n");
    return 1;
  }
   fprintf(stderr, "cao");
  // Open the request fifo
  if ((req_fifo = open(req_pipe_path, O_WRONLY|O_NONBLOCK)) == -1){
    fprintf(stderr, "Failed to open fifo\n");
    return 1;
  }
  printf("entrada");  
  // Open the response fifo
  if ((resp_fifo = open(resp_pipe_path, O_WRONLY|O_NONBLOCK)) == -1){
    fprintf(stderr, "Failed to open fifo\n");
    return 1;
  }

  // Open the notification fifo
  // PRECISO DE LER DO FIFO PORQUE ISTO TA BLOQUEADO
  if ((notif_fifo = open(notif_pipe_path, O_RDONLY|O_NONBLOCK)) == -1){
    fprintf(stderr, "Failed to open fifo\n");
    return 1;
  }
  fprintf(stderr, "cao2");
  fprintf(stderr, "register fifo = %d\n", register_fifo);
  // Send the client id and each fifos fd to the server
  char buffer[MAX_PIPE_PATH_LENGTH];
  sprintf(buffer, "%s %s %s", req_pipe_path, resp_pipe_path, notif_pipe_path);
  ssize_t bytes_writted = write(register_fifo, buffer, MAX_PIPE_PATH_LENGTH);
  fprintf(stderr, "bytes writted = %ld\n", bytes_writted);
  close(register_fifo);
  return 0;
}
 
int kvs_disconnect(char const* req_pipe_path, char const* resp_pipe_path, char const* notif_pipe_path) {
  // Se não tivermos fechado os fds primeiro temos de os trazer para aqui e fachá-los para posteriormente dar unlink
  // close pipes and unlink pipe files

  // Close the request fifo
  if (close(req_pipe_path) == -1){
    fprintf(stderr, "Failed to close fifo\n");
    return 1;
  }

  // Close the response fifo
  if (close(resp_pipe_path) == -1){
    fprintf(stderr, "Failed to close fifo\n");
    return 1;
  }

  // Close the notification fifo
  if (close(notif_pipe_path) == -1){
    fprintf(stderr, "Failed to close fifo\n");
    return 1;
  }



  return 0;
}

int kvs_subscribe(const char* key, int fd_req_pipe, int fd_resp_pipe) {
  // send subscribe message to request pipe and wait for response in response pipe
  
  int index = hash(key);
  char buffer[KEY_OPCODE];
  memset(buffer, '\0', KEY_OPCODE);
  strcpy(buffer, "3");
  strcat(buffer, key);

  if (write(fd_req_pipe, buffer, sizeof(buffer)) == -1) {
    perror("Failed to write to request FIFO");
    return -1;
  }



  if (*key < 1){
    return 1;
  }
  return 0;
}

int kvs_unsubscribe(const char* key) {
    // send unsubscribe message to request pipe and wait for response in response pipe
    if (*key < 1){
    return 1;
  }
  return 0;
}


