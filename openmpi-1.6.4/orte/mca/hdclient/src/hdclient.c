#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <stddef.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdbool.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>

#include "hdclient.h"
#include "hdclient-constants.h"
#include "str_utils.h"
#include "pbc/pbc.h"

#define HANDSHAKE "hamster-001"

struct pbc_env* orte_hdclient_pb_env = NULL;
int orte_umbilical_socket_id = -1;

static int connect_to_server(int socket_id, const char* host, int port);

static void read_file(const char* filename, struct pbc_slice* slice);

int orte_hdclient_init_pb_env() {
    struct pbc_slice slice;
    int rc;

    // get pb file path
    const char* pb_file = getenv(YARN_PB_FILE_ENV_KEY);
    if (!pb_file) {
        opal_output(0, "failed to get YARN_PB_FILE envar"); 
        return -1;
    }

    // read pb file content
    orte_hdclient_pb_env = pbc_new();
    read_file(pb_file, &slice);
    if (!slice.buffer) {
        opal_output(0, "read pb file failed, %s", pb_file);
        return -1;
    }

    // register pb file to pb env
    rc = pbc_register(orte_hdclient_pb_env, &slice);
    if (rc) {
        opal_output(0, "error when register, %s", pbc_error(orte_hdclient_pb_env));
        return -1;
    }

    // cleanups
    free(slice.buffer);
    return 0;
}

int orte_hdclient_connect_to_am() {
    int rc;

    // init socket
    orte_umbilical_socket_id = socket(AF_INET, SOCK_STREAM, 0);

    // read host/port
    char host[1024];
    rc = gethostname(host, 1023);
    char* port_str = getenv(AM_UMBILICAL_PORT_ENV_KEY);
    if (!port_str || (0 != rc)) {
        opal_output(0, "failed to get host or port when trying to connect to AM");
        return -1;
    }
    int port = atoi(port_str);

    // connect to server
    rc = connect_to_server(orte_umbilical_socket_id, host, port);
    if (0 != rc) {
        opal_output(0, "connect to AM failed.");
        return -1;
    }

    // send handshake
    rc = write_all(orte_umbilical_socket_id, HANDSHAKE, strlen(HANDSHAKE));
    if (0 != rc) {
        opal_output(0, "send handshake to socket failed");
        return -1;
    }

    // recv handshake
    char recv[strlen(HANDSHAKE) + 1];
    rc = read_all(orte_umbilical_socket_id, recv, strlen(HANDSHAKE));
    if (0 != rc) {
        opal_output(0, "read handshake from socket failed");
        return -1;
    }
    recv[strlen(HANDSHAKE)] = '\0';

    // valid handshake recved from AM
    if (0 != strcmp(HANDSHAKE, recv)) {
        opal_output(0, "failed to validate handshake from AM, read str is %s", recv);
        return -1;
    }

    return 0;
}

int orte_hdclient_send_message_and_delete(struct pbc_wmessage* msg, enum_hamster_msg_type type) {
    int rc;
    struct pbc_slice slice;
    struct pbc_slice send_slice;

    // insanity check
    if (!msg) {
        opal_output(0, "a NULL msg passed in, what happened?");
        return -1;
    }
    pbc_wmessage_buffer(msg, &slice);

    // create message for send
    struct pbc_wmessage_msg* send_msg = pbc_wmessage_new(orte_hdclient_pb_env, "HamsterHnpRequestProto");
    if (!send_msg) {
        opal_output(0, "failed to create HamsterHnpRequestProto");
        return -1;
    }
    pbc_wmessage_string(send_msg, "request", slice.buffer, slice.len);
    pbc_wmessage_integer(send_msg, "msg_type", type, 0);
    pbc_wmessage_buffer(send_msg, &send_slice);
    
    // pack message and send
    write_endian_swap_int(orte_umbilical_socket_id, send_slice.len);
    rc = write_all(orte_umbilical_socket_id, send_slice.buffer, send_slice.len);
    if (rc != 0) {
        opal_output(0, "failed to send message to AM");
        pbc_wmessage_delete(msg);
        return -1;
    }

    // clean up
    pbc_wmessage_delete(msg);
    pbc_wmessage_delete(send_msg);
    return 0;
}

/* connect_to_server, return 0 if succeed */
static int connect_to_server(int socket_id, const char* host, int port) {
    //define socket variables
    struct sockaddr_in serv_addr;
    struct hostent *server;

    //init port / socket / server
    server = gethostbyname(host);
    if (server == NULL)
    {
        opal_output(0, "ERROR, no such host.\n");
        return -1;
    }
    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr,
          (char *)&serv_addr.sin_addr.s_addr,
                server->h_length);
                serv_addr.sin_port = htons(port);

    //connect via socket
    if (connect(socket_id, &serv_addr, sizeof(serv_addr)) < 0)
    {
        opal_output(0, "ERROR connecting.\n");
        return -1;
    }

    return 0;
}

struct pbc_rmessage* orte_hdclient_recv_message(const char* msg_type) {
    int rc;
    struct pbc_slice slice;

    int8_t success;
    rc = read_all(orte_umbilical_socket_id, &success, 1);
    if (0 != rc) {
        opal_output(0, "read success status from socket failed");
        return NULL;
    }
    if (success == 2) {
        opal_output(0, "recved error response from AM");
        return NULL;
    } else if (success != 1) {
        opal_output(0, "recved unknown response from AM");
        return NULL;
    }

    // read len from socket
    rc = read_all(orte_umbilical_socket_id, &slice.len, sizeof(int));
    if (0 != rc) {
        opal_output(0, "read length of umbilical socket failed.");
        return NULL;
    }
    slice.len = int_endian_swap(slice.len);

    // create buffer and read buffer
    slice.buffer = (char*)malloc(slice.len);
    if (!slice.buffer) {
        opal_output(0, "failed to allocate memory for recv message");
        return NULL;
    }
    rc = read_all(orte_umbilical_socket_id, slice.buffer, slice.len);
    if (rc != 0) {
        opal_output(0, "failed to read response message from AM");
        free(slice.buffer);
        return NULL;
    }

    // deserialize rmessage
    struct pbc_rmessage* msg = pbc_rmessage_new(orte_hdclient_pb_env, msg_type, &slice);
    if (!msg) {
        opal_output(0, "error when parse rmessage, %s", pbc_error(orte_hdclient_pb_env));
        free(slice.buffer);
        return NULL;
    }

    return msg;
}

// read file to a buffer, will be used for load pb image
static void read_file(const char *filename , struct pbc_slice* slice) {
    FILE *f = fopen(filename, "rb");
    if (f == NULL) {
        slice->buffer = NULL;
        slice->len = 0;
        return;
    }
    fseek(f,0,SEEK_END);
    slice->len = ftell(f);
    fseek(f,0,SEEK_SET);
    slice->buffer = malloc(slice->len);
    fread(slice->buffer, 1 , slice->len , f);
    fclose(f);
}
