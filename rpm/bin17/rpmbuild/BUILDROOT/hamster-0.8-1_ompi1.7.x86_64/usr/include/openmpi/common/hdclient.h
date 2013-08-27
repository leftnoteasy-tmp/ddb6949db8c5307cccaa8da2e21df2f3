#ifndef _ORTE_MCA_HD_CLIENT_H
#define _ORTE_MCA_HD_CLIENT_H

#include <stdbool.h>
#include <stdint.h>
#include <pthread.h>

#include "opal/util/output.h"
#include "base/pbc/pbc.h"

extern struct pbc_env* orte_hdclient_pb_env;
extern int orte_umbilical_socket_id; 

typedef enum {
    HAMSTER_MSG_UNKNOWN = 0,
    HAMSTER_MSG_ALLOCATE = 1,
    HAMSTER_MSG_LAUNCH = 2,
    HAMSTER_MSG_REGISTER = 3,
    HAMSTER_MSG_FINISH = 4,
    HAMSTER_MSG_HEARTBEAT = 5,
} enum_hamster_msg_type;

int orte_hdclient_init_pb_env();

int orte_hdclient_connect_to_am();

int orte_hdclient_send_message_and_delete(struct pbc_wmessage* msg, enum_hamster_msg_type type);

struct pbc_rmessage* orte_hdclient_recv_message(const char* msg_type);

#endif
