/*
 * Daniel Levandovschi fc54412
 * Rui Diogo fc54397
 * Afonso Veiga fc54398
 * Grupo 13
 * 
 */


#include "client_stub.h"
#include "client_stub-private.h"
#include "stats-private.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include "network_client.h"
#include "network_server.h"
#include "sdmessage.pb-c.h"
#include <unistd.h>
#include <pthread.h>
#include <zookeeper/zookeeper.h>


void connection_watcherStub(zhandle_t *zzh, int type, int state, const char *path, void* context);
int is_connected;



/* Remote table, que deve conter as informações necessárias para estabelecer a comunicação com o servidor. A definir pelo grupo em client_stub-private.h
 */
zhandle_t *zh;
struct rtable_t;
struct sockaddr_in *server;
void getSock(short port);
int connectSetup(const char *address_port, struct rtable_t *rtable);
/* Função para estabelecer uma associação entre o cliente e o servidor, 
 * em que address_port é uma string no formato <hostname>:<port>.
 * Retorna NULL em caso de erro.
 */
struct rtable_t *rtable_connect(const char *address_port){
    if(address_port == NULL || strlen(address_port) < 1) return NULL;

    struct rtable_t *rtable = malloc(sizeof(struct rtable_t));

    if(rtable== NULL) return NULL;
    char *separator = strdup(address_port);
    strtok(separator, ":");
    char *port = strtok(NULL, "\n");

    zh = zookeeper_init(address_port, connection_watcherStub, atoi(port), 0, 0, 0);
    if( zh == NULL){
        fprintf(stderr, "Erro a conectar ao zookeeper");
        exit(EXIT_FAILURE);
    }
    sleep(2);
    free(separator);

    char *primaryPortIP = malloc(1024);
    int ipLen = 1024;

    if( ZOK != zoo_get(zh, "/kvstore/primary", 0, primaryPortIP, &ipLen, NULL)){
        fprintf(stderr, "no primary nao existe");
        exit(EXIT_FAILURE);
    }

    const char *primaryIP  = strdup(primaryPortIP);
    if( connectSetup(primaryIP, rtable) == -1 ) return NULL;

    if(network_connect(rtable) < 0){
        free(rtable);
        free(server);
        return NULL;
    }
    
    free(primaryPortIP);
    return rtable;
}

/* Termina a associação entre o cliente e o servidor, fechando a 
 * ligação com o servidor e libertando toda a memória local.
 * Retorna 0 se tudo correr bem e -1 em caso de erro.
 */
int rtable_disconnect(struct rtable_t *rtable){
    if(network_close(rtable) < 0) return -1;
    free(rtable-> sock);
    free(rtable);
    return 0;

}

/* Função para adicionar um elemento na tabela.
 * Se a key já existe, vai substituir essa entrada pelos novos dados.
 * Devolve 0 (ok, em adição/substituição) ou -1 (problemas).
 */
int rtable_put(struct rtable_t *rtable, struct entry_t *entry){
    struct MessageT msg;
    message_t__init(&msg);
    msg.opcode = MESSAGE_T__OPCODE__OP_PUT; 
    msg.c_type = MESSAGE_T__C_TYPE__CT_ENTRY; 
    msg.data.len = entry -> value -> datasize;
    msg.data.data = malloc(sizeof(msg.data.len));
    memcpy(msg.data.data , entry -> value -> data, msg.data.len);
    msg.key = strdup(entry -> key);
    struct MessageT *answer = network_send_receive(rtable, &msg);
    free(msg.key);
    free(msg.data.data);
    entry_destroy(entry);
    if(answer == NULL){
        return -1;
    }

    if(answer -> opcode == MESSAGE_T__OPCODE__OP_PUT + 1){
        message_t__free_unpacked(answer, NULL);
        return 0;
    }
    else{
        message_t__free_unpacked(answer, NULL);
        return -1;
    }
    return 0;
}

/* Função para obter um elemento da tabela.
 * Em caso de erro, devolve NULL.
 */
struct data_t *rtable_get(struct rtable_t *rtable, char *key){
    struct MessageT msg;
    message_t__init(&msg);
    msg.opcode = MESSAGE_T__OPCODE__OP_GET; 
    msg.c_type = MESSAGE_T__C_TYPE__CT_KEY;
    msg.key = key;
    struct MessageT *answer = network_send_receive(rtable, &msg);
    if(answer  == NULL) {
        return NULL;
    }
    struct data_t *res = NULL;
    if(answer  -> opcode == MESSAGE_T__OPCODE__OP_GET+1 &&
        answer  -> c_type == MESSAGE_T__C_TYPE__CT_VALUE){
        ProtobufCBinaryData binData = answer -> data;
        int data_size = binData.len;
        char *data = malloc(data_size);
        memcpy(data, binData.data, data_size);

        if( data == NULL || data_size < 1){
            res = data_create2(data_size, NULL); 
            free(data);
        }
        else{
            res = data_create2(data_size, data); 
        }
    }
    else{
        res = data_create2(0,NULL);
    }

    message_t__free_unpacked(answer, NULL);
    return res;
}

/* Função para remover um elemento da tabela. Vai libertar 
 * toda a memoria alocada na respetiva operação rtable_put().
 * Devolve: 0 (ok), -1 (key not found ou problemas).
 */
int rtable_del(struct rtable_t *rtable, char *key){
    struct MessageT msg;
    message_t__init(&msg);
    msg.opcode = MESSAGE_T__OPCODE__OP_DEL; 
    msg.c_type = MESSAGE_T__C_TYPE__CT_KEY; 
    msg.key = key; 
    struct MessageT *answer = network_send_receive(rtable, &msg);

    if(answer == NULL){
        return -1;
    }

    if (answer -> opcode == MESSAGE_T__OPCODE__OP_DEL+1 &&
        answer  -> c_type == MESSAGE_T__C_TYPE__CT_NONE){
        message_t__free_unpacked(answer, NULL);
        return 0;
    }
    else{
        message_t__free_unpacked(answer, NULL);
        return -1;
    }
}

/* Devolve o número de elementos contidos na tabela.
 */
int rtable_size(struct rtable_t *rtable){
    struct MessageT msg;
    message_t__init(&msg);

    msg.opcode = MESSAGE_T__OPCODE__OP_SIZE; 
    msg.c_type = MESSAGE_T__C_TYPE__CT_NONE; 
    struct MessageT *answer = network_send_receive(rtable, &msg);

    if(answer == NULL){
        return -1;
    }
    int ret = 0;
    if(answer -> opcode == MESSAGE_T__OPCODE__OP_SIZE + 1 &&
        answer   -> c_type == MESSAGE_T__C_TYPE__CT_RESULT){
        ret = answer -> table_size;
        message_t__free_unpacked(answer, NULL);
    }  
    else{
        message_t__free_unpacked(answer, NULL);
        return -1;
    }
    return ret;
}

/* Devolve um array de char* com a cópia de todas as keys da tabela,
 * colocando um último elemento a NULL.
 */
char **rtable_get_keys(struct rtable_t *rtable){
    struct MessageT msg;
    message_t__init(&msg);
    msg.opcode = MESSAGE_T__OPCODE__OP_GETKEYS; 
    msg.c_type = MESSAGE_T__C_TYPE__CT_NONE;

    struct MessageT *answer = network_send_receive(rtable, &msg);
    if( answer == NULL) return NULL;

    if( answer -> opcode == MESSAGE_T__OPCODE__OP_GETKEYS + 1 &&
    answer  -> c_type == MESSAGE_T__C_TYPE__CT_KEYS){
        char **ret = malloc(sizeof(char *) * (answer -> n_keys +1));
        for(int i = 0; i < answer -> n_keys; i++){
            ret[i] = strdup(answer -> keys[i]);
        }
        ret[answer -> n_keys] = NULL;
        message_t__free_unpacked(answer, NULL);
        return ret;
    }

    return NULL;
}

/* Liberta a memória alocada por rtable_get_keys().
 */
void rtable_free_keys(char **keys){
    int i = 0;
    while(keys[i] != NULL){
        free(keys[i]);
        i += 1;
    }
    free(keys);
}

/* Função que imprime o conteúdo da tabela remota para o terminal.
 */
void rtable_print(struct rtable_t *rtable){
    struct MessageT msg;
    message_t__init(&msg);
    msg.opcode = MESSAGE_T__OPCODE__OP_PRINT; 
    msg.c_type = MESSAGE_T__C_TYPE__CT_NONE;
    struct MessageT *answer = network_send_receive(rtable, &msg);
    if( answer == NULL) return;
    if( answer -> opcode == MESSAGE_T__OPCODE__OP_PRINT + 1 &&
        answer  -> c_type == MESSAGE_T__C_TYPE__CT_TABLE){
        
        printf("O estado da tabela é o seguinte:\n");
        for(int i = 0; i < answer ->  n_keys; i++){
            printf("%s", answer -> keys[i]);

        }
        message_t__free_unpacked(answer, NULL);
    } else {
        return;
    }
}

/*Função que retorna a estrutura estatisticas do servidor.
*/
struct statistics *rtable_stats(struct rtable_t *rtable){
    struct MessageT msg;
    message_t__init(&msg);
    msg.opcode = MESSAGE_T__OPCODE__OP_STATS; 
    msg.c_type = MESSAGE_T__C_TYPE__CT_NONE;
    struct statistics *stats = NULL;
    struct MessageT *answer = network_send_receive(rtable, &msg);
    if(answer == NULL) return NULL;
    if(answer -> opcode == MESSAGE_T__OPCODE__OP_STATS + 1 &&
    answer -> c_type == MESSAGE_T__C_TYPE__CT_RESULT){
        stats = malloc(sizeof(struct statistics));
        stats -> nGets = answer -> gets;
        stats -> nPuts = answer -> puts;
        stats -> nDels = answer -> dels;
        stats -> nGetKeys = answer -> getkeys;
        stats -> nPrints = answer -> prints;
        stats -> tempoEspera = answer -> espera;
        stats -> nSize = answer -> sizes;
    }
    message_t__free_unpacked(answer, NULL);
    return stats;
}

void getSock(short port){
    server = malloc(sizeof(struct sockaddr_in));
    if(server == NULL) return;
    server -> sin_family = AF_INET;
    server -> sin_port = port;
}

int connectSetup(const char *address_port, struct rtable_t *rtable){
    char *addressConverter = strdup(address_port);
    char *address = strtok(addressConverter, ":");
    short port = htons(atoi(strtok(NULL, "\n")));
    
    getSock(port);  //mete os dados no sockaddr_in server

    if(server == NULL){
        free(addressConverter);
        return -1;
    }

    if((inet_pton(AF_INET, address, (&server -> sin_addr.s_addr))) < 0){
        free(addressConverter);
        free(rtable);
        free(server);
        return -1;
    }

    rtable -> sock = server;
    free(addressConverter);
    return 0;
}


void connection_watcherStub(zhandle_t *zzh, int type, int state, const char *path, void* context) {
	if (type == ZOO_SESSION_EVENT) {
		if (state == ZOO_CONNECTED_STATE) {
			is_connected = 1; 
		} else {
			is_connected = 0; 
		}
	}
}