/*
 * Daniel Levandovschi fc54412
 * Rui Diogo fc54397
 * Afonso Veiga fc54398
 * Grupo 13
 * 
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "../include/table_skel.h"
#include "../include/table.h"
#include "../include/stats-private.h"
#include "client_stub.h"
#include "entry.h"
#include <pthread.h>
#include <unistd.h>
#include <zookeeper/zookeeper.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h> 
#include <netinet/in.h>
#include <pthread.h>
#include <arpa/inet.h>

#define ZDATALEN 1024 * 1024

typedef struct String_vector zoo_string; 

char *root_path = "/kvstore";
zhandle_t *zh;
int is_connected = 0;
struct table_t *tabela;
struct statistics *stats;
double tempoEsperaTotal;
pthread_mutex_t mutexTable = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t condTable = PTHREAD_COND_INITIALIZER;
pthread_mutex_t mutexStats = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t condStats = PTHREAD_COND_INITIALIZER;
int statsWriterCounter = 1;
int statsReaderCounter = 0;
int tableWriterCounter = 1;
int tableReaderCounter = 0;
static char *watcher_ctx = "ZooKeeper Data Watcher";
char *serverID;


struct rtable_t *backup = NULL;
char *primaryIP;
char *backupIP;



void mutexStatsReadInit();
void mutexStatsReadEnd();
void mutexStatsWriteInit();
void mutexStatsWriteEnd();
void mutexTableReadInit();
void mutexTableReadEnd();
void mutexTableWriteInit();
void mutexTableWriteEnd();
void connection_watcher(zhandle_t *zzh, int type, int state, const char *path, void* context);
static void child_watcher(zhandle_t *wzh, int type, int state, const char *zpath, void *watcher_ctx);


/* Inicia o skeleton da tabela.
 * O main() do servidor deve chamar esta função antes de poder usar a
 * função invoke(). O parâmetro n_lists define o número de listas a
 * serem usadas pela tabela mantida no servidor.
 * Retorna 0 (OK) ou -1 (erro, por exemplo OUT OF MEMORY)
 */
int table_skel_init(int n_lists, char* hostPort, char* listenPort){
    stats =  malloc(sizeof(struct statistics));
    if(tabela == NULL){
        tabela = table_create(n_lists);
    }
    if(tabela == NULL) return -1;
    char *hostPort1 = strdup(hostPort);
    strtok(hostPort, ":");
    char *port = strtok(NULL, "\n");
    zh = zookeeper_init(hostPort1, connection_watcher, atoi(port), 0, 0, 0);
    if( zh == NULL){
        fprintf(stderr, "Erro a conectar ao zookeeper");
        exit(EXIT_FAILURE);
    }
    stats -> nGets = 0;
    stats -> nSize = 0;
    stats -> nPuts = 0;
    stats -> nDels = 0;
    stats -> nGetKeys = 0;
    stats -> nPrints = 0;
    stats -> tempoEspera = 0;
    tempoEsperaTotal = 0;
    sleep(2);
    if (is_connected) {
        zoo_string* children_list =	(zoo_string *) malloc(sizeof(zoo_string));
        if(ZNONODE == zoo_exists(zh, root_path, 0, NULL)){
            if(ZOK != zoo_create(zh, root_path, NULL, 0, &ZOO_OPEN_ACL_UNSAFE,0,NULL,0)){
                fprintf(stderr, "Erro a criar");
                exit(EXIT_FAILURE);
            }
        }
        char node_path[50] = "";
        strcat(node_path, root_path);
        strcat(node_path, "/primary");
        /********************************************/
        int server_ID_len = 1024;
        serverID = malloc(server_ID_len);

        /********************************************/
        char hostBuffer[256];
        char *IPBuffer;
        struct hostent *host_entry;
        int hostname;

        hostname = gethostname(hostBuffer, sizeof(hostBuffer));
        if(hostname == -1){
            perror("gethostname");
            return -1;
        }
        host_entry = gethostbyname(hostBuffer);
        if(host_entry == NULL){
            perror("gethostbyname");
            return -1;
        }

        // To convert an Internet network address into ASCII string 
        IPBuffer = inet_ntoa(*((struct in_addr*) host_entry->h_addr_list[0])); 

        /*---------------concatenar ip com port do servidor---------------*/
        char *ip_port = IPBuffer; 
        strcat(ip_port, ":");
        strcat(ip_port, listenPort);
        printf("IP do Primario: %s\n", ip_port);
        /*----------------------------------------------------------------*/
        if(ZOK != zoo_create(zh, node_path, ip_port, strlen(ip_port), &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, serverID, server_ID_len)){
            fprintf(stderr, "Error creating znode from path %s!\n", node_path);
            return -1;
        }
        fprintf(stderr, "Ephemeral ZNode created! ZNode path: %s\n", serverID);
        sleep(5);


        if( ZOK != zoo_get_children(zh, root_path, 0, children_list)){
            fprintf(stderr, "Error getting root child\n");
        }

        if( children_list -> count == 0){
            char node_path[50] = "";
            strcat(node_path, root_path);
            strcat(node_path, "/primary");
            /********************************************/
            int server_ID_len = 1024;
            serverID = malloc(server_ID_len);

            /********************************************/
            char hostBuffer[256];
            char *IPBuffer;
            struct hostent *host_entry;
            int hostname;

            hostname = gethostname(hostBuffer, sizeof(hostBuffer));
            if(hostname == -1){
                perror("gethostname");
                return -1;
            }
            host_entry = gethostbyname(hostBuffer);
            if(host_entry == NULL){
                perror("gethostbyname");
                return -1;
            }

            // To convert an Internet network address into ASCII string 
            IPBuffer = inet_ntoa(*((struct in_addr*) host_entry->h_addr_list[0])); 

            /*---------------concatenar ip com port do servidor---------------*/
            char *ip_port = IPBuffer; 
            strcat(ip_port, ":");
            strcat(ip_port, listenPort);
            primaryIP = ip_port;
            printf("IP do Primario: %s\n", ip_port);
            /*----------------------------------------------------------------*/
            if(ZOK != zoo_create(zh, node_path, ip_port, strlen(ip_port), &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, serverID, server_ID_len)){
                fprintf(stderr, "Error creating znode from path %s!\n", node_path);
                return -1;
            }
            fprintf(stderr, "Ephemeral ZNode created! ZNode path: %s\n", serverID);
            sleep(5);

        }
        else if(children_list -> count == 1){
            if(ZNONODE == zoo_exists(zh, "/kvstore/backup", 0, NULL)){
            char node_path[50] = "";
            strcat(node_path, root_path);
            strcat(node_path, "/backup");
            /********************************************/
            int server_ID_len = 1024;
            serverID = malloc(server_ID_len);

            /********************************************/
            char hostBuffer[256];
            char *IPBuffer;
            struct hostent *host_entry;
            int hostname;

            hostname = gethostname(hostBuffer, sizeof(hostBuffer));
            if(hostname == -1){
                perror("gethostname");
                return -1;
            }
            host_entry = gethostbyname(hostBuffer);
            if(host_entry == NULL){
                perror("gethostbyname");
                return -1;
            }

            // To convert an Internet network address into ASCII string 
            IPBuffer = inet_ntoa(*((struct in_addr*) host_entry->h_addr_list[0])); 

            /*---------------concatenar ip com port do servidor---------------*/
            char *ip_port = IPBuffer;
            strcat(ip_port, ":");
            strcat(ip_port, listenPort);
            /*----------------------------------------------------------------*/
            if(ZOK != zoo_create(zh, node_path, ip_port, strlen(ip_port), &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, serverID, server_ID_len)){
                fprintf(stderr, "Error creating znode from path %s!\n", node_path);
                return -1;
            }
            fprintf(stderr, "Ephemeral ZNode created! ZNode path: %s\n", serverID);
            backupIP = ip_port;
            printf("IP do Backup: %s\n", ip_port);
            //backup = rtable_connect(ip_port);
            printf("Backup conectou");
            sleep(5);

            }
        }
        if (ZOK != zoo_wget_children(zh, root_path, &child_watcher, watcher_ctx, children_list)) {/*quando apanha sinal, chama o child_watcher()*/
			fprintf(stderr, "Error setting watch at %s!\n", root_path); //compromete transparencia?
		}

        for(int i = 0; i < children_list -> count; i++){
            if(strcmp(children_list -> data[i], "primary") == 0){
                backupIP = NULL;
            }
            else{
                int primaryIPLen = 256;
                char *primIP = malloc(primaryIPLen);
                if(ZOK != zoo_get(zh, "/kvstore/primary", 0, primIP, &primaryIPLen, NULL)){
                    printf("ERRO A IR BUSCAR DATA BACKUP 2 FILHOS\n");
                    exit(EXIT_FAILURE);
                }
                primaryIP = primIP;

            }
        }
        free(children_list->data);
        free(children_list);
    }

    printf("Zookeeper setup finished\n");
    return 0;
}

/* Liberta toda a memória e recursos alocados pela função table_skel_init.
 */
void table_skel_destroy(){
    free(stats);
    table_destroy(tabela);
    zookeeper_close(zh);
}

/* Executa uma operação na tabela (indicada pelo opcode contido em msg)
 * e utiliza a mesma estrutura message_t para devolver o resultado.
 * Retorna 0 (OK) ou -1 (erro, por exemplo, tabela nao incializada)
*/
int invoke(MessageT *msg){
    if(msg == NULL || tabela == NULL) return -1;
    if(msg  -> opcode == MESSAGE_T__OPCODE__OP_SIZE ){
        if(msg ->   c_type == MESSAGE_T__C_TYPE__CT_NONE){
            mutexStatsWriteInit();
            stats -> nSize++;
            mutexStatsWriteEnd();
            mutexTableReadInit();
            msg ->   table_size = table_size(tabela);
            mutexTableReadEnd();
            msg ->   opcode = MESSAGE_T__OPCODE__OP_SIZE+1;
            msg ->   c_type = MESSAGE_T__C_TYPE__CT_RESULT;
            return 0;
        }
    }
    if(msg ->   opcode == MESSAGE_T__OPCODE__OP_DEL){
        if(msg ->    c_type == MESSAGE_T__C_TYPE__CT_KEY){
            //if(backup == NULL){
            //    msg ->    opcode = MESSAGE_T__OPCODE__OP_ERROR;
            //    msg ->   c_type = MESSAGE_T__C_TYPE__CT_NONE;
            //    return -1;
            //}

            mutexStatsWriteInit();
            stats -> nDels++;
            mutexStatsWriteEnd();
            mutexTableWriteInit();
            int verify = table_del(tabela, msg ->   key);
            mutexTableWriteEnd();
            if(verify == -1){
                msg ->   opcode = MESSAGE_T__OPCODE__OP_ERROR;
                msg ->   c_type = MESSAGE_T__C_TYPE__CT_NONE;
                return -1;
            }
            msg ->   opcode = MESSAGE_T__OPCODE__OP_DEL+1;
            msg ->   c_type = MESSAGE_T__C_TYPE__CT_NONE;
            return 0;
        }
    }
    if(msg ->   opcode == MESSAGE_T__OPCODE__OP_GET){
        if(msg ->    c_type == MESSAGE_T__C_TYPE__CT_KEY){
            mutexStatsWriteInit();
            stats -> nGets++;
            mutexStatsWriteEnd();
            if(msg ->   key == NULL){
                msg ->    opcode = MESSAGE_T__OPCODE__OP_ERROR;
                msg ->   c_type = MESSAGE_T__C_TYPE__CT_NONE;
                return -1;
            }
            mutexTableReadInit();
            struct data_t *data = table_get(tabela, msg ->   key);
            mutexTableReadEnd();
            ProtobufCBinaryData binData;
            if(data == NULL){
                binData.data = malloc(sizeof(NULL));
                binData.data = NULL;
                binData.len = 0;
                msg ->    opcode = MESSAGE_T__OPCODE__OP_GET + 1;
                msg ->    c_type = MESSAGE_T__C_TYPE__CT_VALUE;
                msg -> data = binData;
                data_destroy(data);
                return 0;
            }
            binData.data = malloc( data -> datasize);
            memcpy( binData.data, data -> data, data -> datasize);
            binData.len= data -> datasize;
            msg -> data = binData;
            msg ->    opcode = MESSAGE_T__OPCODE__OP_GET + 1;
            msg ->    c_type = MESSAGE_T__C_TYPE__CT_VALUE;
            data_destroy(data);
            return 0;
        }
    }
    if(msg ->   opcode == MESSAGE_T__OPCODE__OP_PUT){
        if(msg ->    c_type == MESSAGE_T__C_TYPE__CT_ENTRY){
            //if(backup == NULL){
            //    msg ->    opcode = MESSAGE_T__OPCODE__OP_ERROR;
            //    msg ->   c_type = MESSAGE_T__C_TYPE__CT_NONE;
            //    return -1;
            //}
            ProtobufCBinaryData binData = msg -> data;
            mutexStatsWriteInit();
            stats -> nPuts++;
            mutexStatsWriteEnd();
            if(binData.data == NULL){
                msg ->    opcode = MESSAGE_T__OPCODE__OP_ERROR;
                msg ->   c_type = MESSAGE_T__C_TYPE__CT_NONE;
                return -1;
            }
            struct data_t *data = data_create2( binData.len, binData.data);
            struct entry_t *entry = entry_create(msg ->  key, data);
            mutexTableWriteInit();
            int result = table_put(tabela, msg ->   key, data);
            mutexTableWriteEnd();
            if( result == -1){
                msg ->    opcode = MESSAGE_T__OPCODE__OP_ERROR;
                msg ->   c_type = MESSAGE_T__C_TYPE__CT_NONE;
                free(data);
                return -1;
            }
            msg ->    opcode = MESSAGE_T__OPCODE__OP_PUT + 1;
            msg ->   c_type = MESSAGE_T__C_TYPE__CT_NONE;
            free(data);
            return 0;
        }
    }
    if(msg ->   opcode == MESSAGE_T__OPCODE__OP_GETKEYS){
        if(msg ->    c_type == MESSAGE_T__C_TYPE__CT_NONE){
            mutexStatsWriteInit();
            stats -> nGetKeys++;
            mutexStatsWriteEnd();
            mutexTableReadInit();
            msg ->   n_keys = table_size(tabela);
            msg -> keys = table_get_keys(tabela);
            mutexTableReadEnd();
            msg ->   opcode = MESSAGE_T__OPCODE__OP_GETKEYS + 1;
            msg ->   c_type = MESSAGE_T__C_TYPE__CT_KEYS;
        }
        return 0;
    }
    if(msg -> opcode == MESSAGE_T__OPCODE__OP_PRINT){
        if(msg -> c_type == MESSAGE_T__C_TYPE__CT_NONE ){
            mutexStatsWriteInit();
            stats -> nPrints++;
            mutexStatsWriteEnd();
            mutexTableReadInit();
            msg -> n_keys = table_size(tabela);
            char **keysToPrint = table_get_keys(tabela);
            mutexTableReadEnd();
            msg -> opcode = MESSAGE_T__OPCODE__OP_PRINT + 1;
            msg -> c_type = MESSAGE_T__C_TYPE__CT_TABLE;
            msg -> keys = malloc((msg -> n_keys) * sizeof(char *));
            for(int i = 0; i < msg -> n_keys; i++){
                mutexTableReadInit();
                struct data_t *dataCpy = table_get(tabela, keysToPrint[i]);
                mutexTableReadEnd();
                int size = strlen(keysToPrint[i]) + strlen(dataCpy -> data) + 30;
                char *toCopy = malloc(size);
                snprintf(toCopy, size, "Key: %s, Data: %s\n", keysToPrint[i], (char *) dataCpy -> data );
                msg -> keys[i] = strdup(toCopy);
                data_destroy(dataCpy);
                free(toCopy);
            }
            table_free_keys(keysToPrint);
        }
        return 0;
    }
    if( msg -> opcode == MESSAGE_T__OPCODE__OP_STATS){
        if(msg -> c_type == MESSAGE_T__C_TYPE__CT_NONE){
            if( stats == NULL){
                msg -> opcode = MESSAGE_T__OPCODE__OP_ERROR;
                msg -> c_type = MESSAGE_T__C_TYPE__CT_NONE;
                return -1;
            }
            msg -> opcode = MESSAGE_T__OPCODE__OP_STATS + 1;
            msg -> c_type = MESSAGE_T__C_TYPE__CT_RESULT;
            mutexStatsReadInit();
            msg -> gets = stats -> nGets;
            msg -> sizes = stats -> nSize;
            msg -> puts = stats -> nPuts;
            msg -> dels = stats -> nDels;
            msg -> getkeys = stats -> nGetKeys;
            msg -> prints = stats -> nPrints;
            msg -> espera = stats -> tempoEspera;
            mutexStatsReadEnd();
            return 0;
        }
    }
    if(msg -> opcode == MESSAGE_T__OPCODE__OP_ERROR){
        if(msg -> c_type == MESSAGE_T__C_TYPE__CT_NONE){
            msg -> key = strdup("A key nao existe\n");
        }
    }
    return -1;
}


void tempoEspera(struct timespec *start, struct timespec *end, MessageT__Opcode opcode){
    if( opcode == MESSAGE_T__OPCODE__OP_STATS) return;
    mutexStatsReadInit();
    int nTotalActions = stats -> nGets + stats -> nSize + stats -> nPuts + stats -> nDels + stats -> nGetKeys + stats -> nPrints;
    mutexStatsReadEnd();
    double espera = ((end->tv_sec - start->tv_sec) + 1e-9*(end->tv_nsec - start->tv_nsec));
    mutexStatsWriteInit();
    tempoEsperaTotal += espera;
    stats -> tempoEspera = (double) (tempoEsperaTotal / nTotalActions);
    mutexStatsWriteEnd();
}






void mutexStatsReadInit(){
    pthread_mutex_lock(&mutexStats);
    while(statsWriterCounter == 0){
        pthread_cond_wait(&condStats, &mutexStats);
    }
    statsReaderCounter++;
    pthread_mutex_unlock(&mutexStats);
}

void mutexStatsReadEnd(){
    pthread_mutex_lock(&mutexStats);
    statsReaderCounter--;
    if(statsReaderCounter == 0) pthread_cond_broadcast(&condStats);
    pthread_mutex_unlock(&mutexStats);
}

void mutexStatsWriteInit(){
    pthread_mutex_lock(&mutexStats);
    while(statsReaderCounter > 0 || statsWriterCounter == 0 ){
        pthread_cond_wait(&condStats, &mutexStats);
    }
    statsWriterCounter--;
    pthread_mutex_unlock(&mutexStats);
}

void mutexStatsWriteEnd(){
    pthread_mutex_lock(&mutexStats);
    if(statsWriterCounter == 0){
        statsWriterCounter++;
        pthread_cond_broadcast(&condStats);
    }
    pthread_mutex_unlock(&mutexStats);
}

void mutexTableReadInit(){
    pthread_mutex_lock(&mutexTable);
    while(tableWriterCounter == 0){
        pthread_cond_wait(&condTable, &mutexTable);
    }
    tableReaderCounter++;
    pthread_mutex_unlock(&mutexTable);
}

void mutexTableReadEnd(){
    pthread_mutex_lock(&mutexTable);
    tableReaderCounter--;
    if(tableReaderCounter == 0) pthread_cond_broadcast(&condTable);
    pthread_mutex_unlock(&mutexTable);
}

void mutexTableWriteInit(){
    pthread_mutex_lock(&mutexTable);
    while(tableReaderCounter > 0 || tableWriterCounter == 0){
        pthread_cond_wait(&condTable, &mutexTable);
    }
    tableWriterCounter--;
    pthread_mutex_unlock(&mutexTable);

}


void mutexTableWriteEnd(){
    pthread_mutex_lock(&mutexTable);
    if(tableWriterCounter == 0){
        tableWriterCounter++;
        pthread_cond_broadcast(&condTable);
    }
    pthread_mutex_unlock(&mutexTable);

}


void connection_watcher(zhandle_t *zzh, int type, int state, const char *path, void* context) {
	if (type == ZOO_SESSION_EVENT) {
		if (state == ZOO_CONNECTED_STATE) {
			is_connected = 1; 
		} else {
			is_connected = 0; 
		}
	}
}

static void child_watcher(zhandle_t *wzh, int type, int state, const char *zpath, void *watcher_ctx){
    zoo_string* children_list = (zoo_string *) malloc(sizeof(zoo_string));
    if(state == ZOO_CONNECTED_STATE){
        if(type == ZOO_CHILD_EVENT){
            if (ZOK != zoo_wget_children(zh, root_path, child_watcher, watcher_ctx, children_list)) {
                fprintf(stderr, "Error setting watch at %s!\n", root_path); 
            }
            fprintf(stderr, "\n=== znode listing func=== [ %s ]", root_path); 
            for (int i = 0; i < children_list->count; i++)  {
                fprintf(stderr, "\n(%d): %s", i+1, children_list->data[i]);
            }
            fprintf(stderr, "\n=== done ===\n");

            if(children_list ->  count == 1){
                if(strcmp(children_list -> data[0], "primary") == 0){
                    if(backup != NULL){
                        rtable_disconnect(backup);
                    }
                    backup = NULL;
                    backupIP = NULL;
                }
                else{
                    int backupIPLen = 256;
                    char backIP[256] = "";
                    if(ZOK != zoo_get(zh, "/kvstore/backup", 0, backIP, &backupIPLen, NULL)){
                        printf("ERRO A IR BUSCAR DATA BACKUP 1 FILHO\n");
                        exit(-1);
                    }
                    primaryIP = NULL;
                    if(ZOK != zoo_delete(zh, "/kvstore/backup", -1)){
                        printf("DELETE DO BACKUP\n");
                        exit(-1);
                    }
                    char *node_path = "/kvstore/primary";
                    if(ZOK != zoo_create(zh, node_path, backIP, strlen(backIP), &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, NULL, 0)){
                        fprintf(stderr, "Error creating znode from path %s!\n", node_path);
                        exit(-1);
                    }
                    serverID = "/kvstore/primary"; 
                }
            }
            else{
                if(strcmp(children_list -> data[0], "backup") == 0){
                    int backIPLen = 256;
                    char *backIP = malloc(backIPLen);
                    if( ZOK != zoo_get(zh, "/kvstore/backup", 0, backIP, &backIPLen, NULL)){
                        fprintf(stderr, "Erro a ver ip do server backup\n");
                        exit(EXIT_FAILURE);
                    }
                    backupIP = backIP;
                    backup = rtable_connect(backupIP);
                    char** tableKeys = table_get_keys(tabela);
                    int i = 0;
                    while(tableKeys[i] != NULL){
                        struct data_t *data = table_get(tabela,tableKeys[i]);
                        struct entry_t *entry = entry_create(tableKeys[i], data);
                        if(rtable_put(backup, entry)){
                            fprintf(stderr,"Erro a por na tabela\n");
                        }
                        i++;
                    }
                    table_free_keys(tableKeys);

                }

            }
        }
    }
    free(children_list->data);
    free(children_list);

}

