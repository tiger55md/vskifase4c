/*
 * Daniel Levandovschi fc54412
 * Rui Diogo fc54397
 * Afonso Veiga fc54398
 * Grupo 13
 * 
 */


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include "../include/client_stub.h"
#include "../include/client_stub-private.h"
#include "../include/stats-private.h"
#include <zookeeper/zookeeper.h>

struct rtable_t *rtable;
zhandle_t *zh;
static int is_connected; 
static char *root_path = "/kvstore";

typedef struct String_vector zoo_string;
static char *watcher_ctx = "ZooKeeper Data Watcher";
static void child_watcherStub(zhandle_t *wzh, int type, int state, const char *zpath, void *watcher_ctx);

void connection_watcher(zhandle_t *zzh, int type, int state, const char *path, void* context) {
	if (type == ZOO_SESSION_EVENT) {
		if (state == ZOO_CONNECTED_STATE) {
			is_connected = 1; 
		} else {
			is_connected = 0; 
		}
	}
}


void closeConn(){
    rtable_disconnect(rtable);
    exit(0);
}

void putRemote(char *key, char *value);
void getRemote(char *key);
void delRemote(char *key);

void putRemote(char *key, char *value){
    struct data_t *data = data_create2(strlen(value) + 1, strdup(value));
    struct entry_t *entry = entry_create(strdup(key), data);
    int result = rtable_put(rtable, entry);
    if(result < 0) printf("Erro no put\n");
    else printf("Put bem feito\n");
}


void getRemote(char *key){
    char *keyDup = strdup(key);
    struct data_t *data = rtable_get(rtable, keyDup);
    if(data == NULL) printf("A key nao existe\n");
    else{
        printf("O valor da key é: %s\n", (char*) data -> data);
        data_destroy(data);
    }
    free(keyDup);
}

void delRemote(char *key){
    char *keyDup = strdup(key);
    int result = rtable_del(rtable,keyDup);
    if(result < 0) printf("A key nao existe\n");
    else printf("Delete bem feito\n");
    free(keyDup);
}

int main(int argc, char** argv){
    signal(SIGINT,closeConn); 
    signal(SIGPIPE, SIG_IGN);

    if(argc != 2){
        perror("O comando para executar é: ./table-client <server>:<port> ");
        return -1;
    }

    zoo_string* children_list = (zoo_string *) malloc(sizeof(zoo_string));
    char *converter = strdup(argv[1]);
    strtok(converter, ":");
    char *port = strtok(NULL, "\n");
    zh = zookeeper_init(argv[1], connection_watcher, atoi(port), 0, 0, 0);
    if( zh == NULL){
        fprintf(stderr, "Erro a conectar ao zookeeper");
        exit(EXIT_FAILURE);
    }
    char *primaryPortIP = malloc(1024);
    int ipLen = 1024;

    if (ZOK != zoo_wget_children(zh, root_path, &child_watcherStub, watcher_ctx, children_list)) {/*quando apanha sinal, chama o child_watcher()*/
        fprintf(stderr, "Error setting watch at %s!\n", root_path); //compromete transparencia?
    }

    if( ZOK != zoo_get(zh, "/kvstore/primary", 0, primaryPortIP, &ipLen, NULL)){
        fprintf(stderr, "no primary nao existe");
        exit(EXIT_FAILURE);
    }

    rtable = rtable_connect(primaryPortIP);
    if(rtable == NULL){
        perror("Erro a ligar ao server");
        return -1;
    }
    rtable -> primaryIP = primaryPortIP;
    rtable -> zooHandle = zh;

    if(rtable == NULL){
        fprintf(stderr, "Nao é possivel executar ações na tabela");

    }
    while(1){
        char comando[100];
        printf("Que comando deseja executar?: \n");
        if(fgets(comando, 100, stdin) == NULL){
            return -1;
        }

        char *comm = strtok(comando, " ");

        if(strncmp(comm, "\n", strlen("\n")) == 0){
            continue;
        }
        else{
            if(strncmp(comm, "quit", strlen("quit")) == 0){
                rtable_disconnect(rtable);
                return 0;
            }
            else if(strncmp(comm,"put", strlen("put")) == 0){
                char *key = strtok(NULL, " ");
                char *value = strtok(NULL, " ");
                if( value == NULL || key == NULL){
                    perror("Para fazer put o formato é: put (key) (value)");
                    continue;
                }
                putRemote(key, value);
            }
            else if(strncmp(comm, "size", strlen("size")) == 0){
                printf("Tamanho da tabela: %d\n", rtable_size(rtable));
            }
            else if(strncmp(comm, "getkeys", strlen("getkeys")) == 0){
                char** keys = rtable_get_keys(rtable);
                if(keys == NULL){
                    printf("A tabela nao foi criada\n");
                    continue;
                }
                printf("A tabela contem as seguintes keys: \n");
                int i = 0;
                while(keys[i] != NULL){
                    printf("%s\n", keys[i]);
                    i++;
                }
                rtable_free_keys(keys);
            }
            else if(strncmp(comm,"get", strlen("get")) == 0){
                char *key = strtok(NULL, "\n");
                if(key == NULL){
                    perror("E preciso utilizar o comando no seguinte formato: get <key>\n");
                    continue;
                }
                getRemote(key);
            }
            else if(strncmp(comm,"del", strlen("del")) == 0){
                char *key = strtok(NULL, "\n");
                if(key == NULL){
                    perror("E preciso utilizar o comando no seguinte formato: del <key>\n");
                    continue;
                }
                delRemote(key);
            }
            else if(strncmp(comm, "table_print", strlen("table_print")) == 0){
                rtable_print(rtable);
            }
            else if(strncmp(comm, "stats", strlen("stats")) == 0){
                struct statistics *stats = rtable_stats(rtable);
                printf("OP Get: %ld\n", stats -> nGets);
                printf("OP Size: %ld\n", stats -> nSize);
                printf("OP Put: %ld\n", stats -> nPuts);
                printf("OP Del: %ld\n", stats -> nDels);
                printf("OP getkeys: %ld\n", stats -> nGetKeys);
                printf("OP table_print: %ld\n", stats -> nPrints);
                printf("Tempo Medio de Espera em segundos: %lfs\n",  stats -> tempoEspera);
                free(stats);
            }
        }
    }
}


static void child_watcherStub(zhandle_t *wzh, int type, int state, const char *zpath, void *watcher_ctx){
    zoo_string* children_list = (zoo_string *) malloc(sizeof(zoo_string));
    if(state == ZOO_CONNECTED_STATE){
        if(type == ZOO_CHILD_EVENT){
            if (ZOK != zoo_wget_children(zh, root_path, child_watcherStub, watcher_ctx, children_list)) {
                fprintf(stderr, "Error setting watch at %s!\n", root_path); 
            }
            fprintf(stderr, "\n=== znode listing func=== [ %s ]", root_path); 
            for (int i = 0; i < children_list->count; i++)  {
                fprintf(stderr, "\n(%d): %s", i+1, children_list->data[i]);
            }
            fprintf(stderr, "\n=== done ===\n");

            if(children_list -> count == 1){
                if(strcmp(children_list -> data[0], "primary") != 0){
                    rtable -> primaryIP = NULL;
                    rtable -> zooHandle = NULL;
                    rtable_disconnect(rtable);
                    rtable = NULL;
                }
            }
            else{
                char *primaryPortIP = malloc(1024);
                int ipLen = 1024;
                if( ZOK != zoo_get(zh, "/kvstore/primary", 0, primaryPortIP, &ipLen, NULL)){
                    fprintf(stderr, "no primary nao existe");
                    exit(EXIT_FAILURE);
                }
                rtable = rtable_connect(primaryPortIP);
                rtable -> primaryIP = primaryPortIP;
                rtable -> zooHandle = zh;
            }
        }
    }
}