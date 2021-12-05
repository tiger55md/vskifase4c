/*
 * Daniel Levandovschi fc54412
 * Rui Diogo fc54397
 * Afonso Veiga fc54398
 * Grupo 13
 * 
 */


#include "table_skel.h"
#include "network_server.h"
#include "stats-private.h"
#include "message_private.h"
#include "sdmessage.pb-c.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>

int sockfd;
void *mainLoop(void *clientSocket);

/* Função para preparar uma socket de receção de pedidos de ligação
 * num determinado porto.
 * Retornar descritor do socket (OK) ou -1 (erro).
 */
int network_server_init(short port){
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if( sockfd < 0) return -1;

    int enable = 1;
    if(setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) <0){
        return -1;
    }
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = port;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if(bind(sockfd, (struct sockaddr*) &addr, sizeof(addr)) < 0){
        close(sockfd);
        return -1;
    }
    if(listen(sockfd,0) == -1){
        close(sockfd);
        return -1;
    }
    return sockfd;

}

/* Esta função deve:
 * - Aceitar uma conexão de um cliente;
 * - Receber uma mensagem usando a função network_receive;
 * - Entregar a mensagem de-serializada ao skeleton para ser processada;
 * - Esperar a resposta do skeleton;
 * - Enviar a resposta ao cliente usando a função network_send.
 */
int network_main_loop(int listening_socket){
    if(listening_socket == -1){
        return -1;
    }
    struct sockaddr_in client;
    socklen_t size_client = 0;

    int socketConn;
    MessageT *msg = NULL;

    while( (socketConn = accept(listening_socket, (struct sockaddr *) &client, &size_client) ) != -1){
        pthread_t clientThread;
        if(pthread_create(&clientThread, NULL, &mainLoop, &socketConn) != 0){
            perror("Erro a criar a thread\n");
        }
        if(pthread_detach(clientThread) != 0){
            perror("Erro detach da thread\n");
        }
    }


    if(msg != NULL) message_t__free_unpacked(msg, NULL);
    network_server_close(listening_socket);
    return 0;

}

/* Esta função deve:
 * - Ler os bytes da rede, a partir do client_socket indicado;
 * - De-serializar estes bytes e construir a mensagem com o pedido,
 *   reservando a memória necessária para a estrutura message_t.
 */
MessageT *network_receive(int client_socket){
    int size;

    if((read_all(client_socket, &size, sizeof(int))) < 0){
        return NULL;
    } 

    size = ntohl(size);
    void *buf = malloc(size);

    if((read_all(client_socket, buf, size)) < 0){
        return NULL;
    }

    MessageT *msg = message_t__unpack(NULL, size, buf);
    if(msg == NULL) return NULL;
    free(buf);
    return msg;
}

/* Esta função deve:
 * - Serializar a mensagem de resposta contida em msg;
 * - Libertar a memória ocupada por esta mensagem;
 * - Enviar a mensagem serializada, através do client_socket.
 */
int network_send(int client_socket, MessageT *msg){
    int size = message_t__get_packed_size(msg);
    void *buf = malloc(size);
    if(buf == NULL) return -1;

    message_t__pack(msg, buf);

    size = htonl(size);

    if(write_all(client_socket, &size, sizeof(int)) < 0) return -1;

    size = ntohl(size);

    if(write_all(client_socket, buf, size) < 0) return -1;

    free(buf);

    message_t__free_unpacked(msg, NULL);
    return 0;
}

/* A função network_server_close() liberta os recursos alocados por
 * network_server_init().
 */
int network_server_close(int listening_socket){
    return close(listening_socket);
}

void *mainLoop(void *clientSocket){
    struct timespec start;
    struct timespec end;
    printf("Conexão aceite\n");
    MessageT *msg = NULL;
    int socketConn = *(int *) clientSocket;
    while(1){
        msg = network_receive(socketConn);
        clock_gettime(CLOCK_REALTIME, &start);
        if(msg  == NULL){
            break;
        }
        MessageT__Opcode opcode = msg -> opcode;

        if(invoke(msg) < 0){
            perror("Ocorreu um erro");
        }


        if(network_send(socketConn, msg) < 0){
            perror("Erro ao enviar mensagem");
            break;
        }
        clock_gettime(CLOCK_REALTIME, &end);
        tempoEspera(&start,&end, opcode);
    }
    close(socketConn);
    return NULL;
}

