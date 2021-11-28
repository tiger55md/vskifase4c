/*
 * Daniel Levandovschi fc54412
 * Rui Diogo fc54397
 * Afonso Veiga fc54398
 * Grupo 13
 * 
 */


#include "client_stub.h"
#include "sdmessage.pb-c.h"
#include "message_private.h"
#include "client_stub-private.h"
#include "network_client.h"
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <stdio.h>

MessageT *receiveMessage(int socket);
int sendMessage(int socket, MessageT *msg);
/* Esta função deve:
 * - Obter o endereço do servidor (struct sockaddr_in) a base da
 *   informação guardada na estrutura rtable;
 * - Estabelecer a ligação com o servidor;
 * - Guardar toda a informação necessária (e.g., descritor do socket)
 *   na estrutura rtable;
 * - Retornar 0 (OK) ou -1 (erro).
 */
int network_connect(struct rtable_t *rtable){
    int sockfd;
    if(rtable == NULL){
        perror("A tabela remota é NULL");
        return -1;
    }
    if((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0){
        perror("Erro socket");
        return -1;
    }
    rtable -> sockfd = sockfd;
    
    if((connect(sockfd, (struct sockaddr *)rtable-> sock, sizeof(struct sockaddr))) < 0 ){
        perror("Erro a conectar ao servidor");
        close(sockfd);
        return -1;
    }

    return 0;
}

/* Esta função deve:
 * - Obter o descritor da ligação (socket) da estrutura rtable_t;
 * - Reservar memória para serializar a mensagem contida em msg;
 * - Serializar a mensagem contida em msg;
 * - Enviar a mensagem serializada para o servidor;
 * - Libertar a memória ocupada pela mensagem serializada enviada;
 * - Esperar a resposta do servidor;
 * - Reservar a memória para a mensagem serializada recebida;
 * - De-serializar a mensagem de resposta, reservando a memória 
 *   necessária para a estrutura MessageT que é devolvida;
 * - Libertar a memória ocupada pela mensagem serializada recebida;
 * - Retornar a mensagem de-serializada ou NULL em caso de erro.
 */
MessageT *network_send_receive(struct rtable_t * rtable, MessageT *msg){
    if(rtable == NULL || msg == NULL){
        perror("A tabela ou mensagem estao nulas");
        return NULL;
    }

    int tableSocket = rtable -> sockfd;

    if( sendMessage(tableSocket,msg) < 0){
        return NULL;
    }

    struct MessageT *resposta = receiveMessage(tableSocket);
    if( resposta == NULL) return NULL;
    return resposta;
}

/* A função network_close() fecha a ligação estabelecida por
 * network_connect().
 */
int network_close(struct rtable_t * rtable){
    return close(rtable -> sockfd);
}


// Funcao auxiliar para enviar mensagem
int sendMessage(int socket, MessageT *msg){
    int msgSize = message_t__get_packed_size(msg);
    void *buf = malloc(msgSize);
    if( buf == NULL){
        perror("Erro a alocar memoria");
        return -1;
    }

    message_t__pack(msg, buf);
    msgSize = htonl(msgSize);
    if(write_all(socket, &msgSize, sizeof(int)) < 0 ){
        perror("Erro a escrever no socket");
        free(buf);
        return -1;
    }

    msgSize = ntohl(msgSize);
    if(write_all(socket, buf, msgSize) < 0){
        perror("Erro a escrever no socket");
        free(buf);
        return -1;
    }

    free(buf);
    return 0;
}

MessageT *receiveMessage(int socket){
    struct MessageT *resposta = malloc(sizeof(struct MessageT));
    int msgSize = 0;
    if(read_all(socket, &msgSize, sizeof(int)) < 0){
        perror("Nao leu tamanho da mensagem");
        return NULL;
    }

    msgSize = ntohl(msgSize);
    void *buffer = malloc(msgSize);

    if(read_all(socket, buffer, msgSize) < 0){
        perror("Nao leu os dados");
        free(buffer);
        return NULL;
    }

    resposta = message_t__unpack(NULL, msgSize, buffer);
    free(buffer);
    return resposta;
}