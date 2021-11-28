/*
 * Daniel Levandovschi fc54412
 * Rui Diogo fc54397
 * Afonso Veiga fc54398
 * Grupo 13
 * 
 */

#include <stdio.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <signal.h>
#include "../include/network_server.h"
#include "../include/table_skel.h"

int listenSocket;

void closeConn(){
    table_skel_destroy();
    network_server_close(listenSocket);
    exit(0);
}

int main(int argc, char *argv[]){
    signal(SIGINT, closeConn);
    signal(SIGPIPE, SIG_IGN);

    if(argc != 3){
        printf("Como correr: ./table-server <porto> <n_listas>\n");
        printf("Exemplo: ./table-server 42221 4\n");
        return -1;
    }

    int porta = htons(atoi(argv[1]));
    listenSocket = network_server_init(porta);
    if(listenSocket < 0){
        perror("Socket error");
        return -1;
    }

    int nListas = atoi(argv[2]);
    if(table_skel_init(nListas) < 0){
        perror("Error starting table");
        network_server_close(listenSocket);
        return -1;
    }

    if(network_main_loop(listenSocket) < 0){
        table_skel_destroy();
        network_server_close(listenSocket);
        return -1;
    }

    table_skel_destroy();
    network_server_close(listenSocket);
    return 0;
}