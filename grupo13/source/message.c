/*
 * Daniel Levandovschi fc54412
 * Rui Diogo fc54397
 * Afonso Veiga fc54398
 * Grupo 13
 * 
 */

#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "../include/message_private.h"

ssize_t read_all(int fd, void *buf, ssize_t size){
    int tamanho = size;
    int readBytes = 0;

    while(size > 0){
        if( (readBytes = read(fd,buf,size)) <= 0){
            return -1;
        }
        buf += readBytes;
        size -= readBytes;
    }
    return tamanho;

}

ssize_t write_all(int fd, void *buf, ssize_t size){
    int tamanho = size;
    int writeBytes = 0;

    while(size > 0){
        if( (writeBytes = write(fd,buf,size)) <= 0){
            return -1;
        }
        buf += writeBytes;
        size -= writeBytes;
    }
    return tamanho;
}