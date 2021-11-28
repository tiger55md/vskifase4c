/*
 * Daniel Levandovschi fc54412
 * Rui Diogo fc54397
 * Afonso Veiga fc54398
 * Grupo 13
 * 
 */


#ifndef _MESSAGE_PRIVATE_H
#define _MESSAGE_PRIVATE_H

#include <unistd.h>

ssize_t read_all(int fd, void *buf, ssize_t count);

ssize_t write_all(int fd, void *buf, ssize_t count);


#endif