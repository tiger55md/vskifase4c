/*
 * Daniel Levandovschi fc54412
 * Rui Diogo fc54397
 * Afonso Veiga fc54398
 * Grupo 13
 * 
 */

#ifndef _LIST_PRIVATE_H
#define _LIST_PRIVATE_H

#include "list.h"

struct node_t {
    struct entry_t *entry;
    struct node_t *next;

};

struct list_t {
    struct node_t *head;
    struct node_t *tail;
    int size;

};


#endif
