
/*
 * Daniel Levandovschi fc54412
 * Rui Diogo fc54397
 * Afonso Veiga fc54398
 * Grupo 13
 * 
 */


#ifndef _TABLE_PRIVATE_H
#define _TABLE_PRIVATE_H

#include "list.h"

struct table_t {
    struct list_t** table;
    int size;
};


int hash_calc(char* key, struct table_t *table); /* funcao privada para calcular o hash*/


#endif
