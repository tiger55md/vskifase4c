/*
 * Daniel Levandovschi fc54412
 * Rui Diogo fc54397
 * Afonso Veiga fc54398
 * Grupo 13
 * 
 */
#ifndef _STATS_PRIVATE_H
#define _STATS_PRIVATE_H

#include <time.h>
#include <sys/time.h>
#include "sdmessage.pb-c.h"

struct statistics { 
    long nGets;
    long nSize;
    long nPuts;
    long nDels;
    long nGetKeys;
    long nPrints;
    double tempoEspera;
};

void tempoEspera(struct timespec *start, struct timespec *end, MessageT__Opcode opcode);

#endif