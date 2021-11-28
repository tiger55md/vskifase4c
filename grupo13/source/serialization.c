/*
 * Daniel Levandovschi fc54412
 * Rui Diogo fc54397
 * Afonso Veiga fc54398
 * Grupo 13
 * 
 */


#include "serialization.h"
#include "serializaton-private.h"
#include "table.h"
#include "data.h"
#include "list.h"
#include "entry.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Serializa uma estrutura data num buffer que será alocado
 * dentro da função. Além disso, retorna o tamanho do buffer
 * alocado ou -1 em caso de erro.
 */
int data_to_buffer(struct data_t *data, char **data_buf){
    if(data == NULL || data_buf == NULL) return -1;
    char* buff2 = malloc(sizeof(int) + data ->  datasize);
    memcpy(buff2, &(data -> datasize), sizeof(int));
    memcpy(buff2 + sizeof(int), data -> data, data -> datasize);
    *data_buf = buff2;
    return sizeof(int) + data -> datasize;
}

/* De-serializa a mensagem contida em data_buf, com tamanho
 * data_buf_size, colocando-a e retornando-a numa struct
 * data_t, cujo espaco em memoria deve ser reservado.
 * Devolve NULL em caso de erro.
 */
struct data_t *buffer_to_data(char *data_buf, int data_buf_size){
    if(data_buf == NULL || data_buf_size < 1) return NULL;
    struct data_t *buffData = data_create(data_buf_size - sizeof(int));
    memcpy(buffData -> data, data_buf + sizeof(int), buffData -> datasize);
    return buffData;

}

/* Serializa uma estrutura entry num buffer que sera alocado
 * dentro da função. Além disso, retorna o tamanho deste
 * buffer alocado ou -1 em caso de erro.
 */
int entry_to_buffer(struct entry_t *data, char **entry_buf){
    if( data == NULL || entry_buf == NULL) return -1;
    *entry_buf = entry_to_bufferAux(data);
    return sizeof(int)*2 + strlen(data -> key) + 1  + data -> value -> datasize;
}

/* De-serializa a mensagem contida em entry_buf, com tamanho
 * entry_buf_size, colocando-a e retornando-a numa struct
 * entry_t, cujo espaco em memoria deve ser reservado.
 * Devolve NULL em caso de erro.
 */
struct entry_t *buffer_to_entry(char *entry_buf, int entry_buf_size){
    if(entry_buf == NULL || entry_buf_size < 1) return NULL;
    int size;
    memcpy(&size, entry_buf, sizeof(int));
    char* chave = malloc(size);
    memcpy(chave, entry_buf + sizeof(int), size);
    struct data_t *data = buffer_to_data(entry_buf + size + sizeof(int), entry_buf_size - size - sizeof(int));
    return entry_create(chave,data);
}

char *entry_to_bufferAux(struct entry_t *data){
    char* buff2 = malloc(sizeof(int)*2 + strlen(data -> key) + 1  + data -> value -> datasize);
    int deslocamento = 0;
    int size = strlen(data -> key) +1 ;
    memcpy(buff2, &size, sizeof(int));
    deslocamento += sizeof(int);
    memcpy(buff2 + deslocamento, data -> key, strlen(data -> key) + 1);
    deslocamento += strlen(data-> key) + 1;
    memcpy(buff2 + deslocamento, &(data -> value -> datasize), sizeof(int));
    deslocamento += sizeof(int);
    memcpy(buff2 + deslocamento, data -> value -> data, data -> value -> datasize);
    return buff2;
}