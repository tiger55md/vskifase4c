/*
 * Daniel Levandovschi fc54412
 * Rui Diogo fc54397
 * Afonso Veiga fc54398
 * Grupo 13
 * 
 */



#include "../include/data.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>


/* Função que cria um novo elemento de dados data_t e reserva a memória
 * necessária, especificada pelo parâmetro size 
 */
struct data_t *data_create(int size){
    if(size < 1) return NULL;
    struct data_t *new = malloc(sizeof(struct data_t));
    new ->  datasize = size;
    new -> data = malloc(size);
    return new;

}

/* Função idêntica à anterior, mas que inicializa os dados de acordo com
 * o parâmetro data.
 */
struct data_t *data_create2(int size, void *data){
    if(size < 1 || data == NULL) return NULL;
    struct data_t *new = malloc(sizeof(struct data_t));
    new -> data = data;
    new -> datasize = size;
    return new;
}

/* Função que elimina um bloco de dados, apontado pelo parâmetro data,
 * libertando toda a memória por ele ocupada.
 */
void data_destroy(struct data_t *data){
    if(data == NULL || data -> data == NULL) return;
    free(data -> data);
    free(data);
}

/* Função que duplica uma estrutura data_t, reservando a memória
 * necessária para a nova estrutura.
 */
struct data_t *data_dup(struct data_t *data){
  if(data == NULL || data->datasize < 1 || data->data == NULL) return NULL;
   struct data_t* new = data_create(data -> datasize);
   memcpy(new->data,data->data,data->datasize); 
   return new;

}



/* Função que substitui o conteúdo de um elemento de dados data_t.
*  Deve assegurar que destroi o conteúdo antigo do mesmo.
*/
void data_replace(struct data_t *data, int new_size, void *new_data){
    if( data == NULL || new_size < 1 || new_data == NULL) return;
    data_destroy(data);
    struct data_t *new = malloc(sizeof(struct data_t));
    new -> data = new_data;
    new -> datasize = new_size;
    data = new;
}

