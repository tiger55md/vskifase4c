/*
 * Daniel Levandovschi fc54412
 * Rui Diogo fc54397
 * Afonso Veiga fc54398
 * Grupo 13
 * 
 */


#include "../include/entry.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* Função que cria uma entry, reservando a memória necessária e
 * inicializando-a com a string e o bloco de dados passados.
 */
struct entry_t *entry_create(char *key, struct data_t *data){
    if(key == NULL || data == NULL) return NULL;
    struct entry_t *entry = malloc(sizeof(struct entry_t));
    entry -> key = key;
    entry -> value = data;
    return entry;
}

/* Função que inicializa os elementos de uma entrada na tabela com o
 * valor NULL.
 */
void entry_initialize(struct entry_t *entry){
    if(entry == NULL) return;
    else{
        entry ->  key = NULL;
        entry -> value = NULL;
    }
}

/* Função que elimina uma entry, libertando a memória por ela ocupada
 */
void entry_destroy(struct entry_t *entry){
    if(entry == NULL) return;
    if(entry -> value != NULL) data_destroy(entry -> value);
    if(entry -> key != NULL){
        free(entry -> key);
        entry ->  key = NULL;
    }
    free(entry);
}

/* Função que duplica uma entry, reservando a memória necessária para a
 * nova estrutura.
 */
struct entry_t *entry_dup(struct entry_t *entry){
    if(entry == NULL || entry -> key == NULL || entry -> value == NULL) return NULL;
    struct entry_t * entry2 = entry_create(strdup(entry -> key), data_dup(entry -> value));
    return entry2;
}

/* Função que substitui o conteúdo de uma entrada entry_t.
*  Deve assegurar que destroi o conteúdo antigo da mesma.
*/
void entry_replace(struct entry_t *entry, char *new_key, struct data_t *new_value){
    if(entry == NULL || new_key == NULL || new_value == NULL) return;
    entry_destroy(entry);
    entry = entry_create(new_key,new_value);
}

/* Função que compara duas entradas e retorna a ordem das mesmas.
*  Ordem das entradas é definida pela ordem das suas chaves.
*  A função devolve 0 se forem iguais, -1 se entry1<entry2, e 1 caso contrário.
*/
int entry_compare(struct entry_t *entry1, struct entry_t *entry2){
    int compare = strcmp(entry1 -> key, entry2 -> key);
    if(compare > 0) return 1;
    else if(compare < 0) return -1;
    else return 0;
}

