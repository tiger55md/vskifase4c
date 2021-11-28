/*
 * Daniel Levandovschi fc54412
 * Rui Diogo fc54397
 * Afonso Veiga fc54398
 * Grupo 13
 * 
 */


#include "table.h"
#include "entry.h"
#include "table-private.h"
#include "list.h"
#include "list-private.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct table_t; /* A definir pelo grupo em table-private.h */

/* Função para criar/inicializar uma nova tabela hash, com n
 * linhas (n = módulo da função hash)
 * Em caso de erro retorna NULL.
 */
struct table_t *table_create(int n){
    if( n < 1) return NULL;
    struct table_t *table = malloc(sizeof(struct table_t));
    if(table == NULL) return NULL;
    table -> table = malloc(sizeof(struct list_t*) * n);
    if(table -> table == NULL) return NULL;
    table -> size = n;
    for(int i = 0; i < n; i++){
        table -> table[i] = list_create();
    }
    return table;
}

/* Função para libertar toda a memória ocupada por uma tabela.
 */
void table_destroy(struct table_t *table){
    if( table == NULL ) return;
    for(int i = 0; i < table -> size; i++){
        list_destroy(table -> table[i]);
    }
    free(table -> table);
    free(table);
}

/* Função para adicionar um par chave-valor à tabela.
 * Os dados de entrada desta função deverão ser copiados, ou seja, a
 * função vai *COPIAR* a key (string) e os dados para um novo espaço de
 * memória que tem de ser reservado. Se a key já existir na tabela,
 * a função tem de substituir a entrada existente pela nova, fazendo
 * a necessária gestão da memória para armazenar os novos dados.
 * Retorna 0 (ok) ou -1 em caso de erro.
 */
int table_put(struct table_t *table, char *key, struct data_t *value){
    if( table == NULL || key == NULL || value == NULL) return -1;
    int h = hash_calc(key, table);
    struct entry_t* entry = entry_create(strdup(key), data_dup(value));
    if(entry == NULL) return -1;
    return list_add(table -> table[h], entry);

}

/* Função para obter da tabela o valor associado à chave key.
 * A função deve devolver uma cópia dos dados que terão de ser
 * libertados no contexto da função que chamou table_get, ou seja, a
 * função aloca memória para armazenar uma *CÓPIA* dos dados da tabela,
 * retorna o endereço desta memória com a cópia dos dados, assumindo-se
 * que esta memória será depois libertada pelo programa que chamou
 * a função.
 * Devolve NULL em caso de erro.
 */
struct data_t *table_get(struct table_t *table, char *key){
    if(table == NULL || key == NULL) return NULL;
    int h = hash_calc(key, table);
    struct entry_t *temp = list_get( table -> table[h], key);
    if ( temp == NULL || temp ->  value == NULL) return NULL;
    return data_dup(temp -> value);

}

/* Função para remover um elemento da tabela, indicado pela chave key, 
 * libertando toda a memória alocada na respetiva operação table_put.
 * Retorna 0 (ok) ou -1 (key not found).
 */
int table_del(struct table_t *table, char *key){
    if( table == NULL || key == NULL) return -1;
    int h = hash_calc(key, table);
    if(list_get(table -> table[h], key) == NULL) return -1;
    return list_remove(table -> table[h], key);
}

/* Função que devolve o número de elementos contidos na tabela.
 */
int table_size(struct table_t *table){
    if(table == NULL) return 0;
    int soma = 0;
    for(int i = 0; i < table -> size; i++){
        soma += list_size(table -> table[i]);
    }
    return soma;
}

/* Função que devolve um array de char* com a cópia de todas as keys da
 * tabela, colocando o último elemento do array com o valor NULL e
 * reservando toda a memória necessária.
 */
char **table_get_keys(struct table_t *table){
    if(table == NULL) return NULL;
    int tableSize = table_size(table);
    int tableKeysIndex = 0;

    char **table_keys = malloc((tableSize + 1) * sizeof(char*));
    for(int i = 0; i < table -> size; i++){
        char **listKeys = list_get_keys(table -> table[i]);
        for(int j = 0; j < list_size(table -> table[i]); j++){
            table_keys[tableKeysIndex] = strdup(listKeys[j]);
            tableKeysIndex++;
        }
        list_free_keys(listKeys);
    }
    table_keys[tableSize] = NULL;
    return table_keys;
}

/* Função que liberta toda a memória alocada por table_get_keys().
 */
void table_free_keys(char **keys){
    int i = 0;
    while( keys[i] != NULL){
        free(keys[i]);
        i++;
    }
    free(keys);
}

/* Função que imprime o conteúdo da tabela.
 */
void table_print(struct table_t *table){
    printf("Conteudo da tabela:\n");
    for(int i = 0; i < table -> size; i++){
        list_print(table -> table[i]);
    }
}

/* Serve para calcular a hash */
int hash_calc(char* key, struct table_t *table){
    int sum = 0;
    for(int i = 0; i < strlen(key); i++){
        sum += (int) key[i];
    }
    return sum % ( (table -> size));
}
