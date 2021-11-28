
/*
 * Daniel Levandovschi fc54412
 * Rui Diogo fc54397
 * Afonso Veiga fc54398
 * Grupo 13
 * 
 */

#include "list.h"
#include "list-private.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

struct list_t; 
void removeLoop(struct list_t *list, struct node_t *iter, struct node_t *aux);
struct node_t *createNode(struct entry_t *entry, struct node_t *next);


/* Função que cria uma nova lista (estrutura list_t a ser definida pelo
 * grupo no ficheiro list-private.h).
 * Em caso de erro, retorna NULL.
 */
struct list_t *list_create(){
    struct list_t *list = malloc(sizeof(struct list_t));
    if( list == NULL) return NULL;
    list -> size = 0;
    list ->  head = NULL;
    list -> tail = NULL;
    return list;
}

/* Função que elimina uma lista, libertando *toda* a memoria utilizada
 * pela lista.
 */
void list_destroy(struct list_t *list){
    if(list == NULL) return;

    if(list -> head == NULL){
        free(list);
        return;
    }
    struct node_t *start = list -> head;
    while(start != NULL){
        struct node_t *iter = start -> next;
        entry_destroy(start -> entry);
        free(start);
        start = iter;
    }

    free(list);
}

/* Função que adiciona no final da lista (tail) a entry passada como
* argumento caso não exista na lista uma entry com key igual àquela
* que queremos inserir.
* Caso exista, os dados da entry (value) já existente na lista serão
* substituídos pelos os da nova entry.
* Retorna 0 (OK) ou -1 (erro).
*/

int list_add(struct list_t *list, struct entry_t *entry){
    if( entry == NULL || list == NULL) return -1;
    struct entry_t *repeat = list_get(list, entry -> key);
    if( repeat != NULL){
        list_remove(list, repeat -> key);
    }
    struct node_t *toAdd = createNode(entry, NULL);
    if(toAdd == NULL) return -1;

    if(list -> head == NULL){
        list -> head = toAdd;
    }
    else{
        list -> tail -> next = toAdd;
    }
    list -> tail = toAdd;
    list -> size++;
    return 0;
}

/* Função que elimina da lista a entry com a chave key.
 * Retorna 0 (OK) ou -1 (erro).
 */
int list_remove(struct list_t *list, char *key){
    if( list == NULL || list_size(list) == 0 ||
        key == NULL || strlen(key) < 1) return -1;

    struct node_t *iter = list -> head;
    struct node_t *aux = NULL;

    while( iter != NULL){
        if(strcmp(iter -> entry -> key, key) == 0){
            list -> size--;
            removeLoop(list, iter, aux);
            entry_destroy(iter ->  entry);
            free(iter);
            return 0;
        }
        aux = iter;
        iter = iter -> next;
    }
    return -1;
}

/* Função que obtém da lista a entry com a chave key.
 * Retorna a referência da entry na lista ou NULL em caso de erro.
 * Obs: as funções list_remove e list_destroy vão libertar a memória
 * ocupada pela entry ou lista, significando que é retornado NULL
 * quando é pretendido o acesso a uma entry inexistente.
*/
struct entry_t *list_get(struct list_t *list, char *key){
    if(list == NULL || list -> head == NULL ||
        key == NULL) return NULL;
    if( strcmp(key, list ->  head -> entry ->  key) == 0){
        return list ->  head -> entry;
    }
    if(strcmp(key, list -> tail -> entry -> key) == 0){
        return list -> tail -> entry;
    }
    struct node_t *temp = list -> head -> next;
    while(temp != NULL){
        if(strcmp(temp -> entry -> key,key) == 0){
            return temp -> entry;
        }
        temp = temp -> next;
    }
    return NULL;

}

/* Função que retorna o tamanho (número de elementos (entries)) da lista,
 * ou -1 (erro).
 */
int list_size(struct list_t *list){
    if( list == NULL || list -> size <  0) return -1;
    return list -> size;
}

/* Função que devolve um array de char* com a cópia de todas as keys da 
 * tabela, colocando o último elemento do array com o valor NULL e
 * reservando toda a memória necessária.
 */
char **list_get_keys(struct list_t *list){
    if( list == NULL) return NULL;
    char **keys = malloc((list_size(list) + 1 ) * sizeof(char*));
    if(keys == NULL){
        perror("Erro ao alocar memoria");
        return NULL;
    }
    struct node_t *node = list -> head;
    for(int i = 0; i < list_size(list); i++){
        keys[i] = strdup(node -> entry -> key);
        node = node -> next;
    }
    keys[list_size(list)] = NULL;
    return keys;
}

/* Função que liberta a memória ocupada pelo array das keys da tabela,
 * obtido pela função list_get_keys.
 */
void list_free_keys(char **keys){
    int i = 0;
    while( keys[i] != NULL){
        free(keys[i]);
        i++;
    }
    free(keys);
}

/* Função que imprime o conteúdo da lista para o terminal.
 */
void list_print(struct list_t *list){
    struct node_t *node = list -> head;
    while(node != NULL){
        if(node -> next == NULL){
            printf("Key: %s, Value: %s", node -> entry -> key, (char*) node -> entry -> value -> data);
        }
        else{
            printf("Key: %s, Value: %s", node -> entry -> key, (char*) node -> entry -> value -> data);
        }
        node = node -> next;
    }
    
}


void removeLoop(struct list_t *list, struct node_t *iter, struct node_t *aux){
    if(iter == list -> head){
        list -> head = list -> head -> next;
    }
    if(iter == list -> tail){
        list -> tail = aux;
    }
    if(aux != NULL){
        aux ->  next = iter ->  next;
    }
}


struct node_t *createNode(struct entry_t *entry, struct node_t *next){
    struct node_t *toAdd = malloc(sizeof(struct node_t));
    if(toAdd == NULL) return NULL;

    toAdd -> entry = entry;
    toAdd -> next = next;

    return toAdd;
}

