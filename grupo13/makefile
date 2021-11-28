PROTOC_DIR=/usr/local/
INC_DIR = include
OBJ_DIR = object
SRC_DIR = source
BIN_DIR = binary
LIB_DIR = lib
CC = gcc
CFLAGS = -g -Wall -lrt -pthread -lpthread -O2 -iquote $(INC_DIR)
LDFLAGS = /usr/local/lib/libprotobuf-c.a
OBJLIB = table.o data.o list.o entry.o serialization.o client_stub.o network_client.o sdmessage.pb-c.o message.o 
OBJCLIENT = table_client.o client-lib.o 
OBJSERVER = table_server.o network_server.o table_skel.o message.o sdmessage.pb-c.o list.o data.o entry.o table.o serialization.o
PROTOC= protoc --c_out=.

run:
	make clean
	make proto
	make table-client
	make table-server

%.o: $(SRC_DIR)/%.c $($@)
	$(CC) $(CFLAGS) -o $(OBJ_DIR)/$@ -c $< -I $(INC_DIR)


table-server: $(OBJSERVER)
	$(CC) $(addprefix $(OBJ_DIR)/, $^) $(CFLAGS) $(LDFLAGS) -o $(BIN_DIR)/$@	

table-client: $(OBJCLIENT) 
	$(CC) $(OBJ_DIR)/table_client.o $(LIB_DIR)/client-lib.o $(LDFLAGS) -o $(BIN_DIR)/$@

proto: 
	$(PROTOC) sdmessage.proto
	mv sdmessage.pb-c.c $(SRC_DIR)/sdmessage.pb-c.c
	mv sdmessage.pb-c.h $(INC_DIR)/sdmessage.pb-c.h

client-lib.o: $(OBJLIB)
	ld -r $(addprefix $(OBJ_DIR)/, $^) -o $(LIB_DIR)/$@

clean:
	rm -f $(OBJ_DIR)/*
	rm -f $(BIN_DIR)/*
	rm -f $(LIB_DIR)/* 
