CC=gcc
CFLAGS=-Wall -Werror -pthread -O
TARGET=single_mapreduce multi_mapreduce

all: $(TARGET)

single_mapreduce: 
	$(CC) -o $@ wordcount.c single_mapreduce.c $(CFLAGS)

multi_mapreduce:
	$(CC) -o $@ wordcount.c multi_mapreduce.c $(CFLAGS)

clean:
	rm -f $(TARGET)
