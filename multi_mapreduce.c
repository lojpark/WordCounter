#include "mapreduce.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <dirent.h>
#include <pthread.h>
#include <stdatomic.h>

typedef struct _Hash_node
{
	char *value;
	int count;
	struct _Hash_node *parent;
	struct _Hash_node *left;
	struct _Hash_node *right;
}Hash_node;

typedef struct _Hash_map
{
	Hash_node *root;
	Hash_node *freq;
	Hash_node *smallest;
}Hash_map;

typedef struct _Map_Task
{
	int n;
	char **file_name;
}Map_Task;

typedef struct _Reduce_Task
{
	int n;
	int *index;
}Reduce_Task;

Mapper map_function;
Reducer reduce_function;
Hash_map **hash_map;
Partitioner hash_function;
int num_partitions;
pthread_mutex_t *partition_lock;

Hash_node *create_node(char *value)
{
	// Create and initialize the new node
	Hash_node *new_node = (Hash_node*) malloc(sizeof(Hash_node));

	new_node->value = (char *) malloc(sizeof(char) * (strlen(value) + 1));
	strcpy(new_node->value, value);
	new_node->left = NULL;
	new_node->right = NULL;
	new_node->count = 1;

	return new_node;
}

void insert_node(int index, char *value)
{
	Hash_node *new_node, *current_node, *parent_node;

	// Create root node (Need lock)
	pthread_mutex_lock(&partition_lock[index]);
	if (hash_map[index] == NULL) {
		hash_map[index] = (Hash_map*) malloc(sizeof(Hash_map));

		new_node = create_node(value);
		new_node->parent = NULL;

		hash_map[index]->root = new_node;
		hash_map[index]->freq = new_node;
		hash_map[index]->smallest = NULL;
		pthread_mutex_unlock(&partition_lock[index]);
		return;
	}
	pthread_mutex_unlock(&partition_lock[index]);

	// When input value is the most frequent node, just increase the count and return (lock free)
	if (strcmp(hash_map[index]->freq->value, value) == 0) {
		atomic_fetch_add(&hash_map[index]->freq->count, 1);
		return;
	}

	pthread_mutex_lock(&partition_lock[index]);
	parent_node = NULL;
	current_node = hash_map[index]->root;

	// Find appropriate location of the node
	while (current_node != NULL) {
		// If the value already exist, just count it
		if (strcmp(current_node->value, value) == 0) {
			current_node->count++;
			if (current_node->count > hash_map[index]->freq->count) {
				hash_map[index]->freq = current_node;
			}
			pthread_mutex_unlock(&partition_lock[index]);
			return;
		}
		// If not, add new node at appropriate location
		parent_node = current_node;
		if (strcmp(current_node->value, value) > 0) {
			current_node = current_node->left;
		}
		else {
			current_node = current_node->right;
		}
	}

	// Add new node
	new_node = create_node(value);
	new_node->parent = parent_node;
	if (strcmp(parent_node->value, value) > 0) parent_node->left = new_node;
	else parent_node->right = new_node;

	pthread_mutex_unlock(&partition_lock[index]);
}

char *get_next(char *key, int partition_number)
{
	Hash_node *current_node;
	if (hash_map[partition_number]->smallest == NULL) return NULL;

	current_node = hash_map[partition_number]->smallest;

	if (strcmp(current_node->value, key) == 0) {
		// Decrease the count of node
		current_node->count--;
		// Remove node from partition when the count is 0
		if (current_node->count <= 0) {
			// Node that should be erased never have left child
			if (current_node->parent == NULL) {
				hash_map[partition_number]->root = current_node->right;
				if (current_node->right != NULL) current_node->right->parent = NULL;
				hash_map[partition_number]->smallest = hash_map[partition_number]->root;
			}
			else {
				current_node->parent->left = current_node->right;
				if (current_node->right != NULL) current_node->right->parent = current_node->parent;
				hash_map[partition_number]->smallest = current_node->parent;
			}

			free(current_node->value);
			free(current_node);

			// Find the smallest node again
			if (hash_map[partition_number]->smallest != NULL) {
				while (hash_map[partition_number]->smallest->left != NULL) {
					hash_map[partition_number]->smallest = hash_map[partition_number]->smallest->left;
				}
			}
		}
		return key;
	}

	return NULL;
}

void *map_partition(void *assigned_task)
{
	Map_Task task = *(Map_Task*) assigned_task;
	int i;

	// Call the mapping function for each allocated task
	for (i = 0; i < task.n; i++) {
		map_function(task.file_name[i]);
	}

	return NULL;
}

void call_reduce(int index)
{
	char *key;

	key = (char*) malloc(sizeof(char) * (strlen(hash_map[index]->smallest->value) + 1));
	strcpy(key, hash_map[index]->smallest->value);

	reduce_function(key, get_next, index);

	free(key);
}

void *reduce_partition(void *assigned_task)
{
	Reduce_Task task = *(Reduce_Task*) assigned_task;
	int i;

	// Call reducer for each allocated task
	for (i = 0; i < task.n; i++) {
		// Find the smallest node
		hash_map[task.index[i]]->smallest = hash_map[task.index[i]]->root;

		while (hash_map[task.index[i]]->smallest->left != NULL) {
			hash_map[task.index[i]]->smallest = hash_map[task.index[i]]->smallest->left;
		}

		// Call reducer
		while (hash_map[task.index[i]]->root != NULL) {
			call_reduce(task.index[i]);
		}

		free(hash_map[task.index[i]]);
	}

	return NULL;
}

void MR_Emit(char *key, char *value)
{
	unsigned long index;
	index = hash_function(key, num_partitions);

	// Insert node at the hash map
	insert_node(index, key);
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partition)
{
	int i, j, k;
	DIR *dir;
	char file_name[1000];
	struct dirent *ent;
	int n = 0;
	pthread_t *mapper, *reducer;
	Map_Task *map_tasks;
	Reduce_Task *reduce_tasks;

	// Initialize hash_map
	num_partitions = atoi(argv[2]);
	hash_function = partition;
	hash_map = (Hash_map**) malloc(sizeof(Hash_map*) * num_partitions);

	// Initialize mapper
	map_function = map;
	mapper = (pthread_t*) malloc(sizeof(pthread_t) * num_mappers);
	map_tasks = (Map_Task*) malloc(sizeof(Map_Task) * num_mappers);

	// Initialize reducer
	reduce_function = reduce;
	reducer = (pthread_t*) malloc(sizeof(pthread_t) * num_reducers);
	reduce_tasks = (Reduce_Task*) malloc(sizeof(Reduce_Task) * num_reducers);

	// Count the number of files
	n = 0;
	dir = opendir(argv[1]);
	if (dir != NULL) {
		while ((ent = readdir(dir)) != NULL) {
			if (strcmp(ent->d_name, ".") == 0) continue;
			if (strcmp(ent->d_name, "..") == 0) continue;
			
			n++;
		}
		closedir(dir);
	}

	// Allocate the task of mappers
	dir = opendir(argv[1]);
	if (dir != NULL) {
		for (i = 0; i < num_mappers; i++) {
			k = 0;
			map_tasks[i].file_name = (char**) malloc(sizeof(char*) * (n / num_mappers + 1));

			while ((ent = readdir(dir)) != NULL) {
				if (strcmp(ent->d_name, ".") == 0) continue;
				if (strcmp(ent->d_name, "..") == 0) continue;
				strcpy(file_name, argv[1]);
				strcat(file_name, "/");
				strcat(file_name, ent->d_name);

				map_tasks[i].file_name[k] = (char*) malloc(sizeof(char) * (strlen(file_name) + 1));
				strcpy(map_tasks[i].file_name[k++], file_name);
				if (k >= n / num_mappers + 1) break;
			}
			map_tasks[i].n = k;
		}
		closedir(dir);
	}

	// Initialize locker
	partition_lock = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t) * num_partitions);
	for (i = 0; i < num_partitions; i++) {
		pthread_mutex_init(&partition_lock[i], NULL);
	}

	// Create mappers
	for (i = 0; i < num_mappers; i++) {
		pthread_create(&mapper[i], NULL, map_partition, (void *) &map_tasks[i]);
	}
	
	// End mappers
	for (i = 0; i < num_mappers; i++) {
		pthread_join(mapper[i], NULL);
	}

	// Count the number of partitions
	n = 0;
	for (i = 0; i < num_partitions; i++) {
		if (hash_map[i] != NULL) {
			n++;
		}
	}

	// Allocate the task of reducers
	j = 0;
	for (i = 0; i < num_reducers; i++) {
		k = 0;
		reduce_tasks[i].index = (int*) malloc(sizeof(int) * (n / num_reducers + 1));
		for (; j < num_partitions; j++)	{
			if (hash_map[j] != NULL) {
				reduce_tasks[i].index[k++] = j;
				if (k >= n / num_reducers + 1) break;
			}
		}
		j++;
		reduce_tasks[i].n = k;
	}

	// Create reducers
	for (i = 0; i < num_reducers; i++) {
		pthread_create(&reducer[i], NULL, reduce_partition, (void *) &reduce_tasks[i]);
	}
	
	// End reducers
	for (i = 0; i < num_reducers; i++) {
		pthread_join(reducer[i], NULL);
	}

	free(hash_map);
}
