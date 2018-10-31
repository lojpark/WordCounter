#include "mapreduce.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <dirent.h>

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

Hash_map **hash_map;
Partitioner hash_function;
int num_partitions;

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

	// Create root node
	if (hash_map[index] == NULL) {
		hash_map[index] = (Hash_map*) malloc(sizeof(Hash_map));

		new_node = create_node(value);
		new_node->parent = NULL;

		hash_map[index]->root = new_node;
		hash_map[index]->freq = new_node;
		hash_map[index]->smallest = NULL;
		return;
	}

	// When input value is the most frequent node, just increase the count and return
	if (strcmp(hash_map[index]->freq->value, value) == 0) {
		hash_map[index]->freq->count++;
		return;
	}

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

void call_reduce(Reducer reduce, int index)
{
	char *key;

	key = (char*) malloc(sizeof(char) * (strlen(hash_map[index]->smallest->value) + 1));
	strcpy(key, hash_map[index]->smallest->value);

	reduce(key, get_next, index);

	free(key);
}

void MR_Emit(char *key, char *value)
{
	unsigned long index;
	index = hash_function(key, num_partitions);

	// Insert node at the hash map
	insert_node(index, key);

	return;
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partition)
{
	int i;
	DIR *dir;
	char file_name[1000];
	struct dirent *ent;

	num_partitions = atoi(argv[2]);
	hash_function = partition;
	hash_map = (Hash_map**) malloc(sizeof(Hash_map*) * num_partitions);

	dir = opendir(argv[1]);
	if (dir != NULL) {
		while ((ent = readdir(dir)) != NULL) {
			if (strcmp(ent->d_name, ".") == 0) continue;
			if (strcmp(ent->d_name, "..") == 0) continue;
			strcpy(file_name, argv[1]);
			strcat(file_name, "/");
			strcat(file_name, ent->d_name);

			// Call mapper
			map(file_name);
		}
		closedir(dir);
	}

	for (i = 0; i < num_partitions; i++) {
		if (hash_map[i] != NULL) {
			// Find the smallest node
			hash_map[i]->smallest = hash_map[i]->root;

			while (hash_map[i]->smallest->left != NULL) {
				hash_map[i]->smallest = hash_map[i]->smallest->left;
			}

			// Call reducer
			while (hash_map[i]->smallest != NULL)
				call_reduce(reduce, i);
			free(hash_map[i]);
		}
	}

	free(hash_map);
}
