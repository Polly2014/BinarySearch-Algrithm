/*
 * mksary.c for libdivsufsort
 * Copyright (c) 2003-2008 Yuta Mori All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#if HAVE_CONFIG_H
# include "config.h"
#endif
#include <stdio.h>
#if HAVE_STRING_H
# include <string.h>
#endif
#if HAVE_STDLIB_H
# include <stdlib.h>
#endif
#if HAVE_MEMORY_H
# include <memory.h>
#endif
#if HAVE_STDDEF_H
# include <stddef.h>
#endif
#if HAVE_STRINGS_H
# include <strings.h>
#endif
#if HAVE_SYS_TYPES_H
# include <sys/types.h>
#endif
#if HAVE_IO_H && HAVE_FCNTL_H
# include <io.h>
# include <fcntl.h>
#endif
#include <time.h>
#include <divsufsort.h>
#include "lfs.h"
#include "parson.h"
#include "hdfs.h"

char config_schema[] = "{\
		\"hostname\":\"\",\
		\"port\":0,\
		\"index_file\":\"\",\
		\"raw_files\":[\
			\{\
				\"name\":\"\",\
				\"offset\":0,\
				\"length\":0\
			},\
			\{\
				\"name\":\"\",\
				\"offset\":0,\
				\"length\":0\
			}\
		]\
	}";

typedef struct {
	off_t offset;		/*<< starting position in the file */
	off_t length;		/*<< the length of the data that to be considered in the file */
	off_t offset_in_list;	/*<< the starting position is what offset of in the file list as a virtual file? */
	const char *name;	/*<< full path of the file */
} *file_info, file_info_t;

typedef struct {
	size_t count;
	off_t total_len;
	file_info files;
	FILE **fps;
} *file_list, file_list_t;

file_list create_filelist_from_json(JSON_Array *json_array)
{
	size_t i;
	file_list flist;
	JSON_Object *afile;

	/* allocate memory*/
	flist = malloc(sizeof(*flist));
	memset(flist, 0, sizeof(*flist));

	/* set the number of files */
	flist->count = json_array_get_count(json_array);
	if ((flist->count = json_array_get_count(json_array)) == 0) {
		return flist;
	}

	/* Allocate memory for file info and handles */
	flist->files = malloc(sizeof(*flist->files) * flist->count);
	memset(flist->files, 0, sizeof(*flist->files) * flist->count);
	flist->fps = malloc(sizeof(*flist->fps) * flist->count);
	memset(flist->fps, 0, sizeof(*flist->fps) * flist->count);

	/* fill file info */
	for (i = 0, flist->total_len = 0; i < flist->count; i++) {
		afile = json_array_get_object(json_array, i);
		flist->files[i].name = json_object_get_string(afile, "name");
		flist->files[i].offset = (off_t) json_object_get_number(afile, "offset");
		flist->files[i].length = (off_t) json_object_get_number(afile, "length");
		flist->files[i].offset_in_list = flist->total_len;
		flist->total_len += flist->files[i].length;
	}

	return flist;
}

file_info get_file_from_list(file_list flist, off_t offset)
{
	off_t left, middle, right;
	file_info files;

	if (flist == NULL || (files = flist->files) == NULL || flist->count == 0) {
		return NULL;
	}

	/* Binary search to find the appropriate file to read */
	for (left = 0, right = flist->count - 1; left < right; /* idle here */) {
		middle = (left + right) / 2;
		if (offset < files[middle].offset_in_list) {
			right = middle - 1;
		} else if (offset < files[middle].offset_in_list + files[middle].length) {
			/* middle.offset <= offset < middle.offset + length: middle is what we are looking for */
			left = right = middle;
		} else {
			left = middle + 1;
		}
	}

	return (files[left].offset_in_list <= offset && offset < files[left].offset_in_list + files[left].length) ? (files + left) : NULL;
}

#define GRAM_BITS 24
#define GRAM_ARRAY_SIZE (1<<GRAM_BITS)

/**
 * This function calculates the 3-gram starting position of the suffix array
 * @param[in] T the data stream that be calculated, it should be at least 8 bytes larger than n
 * @param[out] GRAM the 3-gram starting position
 * @param[in] size of the data stream
 */
saint_t gram_pos(const sauchar_t *T, saidx_t *GRAM, saidx_t n)
{
	saidx_t i;

	/* sanity check */
	if (T == NULL || GRAM == NULL) {
		return -1;
	}

	memset(GRAM, 0, (size_t)GRAM_ARRAY_SIZE * sizeof(*GRAM));
	for (i = 0; i < n; i++) {
		GRAM[(T[i]<<16) + (T[i+1]<<8) + T[i+2]]++;
	}
	for (i = 0; i < GRAM_ARRAY_SIZE - 1; i++) {
		GRAM[i+1] += GRAM[i];
	}
	for (i = GRAM_ARRAY_SIZE - 1; i > 0; i--) {
		GRAM[i] = GRAM[i-1];
	}
	GRAM[0] = 0;

	return 0;
}

void exit_result(int code, const char *msg) {
	fprintf(stdout, "{\"code\": %d, \"message\":\"%s\"}\n", code, msg);
	exit(code);
}


char errmsg[1024];
#define exit_result(code, strfmt...) {	\
	snprintf(errmsg, sizeof(errmsg), strfmt);\
	fprintf(stdout, "{\"code\": %d, \"message\":\"%s: %s\"}\n", code, argv[0], errmsg);\
	exit(code);\
}

int
main(int argc, const char *argv[])
{

	/* 
	 * 2017-12-25
	 */
	hdfsFS fs;
	hdfsFile hofp, hfp;
	const char *hostname = "default";
	int port = 9000;


	// FILE *fp, *ofp;
	// FILE *fp;
	size_t i;
	sauchar_t *T;
	saidx_t *SA, *GRAM;
	LFS_OFF_T n, curp;

	JSON_Value *config, *schema;
	file_list raw_files;
	file_info infile;
	const char *index_file;
	char errmsg[1024];

	/* Check arguments. */
	if (argc != 2) {
		exit_result(1, "Invalid argument");
	}

	/* Check JSON format. */
	config = json_parse_file(argv[1]);
	schema = json_parse_string(config_schema);
	if (config == NULL || schema == NULL) {
		exit_result(2, "json config file %s error %s", argv[1], schema == NULL?"schema":"");
	} else if (json_validate(schema, config) != JSONSuccess) {
		exit_result(3, "json config file %s does not have valid format", argv[1]);
	}

	/* Convert JSON file into a structure */
	raw_files = create_filelist_from_json(json_object_get_array(json_object(config), "raw_files"));
	index_file = json_object_get_string(json_object(config), "index_file");
	hostname = json_object_get_string(json_object(config), "hostname");
	port = json_object_get_number(json_object(config), "port");
	if ((n = raw_files->total_len) > 0xFFFFFFFFLL) {
		/* We only support 4gB file length at maximum */
		exit_result(6, "overall input file length too huge (exceeds 4GB): %zd", raw_files->total_len);
	}

	/* Connect to HDFS by HDFS C API */
	fs = hdfsConnect(hostname, port);
	if (!fs) {
        exit_result(11, "Connect HDFS failed!");
    }

	/* Allocate 5blocksize bytes of memory. */
	T = (sauchar_t *)malloc((size_t)(n+8) * sizeof(sauchar_t));
	SA = (saidx_t *)malloc((size_t)n * sizeof(saidx_t));
	GRAM = (saidx_t *)malloc((size_t)GRAM_ARRAY_SIZE * sizeof(saidx_t));
	if(T == NULL || SA == NULL || GRAM == NULL) {
		exit_result(7, "cannot allocate memory");
	}
	memset(T+n, 8, 0);

	/* Open each input file and read */
	// Added 2017-12-25
	for (i = 0, curp = 0; i < raw_files->count; i++) {
		infile = raw_files->files + i;
		if (hdfsExists(fs, infile->name) != 0) {
			exit_result(12, "Filepath[%s] not exists", infile->name);
		}
		if ((hfp = hdfsOpenFile(fs, infile->name, O_RDONLY, 0, 0, 0)) == NULL) {
			exit_result(4, "open input file failed: %s", infile->name);
		}
		hdfsSeek(fs, hfp, infile->offset);
		if (hdfsRead(fs, hfp, T + curp, sizeof(sauchar_t)*(size_t)infile->length) != sizeof(sauchar_t)*(size_t)infile->length) {
			exit_result(4, "%s %s: offset %zd, length %zd",
					"Cannot read from(Unexpected EOF in)",
					infile->name, infile->offset, infile->length);
		}
		curp += infile->length;
		hdfsCloseFile(fs, hfp);
	}
	// Ended Added
	/*
	for (i = 0, curp = 0; i < raw_files->count; i++) {
		infile = raw_files->files + i;
		if ((fp = LFS_FOPEN(infile->name, "rb")) == NULL) {
			exit_result(4, "open input file failed: %s", infile->name);
		}
		LFS_FSEEK(fp, infile->offset, SEEK_SET);	// SEEK_SET means "fromwhere=top"
		if (fread(T + curp, sizeof(sauchar_t), (size_t)infile->length, fp) != (size_t)infile->length) {
			exit_result(4, "%s %s: offset %zd, length %zd",
					(ferror(fp) || !feof(fp)) ? "Cannot read from" : "Unexpected EOF in",
					infile->name, infile->offset, infile->length);
		}
		curp += infile->length;
		fclose(fp);
	}
	*/
	

	/* open index file to write */
	// Added 2017-12-25
	if ((hofp = hdfsOpenFile(fs, index_file, O_WRONLY||O_CREAT, 0, 0, 0)) == NULL) {
		exit_result(4, "cannot open index file %s", index_file);
	}
	// End Added
	/*
	if ((ofp = LFS_FOPEN(index_file, "wb")) == NULL) {
		exit_result(4, "cannot open index file %s", index_file);
	}
	*/

	/* Construct&write the suffix array */
	if(divsufsort(T, SA, (saidx_t)n) != 0) {
		exit_result(7, "cannot allocate memory");
	}
	// Added 2017-12-25
	if ((hdfsWrite(fs, hofp, SA, sizeof(saidx_t)*(size_t)n)) != sizeof(saidx_t)*(size_t)n) {
		exit_result(4, "cannot write to index file %s", index_file);
	}
	// End Added
	/*
	if (fwrite(SA, sizeof(saidx_t), (size_t)n, ofp) != (size_t)n) {
		exit_result(4, "cannot write to index file %s", index_file);
	}
	*/

	/* Construct&write the 3-gram position array */
	gram_pos(T, GRAM, n);
	// Added 2017-12-25
	if ((hdfsWrite(fs, hofp, GRAM, sizeof(saidx_t)*(size_t)GRAM_ARRAY_SIZE)) != sizeof(saidx_t)*(size_t)GRAM_ARRAY_SIZE) {
		exit_result(4, "cannot write to index file %s", index_file);
	}
	// End Added
	/*
	if (fwrite(GRAM, sizeof(saidx_t), (size_t)GRAM_ARRAY_SIZE, ofp) != (size_t)GRAM_ARRAY_SIZE) {
		exit_result(4, "cannot write to index file %s", index_file);
	}
	*/
	if (hdfsFlush(fs, hofp)) {
		exit_result(4, "cannot write to index file %s", index_file);	
	}

	hdfsCloseFile(fs, hofp);
	hdfsDisconnect(fs);

	// fclose(ofp);
	free(SA);
	free(GRAM);
	free(T);

	exit_result(0, "Success");
}
