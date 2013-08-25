/*
 * SFSClient.cpp
 *
 *  Created on: 2012-11-8
 *      Author: LiuYongJin
 */
#include <string.h>
#include <stdio.h>
#include <string>
using std::string;
#include "SFDSFile.h"

void print_usage()
{
	printf("usage:\tsfs -i fid ... [get file info of fid]\n"
			"\tsfs -s file ... [save file to sfs]\n"
			"\tsfs -g fid file ...[get file of fid from sfs]\n");
}

int main(int argc, char* argv[])
{
	if(argc < 3)
	{
		print_usage();
		return -1;
	}

	string master_addr="127.0.0.1";
	int master_port = 3012;
	SFDS::File sfs_file(master_addr, master_port, 2);

	if(strcmp(argv[1], "-i") == 0) //get file info
	{
		string fid = argv[2];
		if(fid.size() != 40)
		{
			printf("fid is invalid.\n");
			return -11;
		}
		FileInfo file_info;
		file_info.result = FileInfo::RESULT_INVALID;
		if(sfs_file.GetFileInfo(fid, file_info))
		{
			printf("get file info succ. filename=%s. fid=%s, size=%d\n", file_info.name.c_str(), file_info.fid.c_str(), file_info.size);
			int i;
			for(i=0; i<file_info.GetChunkPathCount(); ++i)
			{
				ChunkPath &chunk_path = file_info.GetChunkPath(i);
				printf("chunk_path[%d]: id=%s, ip=%s, port=%d, %d_%d\n"
							, i
							, chunk_path.id.c_str()
							, chunk_path.ip.c_str()
							, chunk_path.port
							, chunk_path.index
							, chunk_path.offset);
			}
		}
		else
			printf("get file info failed.fid=%s, result=%d\n", fid.c_str(), file_info.result);
	}
	else if(strcmp(argv[1], "-s") == 0) //save file
	{
		string filename = argv[2];
		FileInfo file_info;
		file_info.result = FileInfo::RESULT_INVALID;
		if(sfs_file.SaveFile(file_info, filename) && file_info.result==FileInfo::RESULT_SUCC)
		{
			printf("save file succ. filename=%s. fid=%s, size=%d\n", file_info.name.c_str(), file_info.fid.c_str(), file_info.size);
			int i;
			for(i=0; i<file_info.GetChunkPathCount(); ++i)
			{
				ChunkPath &chunk_path = file_info.GetChunkPath(i);
				printf("chunk_path[%d]: id=%s, ip=%s, port=%d, %d_%d.\n"
							,i
							,chunk_path.id.c_str()
							,chunk_path.ip.c_str()
							,chunk_path.port
							,chunk_path.index
							,chunk_path.offset);
			}
		}
		else
			printf("save file failed.filename=%s, result=%d\n", filename.c_str(), file_info.result);
	}
	else if(strcmp(argv[1], "-g") == 0 && argc >= 4) //get file
	{
		string fid = argv[2];
		string filename = argv[3];
		sfs_file.GetFile(fid, filename);
	}
	else
		print_usage();

	return 0;
}


