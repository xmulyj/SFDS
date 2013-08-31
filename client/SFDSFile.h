/*
 * SFSClient.h
 *
 *  Created on: 2012-11-8
 *      Author: LiuYongJin
 */

#ifndef _LIB_SFS_CLIENT_H_20121108
#define _LIB_SFS_CLIENT_H_20121108

#include "Socket.h"
#include "ByteBuffer.h"
#include "KVData.h"
using namespace easynet;

#include "SFDSProtocolFactory.h"
#include "Protocol.h"

#include "Logger.h"

#include <stdint.h>
#include <string>
#include <vector>
using std::string;
using std::vector;

namespace SFDS
{

class File
{
public:
	File(string &master_addr, uint32_t master_port, uint32_t n_replica);

	//获取fid的文件信息,返回值: true(请求成功); false(请求失败)
	bool GetFileInfo(string &fid, FileInfo &file_info);

	//从sfs读取fid文件保存到local_file中
	bool GetFile(string &fid, string &local_file);

	//将文件local_file保存到sfs系统
	//失败返回false; 成功返回true,fileinfo表示保存后的文件信息
	bool SaveFile(FileInfo &fileinfo, string &local_file);
private:
	string m_master_addr;
	uint32_t    m_master_port;
	uint32_t    m_n_replica;
private:
	//获取fid的文件信息
	//query_chunk:当没有文件信息的时候是否请求分配chunk path
	//返回值:true(请求成功), false(请求失败)
	bool _get_file_info(string &fid, bool query_chunk, FileInfo &file_info);
	bool _send_file_to_chunk(string &local_file, string &fid, string &chunk_addr, int chunk_port, FileInfo &fileinfo);
	//保存数据到文件
	bool _save_filedata_to_file(FILE *fd, FileData &file_data);


	bool SendData(int fd, KVData &kvdata);
	bool RecvData(int fd, ByteBuffer &byte_buffer, KVData &kvdata);
private:
	DECL_LOGGER(logger);
};

}
#endif //_LIB_SFS_CLIENT_H_20121108


