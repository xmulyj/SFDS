/*
 * SFSProtocolFamily.h
 *
 *  Created on: 2012-11-7
 *      Author: LiuYongJin
 */

#ifndef _SFS_PROTOCOL_FAMILY_H_
#define _SFS_PROTOCOL_FAMILY_H_

#include "DefaultProtocolFamily.h"
#include "CommonType.h"

#include <stdint.h>
#include <string>
#include <vector>
using std::string;
using std::vector;

//文件信息
#define PROTOCOL_FILE_INFO_REQ          0    //请求文件信息
#define PROTOCOL_FILE_INFO              1    //文件信息
#define PROTOCOL_FILE_INFO_SAVE_RESULT  2    //文件信息保存结果
//文件数据
#define PROTOCOL_FILE_REQ               3    //请求文件数据
#define PROTOCOL_FILE                   4    //文件数据
#define PROTOCOL_FILE_SAVE_RESULT       5    //文件保存状态
//chunk信息
#define PROTOCOL_CHUNK_PING             6    //chunk请求master保存chunk信息
#define PROTOCOL_CHUNK_PING_RESP        7    //master回复chunk保存信息结果


///////////////////////////////////////  Protocol Family  ///////////////////////////////////////
class SFSProtocolFamily:public DefaultProtocolFamily
{
public:
	Protocol* create_protocol_by_header(ProtocolHeader *header);
	void destroy_protocol(Protocol *protocol);
};

//////////////////////////////  0. FileInfoReq Protocol  //////////////////////////////
class ProtocolFileInfoReq:public Protocol
{
public://实现protocol的接口
	//协议的描述信息
	const char* details(){return "FileInfoReq Protocol";}
	//编码协议体数据到byte_buffer,成功返回true,失败返回false.
	bool encode_body(ByteBuffer *byte_buffer);
	//解码大小为size的协议体数据buf.成功返回true,失败返回false.
	bool decode_body(const char *buf, int size);
public:
	string& get_fid(){return m_fid;}
	bool& get_query_chunkpath(){return m_query_chunkpath;}
private:
	string m_fid;           //文件的fid
	bool m_query_chunkpath; //如果没有文件信息,是否请求分配chunk
};

//////////////////////////////  1. FileInfo Protocol  //////////////////////////////
class ProtocolFileInfo:public Protocol
{
public://实现protocol的接口
	//协议的描述信息
	const char* details(){return "FileInfo Protocol";}
	//编码协议体数据到byte_buffer,成功返回true,失败返回false.
	bool encode_body(ByteBuffer *byte_buffer);
	//解码大小为size的协议体数据buf.成功返回true,失败返回false.
	bool decode_body(const char *buf, int size);
public:
	FileInfo& get_fileinfo(){return m_fileinfo;}
private:
	FileInfo m_fileinfo;
};

//////////////////////////////  2. ProtocolFileInfoSaveResult Protocol  //////////////////////////////
class ProtocolFileInfoSaveResult:public Protocol
{
public://实现protocol的接口
	//协议的描述信息
	const char* details(){return "FileInfoSaveResult Protocol";}
	//编码协议体数据到byte_buffer,成功返回true,失败返回false.
	bool encode_body(ByteBuffer *byte_buffer);
	//解码大小为size的协议体数据buf.成功返回true,失败返回false.
	bool decode_body(const char *buf, int size);
public:
	FileInfoSaveResult& get_save_result(){return m_save_result;}
private:
	FileInfoSaveResult m_save_result;
};

//////////////////////////////  3. ProtocolFileReq Protocol  //////////////////////////////
class ProtocolFileReq:public Protocol
{
public://实现protocol的接口
	//协议的描述信息
	const char* details(){return "FileReq Protocol";}
	//编码协议体数据到byte_buffer,成功返回true,失败返回false.
	bool encode_body(ByteBuffer *byte_buffer);
	//解码大小为size的协议体数据buf.成功返回true,失败返回false.
	bool decode_body(const char *buf, int size);
public:
	FileReq& get_file_req(){return m_file_req;}
private:
	FileReq m_file_req;
};

//////////////////////////////  4. ProtocolFile Protocol  //////////////////////////////
class ProtocolFile:public Protocol
{
public://实现protocol的接口
	//协议的描述信息
	const char* details(){return "File Protocol";}
	//编码协议体数据到byte_buffer,成功返回true,失败返回false.
	bool encode_body(ByteBuffer *byte_buffer);
	//解码大小为size的协议体数据buf.成功返回true,失败返回false.
	bool decode_body(const char *buf, int size);
public:
	FileSeg& get_file_seg(){return m_file_seg;}
private:
	FileSeg m_file_seg;
};

//////////////////////////////  5. FileSaveResult Protocol  //////////////////////////////
class ProtocolFileSaveResult:public Protocol
{
public://实现protocol的接口
	//协议的描述信息
	const char* details(){return "SaveStatus Protocol";}
	//编码协议体数据到byte_buffer,成功返回true,失败返回false.
	bool encode_body(ByteBuffer *byte_buffer);
	//解码大小为size的协议体数据buf.成功返回true,失败返回false.
	bool decode_body(const char *buf, int size);
public:
	FileSaveResult& get_save_result(){return m_save_result;}
private:
	FileSaveResult m_save_result;
};

//////////////////////////////  6. ChunkPing Protocol  //////////////////////////////
class ProtocolChunkPing:public Protocol
{
public://实现protocol的接口
	//协议的描述信息
	const char* details(){return "ChunkPing Protocol";}
	//编码协议体数据到byte_buffer,成功返回true,失败返回false.
	bool encode_body(ByteBuffer *byte_buffer);
	//解码大小为size的协议体数据buf.成功返回true,失败返回false.
	bool decode_body(const char *buf, int size);
public:
	//chunk info
	ChunkInfo& get_chunk_info(){return m_chunk_info;}
private:
	ChunkInfo m_chunk_info;
};

//////////////////////////////  7. ChunkPingResp Protocol  //////////////////////////////
class ProtocolChunkPingResp:public Protocol
{
public://实现protocol的接口
	//协议的描述信息
	const char* details(){return "ChunkPingResp Protocol";}
	//编码协议体数据到byte_buffer,成功返回true,失败返回false.
	bool encode_body(ByteBuffer *byte_buffer);
	//解码大小为size的协议体数据buf.成功返回true,失败返回false.
	bool decode_body(const char *buf, int size);
public:
	bool& get_result(){return m_result;}
	string& get_chunk_id(){return m_chunk_id;}
private:
	bool m_result;
	string m_chunk_id;
};

#endif //_SFS_PROTOCOL_FAMILY_H_


