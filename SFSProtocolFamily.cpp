/*
 * SFSProtocolFamily.cpp
 *
 *  Created on: 2012-11-7
 *      Author: LiuYongJin
 */

#include "SFSProtocolFamily.h"

#define Case_Create_Protocol(Type, Protocol) case Type: \
protocol = new Protocol; break

Protocol* SFSProtocolFamily::create_protocol_by_header(ProtocolHeader *header)
{
	int protocol_type = ((DefaultProtocolHeader *)header)->get_protocol_type();
	Protocol *protocol = NULL;
	switch(protocol_type)
	{
		Case_Create_Protocol(PROTOCOL_FILE_INFO_REQ,         ProtocolFileInfoReq);
		Case_Create_Protocol(PROTOCOL_FILE_INFO,             ProtocolFileInfo);
		Case_Create_Protocol(PROTOCOL_FILE_INFO_SAVE_RESULT, ProtocolFileInfoSaveResult);
		Case_Create_Protocol(PROTOCOL_FILE_REQ,              ProtocolFileReq);
		Case_Create_Protocol(PROTOCOL_FILE,                  ProtocolFile);
		Case_Create_Protocol(PROTOCOL_FILE_SAVE_RESULT,      ProtocolFileSaveResult);
		Case_Create_Protocol(PROTOCOL_CHUNK_PING,            ProtocolChunkPing);
		Case_Create_Protocol(PROTOCOL_CHUNK_PING_RESP,       ProtocolChunkPingResp);
	}

	return protocol;
}

void SFSProtocolFamily::destroy_protocol(Protocol *protocol)
{
	if(protocol != NULL)
		delete protocol;
}

//////////////////////////////  0. FileInfoReq Protocol  //////////////////////////////
bool ProtocolFileInfoReq::encode_body(ByteBuffer *byte_buffer)
{
	////fid
	ENCODE_STRING(m_fid);
	////query chunk path
	char temp = (char)m_query_chunkpath;
	ENCODE_CHAR(temp);

	return true;
}

bool ProtocolFileInfoReq::decode_body(const char *buf, int size)
{
	////fid
	DECODE_STRING(m_fid);
	////query chunk path
	char temp;
	DECODE_CHAR(temp);
	m_query_chunkpath = (bool)temp;
	return true;
}

//////////////////////////////  1. FileInfo Protocol  //////////////////////////
bool ProtocolFileInfo::encode_body(ByteBuffer *byte_buffer)
{
	int temp;
	////result
	temp = (int)m_fileinfo.result;
	ENCODE_INT(temp);
	////fid
	ENCODE_STRING(m_fileinfo.fid);
	////file name
	ENCODE_STRING(m_fileinfo.name);
	////file size
	ENCODE_INT(m_fileinfo.size);
	////chunk info
	temp = m_fileinfo.get_chunkpath_count();
	ENCODE_INT(temp);
	while(temp > 0)
	{
		ChunkPath chunkpath = m_fileinfo.get_chunkpath(--temp);
		//chunk id
		ENCODE_STRING(chunkpath.id);
		//chunk ip
		ENCODE_STRING(chunkpath.ip);
		//chunk port
		ENCODE_INT(chunkpath.port);
		//index
		ENCODE_INT(chunkpath.index);
		//chunk offset
		ENCODE_INT(chunkpath.offset);
	}

	return true;
}

bool ProtocolFileInfo::decode_body(const char *buf, int size)
{
	int temp;
	////result
	DECODE_INT(temp);
	m_fileinfo.result = (FileInfo::Result)temp;
	////fid
	DECODE_STRING(m_fileinfo.fid);
	////file name
	DECODE_STRING(m_fileinfo.name);
	////file size
	DECODE_INT(m_fileinfo.size);
	////chunk info
	DECODE_INT(temp);
	while(temp-- > 0)
	{
		ChunkPath chunkpath;
		//chunk id
		DECODE_STRING(chunkpath.id);
		//chunk ip
		DECODE_STRING(chunkpath.ip);
		//chunk port
		DECODE_INT(chunkpath.port);
		//index
		DECODE_INT(chunkpath.index);
		//chunk offset
		DECODE_INT(chunkpath.offset);
		m_fileinfo.add_chunkpath(chunkpath);
	}

	return true;
}

//////////////////////////////  2. FileInfoSaveResult Protocol  //////////////////////////
bool ProtocolFileInfoSaveResult::encode_body(ByteBuffer *byte_buffer)
{
	int temp;
	////result
	temp = (int)m_save_result.result;
	ENCODE_INT(temp);
	////fid
	ENCODE_STRING(m_save_result.fid);

	return true;
}

bool ProtocolFileInfoSaveResult::decode_body(const char *buf, int size)
{
	int temp;
	////result
	DECODE_INT(temp);
	m_save_result.result = (FileInfoSaveResult::Result)temp;
	////fid
	DECODE_STRING(m_save_result.fid);

	return true;
}

//////////////////////////////  3. FileReq Protocol  //////////////////////////
bool ProtocolFileReq::encode_body(ByteBuffer *byte_buffer)
{
	////fid
	ENCODE_STRING(m_file_req.fid);
	////index
	ENCODE_INT(m_file_req.index);
	////offset
	ENCODE_INT(m_file_req.offset);
	////size
	ENCODE_INT(m_file_req.size);

	return true;
}

bool ProtocolFileReq::decode_body(const char *buf, int size)
{
	////fid
	DECODE_STRING(m_file_req.fid);
	////index
	DECODE_INT(m_file_req.index);
	////offset
	DECODE_INT(m_file_req.offset);
	////size
	DECODE_INT(m_file_req.size);

	return true;
}

//////////////////////////////  4. File Protocol  //////////////////////////
bool ProtocolFile::encode_body(ByteBuffer *byte_buffer)
{
	int temp;
	////flag
	temp = (int)m_file_seg.flag;
	ENCODE_INT(temp);
	////fid
	ENCODE_STRING(m_file_seg.fid);
	////name
	ENCODE_STRING(m_file_seg.name);
	////file size
	ENCODE_INT(m_file_seg.filesize);
	////seg offset
	ENCODE_INT(m_file_seg.offset);
	////seg index
	ENCODE_INT(m_file_seg.index);
	////seg size
	ENCODE_INT(m_file_seg.size);

	return true;
}

bool ProtocolFile::decode_body(const char *buf, int size)
{
	int temp;
	////flag
	DECODE_INT(temp);
	m_file_seg.flag = (FileSeg::FileFlag)temp;
	////fid
	DECODE_STRING(m_file_seg.fid);
	////name
	DECODE_STRING(m_file_seg.name);
	////file size
	DECODE_INT(m_file_seg.filesize);
	////seg offset
	DECODE_INT(m_file_seg.offset);
	////seg index
	DECODE_INT(m_file_seg.index);
	////seg size
	DECODE_INT(m_file_seg.size);
	////data
	if(size<m_file_seg.size) return false;
	m_file_seg.data = buf;

	return true;
}

//////////////////////////////  5. FileSaveResult Protocol  //////////////////////////
bool ProtocolFileSaveResult::encode_body(ByteBuffer *byte_buffer)
{
	int temp;
	//status
	temp = (int)m_save_result.status;
	ENCODE_INT(temp);
	////fid
	ENCODE_STRING(m_save_result.fid);
	////seg index
	ENCODE_INT(m_save_result.index);

	return true;
}

bool ProtocolFileSaveResult::decode_body(const char *buf, int size)
{
	int temp;
	//result
	DECODE_INT(temp);
	m_save_result.status = (FileSaveResult::Status)temp;
	////fid
	DECODE_STRING(m_save_result.fid);
	////seg index
	DECODE_INT(m_save_result.index);

	return true;
}

//////////////////////////////  6. ChunkPing Protocol  //////////////////////////
bool ProtocolChunkPing::encode_body(ByteBuffer *byte_buffer)
{
	////chunk id
	ENCODE_STRING(m_chunk_info.id);
	////chunk ip
	ENCODE_STRING(m_chunk_info.ip);
	////chunk port
	ENCODE_INT(m_chunk_info.port);
	//// disk space
	ENCODE_INT64(m_chunk_info.disk_space);
	////disk used
	ENCODE_INT64(m_chunk_info.disk_used);

	return true;
}

bool ProtocolChunkPing::decode_body(const char *buf, int size)
{
	////chunk id
	DECODE_STRING(m_chunk_info.id);
	////chunk ip
	DECODE_STRING(m_chunk_info.ip);
	////chunk port
	DECODE_INT(m_chunk_info.port);
	////disk space
	DECODE_INT64(m_chunk_info.disk_space);
	////disk used
	DECODE_INT64(m_chunk_info.disk_used);

	return true;
}

//////////////////////////////  7. ChunkPingResp Protocol  //////////////////////////
bool ProtocolChunkPingResp::encode_body(ByteBuffer *byte_buffer)
{
	int temp;
	////result
	temp = (int)m_result;
	ENCODE_INT(temp);
	////chunk id
	ENCODE_STRING(m_chunk_id);

	return true;
}

bool ProtocolChunkPingResp::decode_body(const char *buf, int size)
{
	int temp;
	////result
	DECODE_INT(temp);
	m_result = (bool)temp;
	////chunk id
	DECODE_STRING(m_chunk_id);

	return true;
}

