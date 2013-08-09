/*
 * SFDSProtocolFactory.cpp
 *
 *  Created on: 2013-08-08
 *      Author: tim
 */

#include "SFDSProtocolFactory.h"
#include "KVData.h"
#include "CommonType.h"
#include <netinet/in.h>
#include <new>

IMPL_LOGGER(SFDSProtocolFactory, logger);


#define HEADER_SIZE 8  //magic_num(4Bytes)+body_len(4Bytes)
#define MAGIC_NUM 0xD0C0A0B0

uint32_t SFDSProtocolFactory::HeaderSize()
{
	////Add Your Code Here
	return HEADER_SIZE;
}

DecodeResult SFDSProtocolFactory::DecodeHeader(const char *buffer, DataType &type, uint32_t &body_size)
{
	////Add Your Code Here
	int32_t temp = *(uint32_t *)buffer;  //magic_num
	temp = ntohl(temp);
	if(temp != MAGIC_NUM)
	{
		LOG_ERROR(logger, "invalid magic num. recv="<<temp<<" expect="<<MAGIC_NUM);
		return DECODE_ERROR;
	}

	temp = *(uint32_t*)(buffer+sizeof(uint32_t));  //body_size
	temp = ntohl(temp);
	if(temp <= 0)
	{
		LOG_ERROR(logger, "invalid body size. recv="<<temp);
		return DECODE_ERROR;
	}
	LOG_DEBUG(logger, "decode body_size="<<temp);

	body_size = temp;
	type = DTYPE_BIN;
	return DECODE_SUCC;
}

void SFDSProtocolFactory::EncodeHeader(char *buffer, uint32_t body_size)
{
	////Add Your Code Here

	return ;
}

#define Case_Create_Protocol(type, pro_type) case type: \
	temp_protocol = m_Memory->Alloc(sizeof(pro_type));   \
	temp_protocol = new(temp_protocol) pro_type;         \
	break;

#define Case_Delete_Protocol(type, pro_type) case type: \
	m_Memory->Free(protocol, sizeof(pro_type)); \
	break;

DecodeResult SFDSProtocolFactory::DecodeBinBody(ProtocolContext *context)
{
	////Add Your Code Here
	KVData kv_data;
	assert(context->header_size == HEADER_SIZE);
	char *body_data = context->Buffer+HEADER_SIZE;
	if(kv_data.UnPack(body_data, context->body_size, true) == false)
		return DECODE_ERROR;
	int32_t protocol_type;
	if(kv_data.GetInt32(KEY_PROTOCOL_TYPE, protocol_type) == false)
		return DECODE_ERROR;
	if(protocol_type<=PROTOCOL_BEGIN || protocol_type>=PROTOCOL_END)
		return DECODE_ERROR;

	void *temp_protocol = NULL;    //不要修改该变量名,宏定义中使用
	switch(protocol_type)
	{
	Case_Create_Protocol(PROTOCOL_CHUNK_PING, ChunkPing);
	Case_Create_Protocol(PROTOCOL_CHUNK_PING_RESP, ChunkPingRsp);
	Case_Create_Protocol(PROTOCOL_FILE_INFO_REQ, FileInfoReq);
	Case_Create_Protocol(PROTOCOL_FILE_INFO, FileInfo);
	Case_Create_Protocol(PROTOCOL_FILE_INFO_SAVE_RESULT, FileInfoSaveResult);
	Case_Create_Protocol(PROTOCOL_FILE_REQ, FileReq);
	Case_Create_Protocol(PROTOCOL_FILE, File);
	Case_Create_Protocol(PROTOCOL_FILE_SAVE_RESULT, FileSaveResult);
	default:
		return DECODE_ERROR;
	}

	context->protocol = temp_protocol;
	context->protocol_type = protocol_type;

	return DECODE_SUCC;
}

DecodeResult SFDSProtocolFactory::DecodeTextBody(ProtocolContext *context)
{
	////Add Your Code Here

	return DECODE_SUCC;
}

void SFDSProtocolFactory::DeleteProtocol(uint32_t protocol_type, void *protocol)
{
	////Add Your Code Here
	switch(protocol_type)
	{
	Case_Delete_Protocol(PROTOCOL_CHUNK_PING, ChunkPing);
	Case_Delete_Protocol(PROTOCOL_CHUNK_PING_RESP, ChunkPingRsp);
	Case_Delete_Protocol(PROTOCOL_FILE_INFO_REQ, FileInfoReq);
	Case_Delete_Protocol(PROTOCOL_FILE_INFO, FileInfo);
	Case_Delete_Protocol(PROTOCOL_FILE_INFO_SAVE_RESULT, FileInfoSaveResult);
	Case_Delete_Protocol(PROTOCOL_FILE_REQ, FileReq);
	Case_Delete_Protocol(PROTOCOL_FILE, File);
	Case_Delete_Protocol(PROTOCOL_FILE_SAVE_RESULT, FileSaveResult);
	}

	return ;
}

