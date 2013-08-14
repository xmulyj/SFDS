/*
 * SFDSProtocolFactory.cpp
 *
 *  Created on: 2013-08-08
 *      Author: tim
 */

#include "SFDSProtocolFactory.h"
#include "KVData.h"
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
	uint32_t temp = *(uint32_t *)buffer;  //magic_num
	temp = ntohl(temp);
	if(temp != (uint32_t)MAGIC_NUM)
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
	uint32_t magic_num = htonl(MAGIC_NUM);
	*(uint32_t*)buffer = magic_num;

	body_size = htonl(body_size);
	*(uint32_t*)(buffer+sizeof(uint32_t)) = body_size;

	return ;
}

DecodeResult SFDSProtocolFactory::DecodeBinBody(ProtocolContext *context)
{
	////Add Your Code Here
	assert(context->header_size == HEADER_SIZE);

	void *temp = m_Memory->Alloc(sizeof(KVData));
	assert(temp != NULL);
	KVData *kv_data = new(temp) KVData(true);

	char *body_data = context->Buffer+HEADER_SIZE;
	if(kv_data->UnSerialize(body_data, context->body_size) == false)
	{
		m_Memory->Free(temp, sizeof(KVData));
		return DECODE_ERROR;
	}

	context->protocol = (void*)kv_data;
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
	m_Memory->Free(protocol, sizeof(KVData));
	return ;
}

