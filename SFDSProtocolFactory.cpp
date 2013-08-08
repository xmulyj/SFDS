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

	return ;
}

