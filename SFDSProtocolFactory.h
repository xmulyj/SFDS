/*
 * SFDSProtocolFactory.h
 *
 *  Created on: 2013-08-08
 *      Author: tim
 */

#ifndef _SFDSPROTOCOLFACTORY_H_
#define _SFDSPROTOCOLFACTORY_H_

#include "IProtocolFactory.h"
#include "Logger.h"
using namespace easynet;

class SFDSProtocolFactory:public IProtocolFactory
{
public:
	SFDSProtocolFactory(){m_Memory = &m_Sysmemory;}
	SFDSProtocolFactory(IMemory *memory):m_Memory(memory){}
private:
	IMemory *m_Memory;
	SystemMemory m_Sysmemory;
////////////////////////////////////
////       实现基类接口         ////
////////////////////////////////////
public:
	//能够识别二进制/文本协议的头部大小
	uint32_t HeaderSize();

	//从buffer中解码头部,并获取协议体数据.成功返回true,失败返回false.
	DecodeResult DecodeHeader(const char *buffer, DataType &type, uint32_t &body_size);

	//将头部数据编码写入到buffer(大小不能小于HeaderSize的返回值).body_size为协议体大小
	void EncodeHeader(char *buffer, uint32_t body_size);

	//解码二进制协议体数据
	DecodeResult DecodeBinBody(ProtocolContext *context);

	//解码文本协议体数据
	DecodeResult DecodeTextBody(ProtocolContext *context);

	//删除DecodeBinBody或DecodeTextBody时创建的protocol实例
	void DeleteProtocol(uint32_t protocol_type, void *protocol);
private:
	DECL_LOGGER(logger);
};

#endif //_SFDSPROTOCOLFACTORY_H_


