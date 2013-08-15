/*
 * ChunkInterface.cpp
 *
 *  Created on: 2013-08-14
 *      Author: tim
 */

#include "ChunkInterface.h"

//配置文件
#include "ConfigReader.h"
ConfigReader g_config_reader;
const char config_path[] = "config/server.config";

IMPL_LOGGER(ChunkInterface, logger);

bool ChunkInterface::Start()
{
	//Add Your Code Here
	if(g_config_reader.Init(config_path) == false)
		assert(0);

	//创建线程
	m_ChunkWorkerNum = g_config_reader.GetValue("ChunkWorkerNum", -1);
	assert(m_ChunkWorkerNum > 0);
	m_ChunkWorker = new ChunkWorker[m_ChunkWorkerNum](&g_config_reader);
	m_CurThreadIndex = 0;
	for(int i=0; i<m_ChunkWorkerNum; ++i)
		(Thread*)(m_ChunkWorker+i)->StartThread();

	//监听端口
	int32_t chunk_port = g_config_reader.GetValue("ChunkPort", -1);
	assert(chunk_port != -1);
	int32_t fd = Listen(chunk_port);
	assert(fd == true);
	LOG_INFO(logger, "Chunk Server listen on port="<<chunk_port<<" succ.");

	IEventServer *event_server = GetEventServer();
	event_server->RunLoop();

	return true;
}

int32_t ChunkInterface::GetSocketRecvTimeout()
{
	return -1;
}

int32_t ChunkInterface::GetSocketIdleTimeout()
{
	return 3000;
}

int32_t ChunkInterface::GetMaxConnections()
{
	return 1000;
}

bool ChunkInterface::OnReceiveProtocol(int32_t fd, ProtocolContext *context, bool &detach_context)
{
	//Add Your Code Here
	LOG_DEBUG(logger, "receive protocol on fd="<<fd);
	
	return true;
}

void ChunkInterface::OnSendSucc(int32_t fd, ProtocolContext *context)
{
	//Add Your Code Here
	LOG_DEBUG(logger, "send protocol succ on fd="<<fd<<", info='"<<context->Info<<"'");
	
	return ;
}

void ChunkInterface::OnSendError(int32_t fd, ProtocolContext *context)
{
	//Add Your Code Here
	LOG_ERROR(logger, "send protocol failed on fd="<<fd<<", info='"<<context->Info<<"'");
	
	return ;
}

void ChunkInterface::OnSendTimeout(int32_t fd, ProtocolContext *context)
{
	//Add Your Code Here
	LOG_WARN(logger, "send protocol timeout on fd="<<fd<<", info='"<<context->Info<<"'");
	
	return ;
}

void ChunkInterface::OnSocketFinished(int32_t fd)
{
	//Add Your Code Here
	LOG_INFO(logger, "socket finished. fd="<<fd);
	
	//close it?
	//Socket::Close(fd);

	return ;
}

bool ChunkInterface::AcceptNewConnect(int32_t fd)
{
	LOG_INFO(logger, "send fd="<<fd<<" to Index="<<m_Index);
	m_ChunkWorker[m_CurThreadIndex].SendMessage(fd);
	m_CurThreadIndex = (m_CurThreadIndex+1)%2;
	return true;
}
