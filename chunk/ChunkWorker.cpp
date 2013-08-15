/*
 * ChunkWorker.cpp
 *
 *  Created on: 2013-08-14
 *      Author: tim
 */

#include "ChunkWorker.h"
#include "SFDSProtocolFactory.h"

IMPL_LOGGER(ChunkWorker, logger);

ChunkWorker::ChunkWorker(ConfigReader *config):m_Config(config)
{
	m_ProtocolFactory = (IProtocolFactory*)new SFDSProtocolFactory(GetMemory());
}

bool ChunkWorker::Start()
{
	//Add Your Code Here
	assert(m_Config != NULL);
	
	m_MasterIP = m_Config->GetValue("MasterIP", "");
	assert(m_MasterIP.size() > 0);
	m_MasterPort = m_Config->GetValue("MasterPort", -1);
	assert(m_MasterPort > 0);
	m_MasterSocket = -1;

	IEventServer *event_server = GetEventServer();
	event_server->RunLoop();
	return true;
}

int32_t ChunkWorker::GetSocketRecvTimeout()
{
	return -1;
}

int32_t ChunkWorker::GetSocketIdleTimeout()
{
	return 3000;
}

int32_t ChunkWorker::GetMaxConnections()
{
	return 1000;
}

bool ChunkWorker::OnReceiveProtocol(int32_t fd, ProtocolContext *context, bool &detach_context)
{
	//Add Your Code Here
	LOG_DEBUG(logger, "receive protocol on fd="<<fd);
	
	return true;
}

void ChunkWorker::OnSendSucc(int32_t fd, ProtocolContext *context)
{
	//Add Your Code Here
	LOG_DEBUG(logger, "send protocol succ on fd="<<fd<<", info='"<<context->Info<<"'");
	
	return ;
}

void ChunkWorker::OnSendError(int32_t fd, ProtocolContext *context)
{
	//Add Your Code Here
	LOG_ERROR(logger, "send protocol failed on fd="<<fd<<", info='"<<context->Info<<"'");
	
	return ;
}

void ChunkWorker::OnSendTimeout(int32_t fd, ProtocolContext *context)
{
	//Add Your Code Here
	LOG_WARN(logger, "send protocol timeout on fd="<<fd<<", info='"<<context->Info<<"'");
	
	return ;
}

void ChunkWorker::OnSocketFinished(int32_t fd)
{
	//Add Your Code Here
	LOG_INFO(logger, "socket finished. fd="<<fd);
	
	//close it?
	//Socket::Close(fd);

	return ;
}


/////////////////////////////////////////
void ChunkWorker::DoRun()
{
	Start();
}

IProtocolFactory* ChunkWorker::GetProtocolFactory()
{
	return m_ProtocolFactory;
}

int32_t ChunkWorker::GetMasterConnect()
{
	if(m_MasterSocket == -1)
		m_MasterSocket = Socket::Connect(m_MasterPort, m_MasterIP.c_str(), false);
	assert(m_MasterSocket != -1);
	return m_MasterSocket;
}
