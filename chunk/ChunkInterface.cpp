/*
 * ChunkInterface.cpp
 *
 *  Created on: 2013-08-14
 *      Author: tim
 */

#include "ChunkInterface.h"

IMPL_LOGGER(ChunkInterface, logger);

bool ChunkInterface::Start()
{
	//Add Your Code Here
	
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
