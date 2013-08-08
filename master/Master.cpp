/*
 * Master.cpp
 *
 *  Created on: 2013-08-08
 *      Author: tim
 */

#include "Master.h"
#include "Socket.h"
IMPL_LOGGER(Master, logger);

//配置文件
#include "ConfigReader.h"
ConfigReader *g_config_reader = NULL;
const char config_path[] = "config/server.config";


bool Master::Start()
{
	//Add Your Code Here
	g_config_reader = new ConfigReader(config_path);

	//监听端口
	int32_t master_port = g_config_reader->GetValue("MasterPort", -1);
	assert(master_port != -1);

	bool ret = Listen(master_port);
	assert(ret == true);
	LOG_INFO(logger, "Master Server listen on port="<<master_port<<" succ.");

	//数据库
	m_DBConnection = NULL;
	m_DBIP     = g_config_reader->GetValue("DBIP", "");
	m_DBPort   = g_config_reader->GetValue("DBPort", 0);
	m_DBUser   = g_config_reader->GetValue("DBUser", "");
	m_DBPasswd = g_config_reader->GetValue("DBPassword", "");
	m_DBName   = g_config_reader->GetValue("DBName", "");
	if(m_DBIP!="" && m_DBUser!="" && m_DBPasswd!="" && m_DBName!="")
	{
		m_DBConnection = new Connection(m_DBName.c_str(), m_DBIP.c_str(), m_DBUser.c_str(), m_DBPasswd.c_str(), m_DBPort);
		if(!m_DBConnection->connected())
		{
			LOG_ERROR(logger, "connect DB error. db="<<m_DBName<<" ip="<<m_DBIP<<" port="<<m_DBPort<<" user="<<m_DBUser<<" pwd="<<m_DBPasswd);
			delete m_DBConnection;
			assert(0);
		}
	}
	else
	{
		LOG_ERROR(logger, "DB parameters is invalid !!!");
		assert(0);
	}

	//循环处理请求
	IEventServer *event_server = GetEventServer();
	event_server->RunLoop();

	return true;
}

int32_t Master::GetSocketRecvTimeout()
{
	return -1;
}

int32_t Master::GetSocketIdleTimeout()
{
	return 3000;
}

int32_t Master::GetMaxConnections()
{
	return 1000;
}

bool Master::OnReceiveProtocol(int32_t fd, ProtocolContext *context, bool &detach_context)
{
	//Add Your Code Here
	LOG_DEBUG(logger, "receive protocol on fd="<<fd);
	
	return true;
}

void Master::OnSendSucc(int32_t fd, ProtocolContext *context)
{
	//Add Your Code Here
	LOG_DEBUG(logger, "send protocol succ on fd="<<fd<<", info='"<<context->Info<<"'");
	
	return ;
}

void Master::OnSendError(int32_t fd, ProtocolContext *context)
{
	//Add Your Code Here
	LOG_ERROR(logger, "send protocol failed on fd="<<fd<<", info='"<<context->Info<<"'");
	
	return ;
}

void Master::OnSendTimeout(int32_t fd, ProtocolContext *context)
{
	//Add Your Code Here
	LOG_WARN(logger, "send protocol timeout on fd="<<fd<<", info='"<<context->Info<<"'");
	
	return ;
}

void Master::OnSocketFinished(int32_t fd)
{
	//Add Your Code Here
	LOG_INFO(logger, "socket finished. fd="<<fd);
	
	//close it?
	Socket::Close(fd);

	return ;
}
