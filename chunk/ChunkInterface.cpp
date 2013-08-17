/*
 * ChunkInterface.cpp
 *
 *  Created on: 2013-08-14
 *      Author: tim
 */

#include "ChunkInterface.h"
#include "SFDSProtocolFactory.h"
#include "DiskMgr.h"
#include "Socket.h"
#include "KeyDefine.h"

//配置文件
#include "ConfigReader.h"
using namespace easynet;
ConfigReader *g_config_reader = NULL;
const char config_path[] = "../config/server.config";

IMPL_LOGGER(ChunkInterface, logger);

ChunkInterface::ChunkInterface()
{
	m_ProtocolFactory = (IProtocolFactory*)new SFDSProtocolFactory(GetMemory());
}
ChunkInterface::~ChunkInterface()
{
	delete m_ProtocolFactory;
}

bool ChunkInterface::Start()
{
	//Add Your Code Here
	g_config_reader = new ConfigReader(config_path);

	//初始化磁盘管理器
	DiskMgr::GetInstance()->Init(g_config_reader);

	//创建线程
	m_ChunkWorkerNum = g_config_reader->GetValue("ChunkWorkerNum", -1);
	assert(m_ChunkWorkerNum > 0);
	m_CurThreadIndex = 0;
	for(int i=0; i<m_ChunkWorkerNum; ++i)
	{
		ChunkWorker *chunk_worker = new ChunkWorker(g_config_reader);
		(Thread*)(chunk_worker)->StartThread();
		m_ChunkWorkers.push_back(chunk_worker);
	}

	//监听端口
	int32_t chunk_port = g_config_reader->GetValue("ChunkPort", -1);
	assert(chunk_port != -1);
	int32_t fd = Listen(chunk_port);
	assert(fd > 0);
	LOG_INFO(logger, "Chunk Server listen on port="<<chunk_port<<" succ. fd="<<fd);


	//连接到master
	string master_ip = g_config_reader->GetValue("MasterIP", "");
	assert(master_ip.size() > 0);
	int32_t master_port = g_config_reader->GetValue("MasterPort", -1);
	assert(master_port > 0);
	m_MasterSocket = -1;
	m_MasterSocket = Socket::Connect(master_port, master_ip.c_str(), false, 2000);
	assert(m_MasterSocket > 0);
	LOG_INFO(logger, "connect to master succ. master_ip="<<master_ip<<" master port="<<master_port<<" fd="<<m_MasterSocket);

	IEventServer *event_server = GetEventServer();

	//注册定时发送ping包时钟
	if(event_server->AddTimer(this, 1000, true) == false)
		assert(0);

	event_server->RunLoop();

	return true;
}

int32_t ChunkInterface::GetSocketRecvTimeout()
{
	return -1;
}

int32_t ChunkInterface::GetSocketIdleTimeout()
{
	return -1;
}

int32_t ChunkInterface::GetMaxConnections()
{
	return 1000;
}

bool ChunkInterface::OnReceiveProtocol(int32_t fd, ProtocolContext *context, bool &detach_context)
{
	//Add Your Code Here
	LOG_DEBUG(logger, "receive protocol on fd="<<fd);
	KVData *kvdata = (KVData*)context->protocol;
	ChunkPingRsp ping_resp;
	if(kvdata->GetValue(KEY_CHUNK_RSP_RESULT, ping_resp.result) == false)
	{
		LOG_ERROR(logger, "handle ChunkPingResp: get result failed. fd="<<fd);
		return false;
	}
	if(kvdata->GetValue(KEY_CHUNK_RSP_RESULT, ping_resp.chunk_id) == false)
	{
		LOG_ERROR(logger, "handle ChunkPingResp: get chunk_id failed. fd="<<fd);
		return false;
	}

	LOG_INFO(logger, "handle ChunkPingResp: result="<<ping_resp.result<<", chunk_id="<<ping_resp.chunk_id<<", fd="<<fd);
	return true;
}

void ChunkInterface::OnSendSucc(int32_t fd, ProtocolContext *context)
{
	//Add Your Code Here
	LOG_DEBUG(logger, "send protocol succ on fd="<<fd<<", info='"<<context->Info<<"'");
	DeleteProtocolContext(context);
	return ;
}

void ChunkInterface::OnSendError(int32_t fd, ProtocolContext *context)
{
	//Add Your Code Here
	LOG_ERROR(logger, "send protocol failed on fd="<<fd<<", info='"<<context->Info<<"'");
	DeleteProtocolContext(context);
	return ;
}

void ChunkInterface::OnSendTimeout(int32_t fd, ProtocolContext *context)
{
	//Add Your Code Here
	LOG_WARN(logger, "send protocol timeout on fd="<<fd<<", info='"<<context->Info<<"'");
	DeleteProtocolContext(context);
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
	LOG_INFO(logger, "send fd="<<fd<<" to Index="<<m_CurThreadIndex);
	m_ChunkWorkers[m_CurThreadIndex]->SendMessage(fd);
	m_CurThreadIndex = (m_CurThreadIndex+1)%2;
	return true;
}

#define SerializeKVData(kvdata, send_context, info)  do{  \
send_context = NewProtocolContext();  \
assert(send_context != NULL);  \
send_context->type = DTYPE_BIN;  \
send_context->Info = info;  \
uint32_t header_size = m_ProtocolFactory->HeaderSize();  \
uint32_t body_size = kvdata.Size();  \
send_context->CheckSize(header_size+body_size);  \
m_ProtocolFactory->EncodeHeader(send_context->Buffer, body_size);  \
send_kvdata.Serialize(send_context->Buffer+header_size);  \
send_context->Size = header_size+body_size;  \
}while(0)

void ChunkInterface::OnTimeout(uint64_t nowtime_ms)
{
	DiskMgr::GetInstance()->Update(); //更新磁盘信息

	//发送ping包到master
	ChunkInfo chunk_info;
	chunk_info.id = g_config_reader->GetValue("ChunkID", ""); //chunk ID
	chunk_info.ip = g_config_reader->GetValue("ChunkIP", ""); //chunk ip
	chunk_info.port = g_config_reader->GetValue("ChunkPort", -1); //chunk端口
	assert(chunk_info.ip!="" && chunk_info.id!="" && chunk_info.port!=-1);
	DiskMgr::GetInstance()->GetDiskSpace(chunk_info.disk_space, chunk_info.disk_used); //磁盘空间信息

	//序列化
	KVData send_kvdata(true);
	send_kvdata.SetValue(KEY_PROTOCOL_TYPE, PROTOCOL_CHUNK_PING);
	send_kvdata.SetValue(KEY_CHUNK_ID, chunk_info.id);
	send_kvdata.SetValue(KEY_CHUNK_IP, chunk_info.ip);
	send_kvdata.SetValue(KEY_CHUNK_PORT, chunk_info.port);
	send_kvdata.SetValue(KEY_CHUNK_DISK_SPACE, chunk_info.disk_space);
	send_kvdata.SetValue(KEY_CHUNK_DISK_USED, chunk_info.disk_used);

	ProtocolContext *send_context = NULL;
	SerializeKVData(send_kvdata, send_context, "ChunkPing");

	if(!SendProtocol(m_MasterSocket, send_context))
	{
		LOG_ERROR(logger, "send chunk_ping to master failed.");
		DeleteProtocolContext(send_context);
	}
	else
	{
		LOG_DEBUG(logger, "send chunk_ping data to framework succ. chunk_id="<<chunk_info.id
			<<", chunk_ip="<<chunk_info.ip
			<<", chunk_port="<<chunk_info.port
			<<", disk_space="<<chunk_info.disk_space
			<<", disk_used="<<chunk_info.disk_used);
	}
}
