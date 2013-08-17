/*
 * Master.cpp
 *
 *  Created on: 2013-08-08
 *      Author: tim
 */

#include "Master.h"
#include "Socket.h"

#include "KeyDefine.h"
#include "SFDSProtocolFactory.h"


IMPL_LOGGER(Master, logger);

//配置文件
#include "ConfigReader.h"
ConfigReader *g_config_reader = NULL;
const char config_path[] = "../config/server.config";

Master::Master()
{
	m_SendTimeout = -1;
	m_ProtocolFactory = (IProtocolFactory*)new SFDSProtocolFactory(GetMemory());
}
Master::~Master()
{
	delete m_ProtocolFactory;
	delete g_config_reader;
}

bool Master::Start()
{
	//Add Your Code Here
	g_config_reader = new ConfigReader(config_path);

	//监听端口
	int32_t master_port = g_config_reader->GetValue("MasterPort", -1);
	assert(master_port != -1);

	int32_t fd = Listen(master_port);
	assert(fd > 0);
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

	//发送超时时间
	m_SendTimeout = g_config_reader->GetValue("SendTimeout", -1);
	m_SavingTaskTimeoutSec = g_config_reader->GetValue("SavingTaskTimeout", 10);

	//循环处理请求
	IEventServer *event_server = GetEventServer();
	if(event_server->AddTimer(this, 1000, true) == false)
		assert(0);
	LOG_INFO(logger, "add master timer succ. timeout_ms=1000");

	LOG_INFO(logger, "master goto run_loop...");
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
	KVData *kv_data = (KVData*)context->protocol;
	assert(kv_data != NULL);

	int32_t protocol_type;
	if(kv_data->GetValue(KEY_PROTOCOL_TYPE, protocol_type) == false)
	{
		LOG_ERROR(logger, "recv_protocol:get protocol_type failed. fd="<<fd);
		return false;
	}
	if(protocol_type<=(int32_t)PROTOCOL_BEGIN || protocol_type>=(int32_t)PROTOCOL_END)
	{
		LOG_ERROR(logger, "recv_protocol:protocol_type="<<protocol_type<<" invalid. fd="<<fd);
		return false;
	}
	LOG_DEBUG(logger, "recv_protocol:get protocol_type="<<protocol_type<<" succ. fd="<<fd);
	
	switch(protocol_type)
	{
	case PROTOCOL_FILE_INFO_REQ:    //响应FileInfo的请求
		OnFileInfoReq(fd, kv_data);
		break;
	case PROTOCOL_CHUNK_PING:       //响应chunk的ping包
		OnChunkPing(fd, kv_data);
		break;
	case PROTOCOL_FILE_INFO:        //响应chunk上报文件信息
		OnFileInfoSave(fd, kv_data);
		break;
	default:
		LOG_WARN(logger, "recv_protocol:un-expect protocol_type="<<protocol_type<<" and ignore it.");
		break;
	}
	return true;
}

void Master::OnSendSucc(int32_t fd, ProtocolContext *context)
{
	//Add Your Code Here
	LOG_DEBUG(logger, "send protocol succ on fd="<<fd<<", info='"<<context->Info<<"'");
	DeleteProtocolContext(context);
	return ;
}

void Master::OnSendError(int32_t fd, ProtocolContext *context)
{
	//Add Your Code Here
	LOG_ERROR(logger, "send protocol failed on fd="<<fd<<", info='"<<context->Info<<"'");
	DeleteProtocolContext(context);
	return ;
}

void Master::OnSendTimeout(int32_t fd, ProtocolContext *context)
{
	//Add Your Code Here
	LOG_WARN(logger, "send protocol timeout on fd="<<fd<<", info='"<<context->Info<<"'");
	DeleteProtocolContext(context);
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

IProtocolFactory* Master::GetProtocolFactory()
{
	return m_ProtocolFactory;
}

void Master::OnTimeout(uint64_t nowtime_ms)
{
	//检查超时的任务
	LOG_DEBUG(logger, "master timer timeout,check all task...");
	RemoveSavingTaskTimeout();
}

////////////////////////////////////////////////////
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

//响应chunk的ping包
void Master::OnChunkPing(int fd, KVData *kv_data)
{
	LOG_DEBUG(logger, "OnChunkPing: fd="<<fd);
	ChunkInfo chunkinfo;
	ChunkPingRsp chunkping_rsp;  //回复包

	chunkping_rsp.result = 0;    //成功
	if(kv_data->GetValue(KEY_CHUNK_ID, chunkinfo.id) == false)
	{
		LOG_ERROR(logger, "OnChunkPing: get chunk_id failed. fd="<<fd);
		chunkping_rsp.result = 1;
	}
	if(chunkping_rsp.result==0 && kv_data->GetValue(KEY_CHUNK_IP, chunkinfo.ip) == false)
	{
		LOG_ERROR(logger, "OnChunkPing: get chunk_ip failed. fd="<<fd);
		chunkping_rsp.result = 1;
	}
	if(chunkping_rsp.result==0 && kv_data->GetValue(KEY_CHUNK_PORT, chunkinfo.port) == false)
	{
		LOG_ERROR(logger, "OnChunkPing: get chunk_port failed. fd="<<fd);
		chunkping_rsp.result = 1;
	}
	if(chunkping_rsp.result==0 && kv_data->GetValue(KEY_CHUNK_DISK_SPACE, chunkinfo.disk_space) == false)
	{
		LOG_ERROR(logger, "OnChunkPing: get chunk_disk_space failed. fd="<<fd);
		chunkping_rsp.result = 1;
	}
	if(chunkping_rsp.result==0 && kv_data->GetValue(KEY_CHUNK_DISK_USED, chunkinfo.disk_used) == false)
	{
		LOG_ERROR(logger, "OnChunkPing: get chunk_disk_used failed. fd="<<fd);
		chunkping_rsp.result = 1;
	}

	if(chunkping_rsp.result == 0)
	{
		LOG_INFO(logger, "OnChunkPing: get chunk info succ. chunk_id="<<chunkinfo.id
				<<" chunk_ip="<<chunkinfo.ip
				<<" chunk_port="<<chunkinfo.port
				<<" chunk_disk_space="<<chunkinfo.disk_space
				<<" chunk_disk_used="<<chunkinfo.disk_used);
		chunkping_rsp.result = AddChunk(chunkinfo)?0:1;
		chunkping_rsp.chunk_id = chunkinfo.id;
	}

	//发送回复包
	KVData send_kvdata(true);
	send_kvdata.SetValue(KEY_CHUNK_RSP_RESULT, chunkping_rsp.result);
	send_kvdata.SetValue(KEY_CHUNK_RSP_CHUNK_ID, chunkping_rsp.chunk_id);

	//序列化数据
	ProtocolContext *send_context = NULL;
	SerializeKVData(send_kvdata, send_context, "ChunkResp");

	if(SendProtocol(fd, send_context, m_SendTimeout) == false)
	{
		LOG_ERROR(logger, "OnChunkPing: send chunk resp failed. chunk_id="<<chunkping_rsp.chunk_id<<" fd="<<fd);
		DeleteProtocolContext(send_context);
	}
}

//响应文件信息查询包
void Master::OnFileInfoReq(int fd, KVData *kv_data)
{
	LOG_DEBUG(logger, "OnFileInfoReq. fd="<<fd);
	FileInfoReq fileinfo_req;
	FileInfo fileinfo;

	fileinfo.result = (int32_t)FileInfo::RESULT_SUCC;
	if(kv_data->GetValue(KEY_FILEINFO_REQ_FID, fileinfo_req.fid) == false)
	{
		LOG_ERROR(logger, "OnFileInfoReq: get fid failed. fd="<<fd);
		fileinfo.result = (int32_t)FileInfo::RESULT_FAILED;
	}
	if(kv_data->GetValue(KEY_FILEINFO_REQ_CHUNKPATH, fileinfo_req.query_chunkpath) == false)
	{
		LOG_DEBUG(logger, "OnFileInfoReq: get query_chunkpath failed. fd="<<fd);
		fileinfo_req.query_chunkpath = 0;
	}

	if(fileinfo.result == (int32_t)FileInfo::RESULT_SUCC)
	{
		LOG_DEBUG(logger,"OnFileInfoReq: fid="<<fileinfo_req.fid<<" query_chunkpath"<<fileinfo_req.query_chunkpath<<" fd="<<fd);
		if(GetFileInfo(fileinfo_req.fid, fileinfo)) //存在
		{
			LOG_DEBUG(logger,"OnFileInfoReq: get fileinfo succ: fid="<<fileinfo.fid<<" size="<<fileinfo.size);
			int i;
			for(i=0; i<fileinfo.GetChunkPathCount(); ++i)
			{
				ChunkPath &chunk_path = fileinfo.GetChunkPath(i);
				LOG_DEBUG(logger,"OnFileInfoReq: chunk["<<i
						<<"]:id="<<chunk_path.id
						<<" ip="<<chunk_path.ip
						<<" port="<<chunk_path.port
						<<" index="<<chunk_path.index
						<<" offset="<<chunk_path.offset);
			}
		}
		else if(FindSavingTask(fileinfo_req.fid))  //正在保存
		{
			LOG_DEBUG(logger, "OnFileInfoReq: fid="<<fileinfo_req.fid<<" is saving.");
			fileinfo.result = FileInfo::RESULT_SAVING;
		}
		else if(fileinfo_req.query_chunkpath)      //请求分配chunk
		{
			fileinfo.fid = fileinfo_req.fid;
			fileinfo.name = ""; //无效
			fileinfo.size = 0;  //无效

			ChunkPath chunk_path;
			ChunkInfo chunk_info;
			if(DispatchChunk(chunk_info))  //分配chunk
			{
				chunk_path.id = chunk_info.id;
				chunk_path.ip = chunk_info.ip;
				chunk_path.port = chunk_info.port;
				chunk_path.index = 0;  //无效
				chunk_path.offset = 0; //无效
				fileinfo.AddChunkPath(chunk_path);

				AddSavingTask(fileinfo.fid);
				LOG_DEBUG(logger, "OnFileInfoReq: dispatch chunk[id="<<chunk_info.id<<" ip="<<chunk_info.ip<<" port="<<chunk_info.port<<"] for fid="<<fileinfo.fid);

				fileinfo.result = FileInfo::RESULT_CHUNK;
			}
			else
			{
				LOG_WARN(logger,"OnFileInfoReq: get chunk failed for fid="<<fileinfo.fid);
				fileinfo.result = FileInfo::RESULT_FAILED;
			}
		}
		else
		{
			LOG_WARN(logger, "OnFileInfoReq: get file_info failed for fid="<<fileinfo.fid);
			fileinfo.result = FileInfo::RESULT_FAILED;
		}
	}

	//发送回复包
	KVData send_kvdata(true);
	//设置结果
	send_kvdata.SetValue(KEY_FILEINFO_RSP_RESULT, fileinfo.result);
	//设置文件名和大小
	if(fileinfo.result == FileInfo::RESULT_SUCC)
	{
		send_kvdata.SetValue(KEY_FILEINFO_RSP_FILE_NAME, fileinfo.name);
		send_kvdata.SetValue(KEY_FILEINFO_RSP_FILE_SIZE, fileinfo.size);
	}

	KVData chunkpath_kvdata[3];
	chunkpath_kvdata[0].NetTrans(true);
	chunkpath_kvdata[1].NetTrans(true);
	chunkpath_kvdata[2].NetTrans(true);

	//设置chunk path
	if(fileinfo.result == FileInfo::RESULT_SUCC || fileinfo.result == FileInfo::RESULT_CHUNK)
	{
		uint32_t chunkpath_count = 0;
		chunkpath_count = fileinfo.GetChunkPathCount();
		assert(chunkpath_count > 0);
		if(chunkpath_count > 3)  //最多只有3个
			chunkpath_count = 3;

		send_kvdata.SetValue(KEY_FILEINFO_RSP_CHUNK_NUM, chunkpath_count);
		for(int32_t i=0; i<chunkpath_count; ++i)
		{
			char buffer[1024];
			ChunkPath &chunkpath = fileinfo.GetChunkPath(i);
			chunkpath_kvdata[i].SetValue(KEY_FILEINFO_RSP_CHUNK_ID, chunkpath.id);
			chunkpath_kvdata[i].SetValue(KEY_FILEINFO_RSP_CHUNK_IP, chunkpath.ip);
			chunkpath_kvdata[i].SetValue(KEY_FILEINFO_RSP_CHUNK_PORT, chunkpath.port);
			chunkpath_kvdata[i].SetValue(KEY_FILEINFO_RSP_CHUNK_INDEX, chunkpath.index);
			chunkpath_kvdata[i].SetValue(KEY_FILEINFO_RSP_CHUNK_OFFSET, chunkpath.offset);
			//设置子结构
			send_kvdata.SetValue(KEY_FILEINFO_RSP_CHUNK_PATH0+i, &chunkpath_kvdata[i]);
		}
	}

	//序列化数据
	ProtocolContext *send_context = NULL;
	SerializeKVData(send_kvdata, send_context, "FileInfoResp");

	if(SendProtocol(fd, send_context, m_SendTimeout) == false)
	{
		LOG_ERROR(logger, "OnFileInfoReq: send FileInfo resp failed. fid="<<fileinfo_req.fid<<" fd="<<fd);
		DeleteProtocolContext(send_context);
	}
}

//响应chunk发送fileinfo保存包
void Master::OnFileInfoSave(int fd, KVData *kv_data)
{
	LOG_DEBUG(logger, "OnFileInfo. fd="<<fd);
	FileInfo fileinfo;
	FileInfoSaveResult saveresult;

	if(kv_data->GetValue(KEY_FILEINFO_SAVE_RESULT, fileinfo.result) == false)
	{
		LOG_ERROR(logger,"OnFileInfo: get result failed. fd="<<fd);
		return ;
	}
	if(kv_data->GetValue(KEY_FILEINFO_SAVE_FID, fileinfo.fid) == false)
	{
		LOG_ERROR(logger,"OnFileInfo: get fid failed. fd="<<fd);
		return ;
	}

	if(fileinfo.result == (int32_t)FileInfo::RESULT_FAILED)
	{
		LOG_INFO(logger,"OnFileInfo: chunk save file failed. remove saving taskfid="<<fileinfo.fid<<" fd="<<fd);
		RemoveSavingTask(fileinfo.fid);
		return ;
	}

	saveresult.fid = fileinfo.fid;

	if(FindSavingTask(fileinfo.fid))    //是否有正在保存的记录
	{
		ChunkPath chunk_path;
		if(kv_data->GetValue(KEY_FILEINFO_SAVE_FILE_NAME, fileinfo.name) == false)
		{
			LOG_ERROR(logger, "OnFileInfo: cant't get file_name. fid="<<fileinfo.fid<<" fd="<<fd);
			saveresult.result = FileInfoSaveResult::RESULT_FAILED;
		}
		else if(kv_data->GetValue(KEY_FILEINFO_SAVE_FILE_SIZE, fileinfo.size) == false)
		{
			LOG_ERROR(logger, "OnFileInfo: cant't get file_size. fid="<<fileinfo.fid<<" fd="<<fd);
			saveresult.result = FileInfoSaveResult::RESULT_FAILED;
		}
		else if(kv_data->GetValue(KEY_FILEINFO_SAVE_CHUNK_ID, chunk_path.id) == false)
		{
			LOG_ERROR(logger, "OnFileInfo: cant't get chunk_id. fid="<<fileinfo.fid<<" fd="<<fd);
			saveresult.result = FileInfoSaveResult::RESULT_FAILED;
		}
		else if(kv_data->GetValue(KEY_FILEINFO_SAVE_CHUNK_IP, chunk_path.ip) == false)
		{
			LOG_ERROR(logger, "OnFileInfo: cant't get chunk_ip. fid="<<fileinfo.fid<<" fd="<<fd);
			saveresult.result = FileInfoSaveResult::RESULT_FAILED;
		}
		else if(kv_data->GetValue(KEY_FILEINFO_SAVE_CHUNK_PORT, chunk_path.port) == false)
		{
			LOG_ERROR(logger, "OnFileInfo: cant't get chunk_port. fid="<<fileinfo.fid<<" fd="<<fd);
			saveresult.result = FileInfoSaveResult::RESULT_FAILED;
		}
		else if(kv_data->GetValue(KEY_FILEINFO_SAVE_CHUNK_INDEX, chunk_path.index) == false)
		{
			LOG_ERROR(logger, "OnFileInfo: cant't get chunk_index. fid="<<fileinfo.fid<<" fd="<<fd);
			saveresult.result = FileInfoSaveResult::RESULT_FAILED;
		}
		else if(kv_data->GetValue(KEY_FILEINFO_SAVE_CHUNK_OFFSET, chunk_path.offset) == false)
		{
			LOG_ERROR(logger, "OnFileInfo: cant't get chunk_offset. fid="<<fileinfo.fid<<" fd="<<fd);
			saveresult.result = FileInfoSaveResult::RESULT_FAILED;
		}
		else
		{
			LOG_DEBUG(logger, "OnFileInfo: recv fileinfo succ. fid="<<fileinfo.fid
					<<" chunk_id"<<chunk_path.id
					<<" chunk_ip"<<chunk_path.ip
					<<" chunk_port"<<chunk_path.port
					<<" chunk_index"<<chunk_path.index
					<<" chunk_offset"<<chunk_path.offset);
			fileinfo.AddChunkPath(chunk_path);

			//添加到cache
			m_FileInfoCache.insert(std::make_pair(fileinfo.fid, fileinfo));
			//保存到数据库
			if(DBSaveFileInfo(fileinfo) == false)
			{
				LOG_WARN(logger, "OnFileInfo: save to db failed. fid="<<fileinfo.fid<<" fd="<<fd);
			}
			//从正在保存的任务中移除
			RemoveSavingTask(fileinfo.fid);

			saveresult.result = FileInfoSaveResult::RESULT_SUCC;
		}
	}
	else
	{
		LOG_WARN(logger, "OnFileInfo: can't find saving task. fid="<<fileinfo.fid<<". fd="<<fd);
		saveresult.result = FileInfoSaveResult::RESULT_FAILED;
	}

	//发送回复包
	KVData send_kvdata(true);
	send_kvdata.SetValue(KEY_FILEINFO_SAVE_RSP_RESULT, saveresult.result);
	send_kvdata.SetValue(KEY_FILEINFO_SAVE_RSP_FID, saveresult.fid);

	//序列化数据
	ProtocolContext *send_context = NULL;
	SerializeKVData(send_kvdata, send_context, "FileInfoSaveResp");

	if(SendProtocol(fd, send_context, m_SendTimeout) == false)
	{
		LOG_ERROR(logger, "OnFileInfo: send FileInfo resp failed. fid="<<saveresult.fid<<" fd="<<fd);
		DeleteProtocolContext(send_context);
	}
}

/////////////////////////////////////////////////
bool Master::AddChunk(ChunkInfo &chunkinfo)
{
	map<string, ChunkInfo>::iterator it = m_ChunkManager.find(chunkinfo.id);
	if(it == m_ChunkManager.end())
	{
		std:pair<map<string, ChunkInfo>::iterator, bool> ret = m_ChunkManager.insert(std::make_pair(chunkinfo.id, chunkinfo));
		if(ret.second == false)
		{
			LOG_ERROR(logger, "insert new chunk to map failed. chunk_id="<<chunkinfo.id);
			return false;
		}
		else
		{
			LOG_DEBUG(logger, "insert new chunk to map succ. chunk_id="<<chunkinfo.id);
			return true;
		}
	}

	//已经存在
	it->second = chunkinfo;
	LOG_DEBUG(logger, "update chunk info. chunk_id="<<chunkinfo.id);
	return true;
}

bool Master::DispatchChunk(ChunkInfo &chunkinfo)
{
	map<string, ChunkInfo>::iterator it = m_ChunkManager.begin();
	if(it != m_ChunkManager.end())
	{
		chunkinfo = it->second;
		return true;
	}
	return false;
}

bool Master::GetFileInfo(string &fid, FileInfo &fileinfo)
{
	//查找cache
	map<string, FileInfo>::iterator it = m_FileInfoCache.find(fid);
	if(it != m_FileInfoCache.end())
	{
		fileinfo = it->second;
		return true;
	}
	//查找数据库
	if(m_DBConnection == NULL)
		return false;

	char sql_str[1024];
	snprintf(sql_str, 1024, "select fid,name,size,chunkid,chunkip,chunkport,findex,foffset from SFS.fileinfo_%s where fid='%s'"
							,fid.substr(0,2).c_str(), fid.c_str());
	Query query = m_DBConnection->query(sql_str);
	StoreQueryResult res = query.store();
	if (!res || res.empty())
		return false;

	size_t i;
	for(i=0; i<res.num_rows(); ++i)
	{
		ChunkPath chunk_path;
		fileinfo.fid      = res[i]["fid"].c_str();
		fileinfo.name     = res[i]["name"].c_str();
		fileinfo.size     = atoi(res[i]["size"].c_str());
		chunk_path.id     = res[i]["chunkid"].c_str();
		chunk_path.ip     = res[i]["chunkip"].c_str();
		chunk_path.port   = atoi(res[i]["chunkport"].c_str());
		chunk_path.index  = atoi(res[i]["findex"].c_str());
		chunk_path.offset = atoi(res[i]["foffset"].c_str());

		fileinfo.AddChunkPath(chunk_path);
	}
	//添加到cache
	m_FileInfoCache.insert(std::make_pair(fileinfo.fid, fileinfo));

	return true;
}

bool Master::FindSavingTask(string &fid)
{
	return m_SavingTaskMap.find(fid) != m_SavingTaskMap.end();
}

bool Master::AddSavingTask(string &fid)
{
	if(FindSavingTask(fid))  //已经存在
	{
		LOG_WARN(logger, "saving taskalready exists, fid="<<fid);
		return true;
	}

	SavingFid saving_fid;
	saving_fid.insert_time = (int)time(NULL);
	saving_fid.fid = fid;
	m_SavingFidList.push_front(saving_fid);  //保存到list头
	m_SavingTaskMap.insert(std::make_pair(fid, m_SavingFidList.begin()));  //保存到map

	LOG_INFO(logger, "add saving task:fid="<<fid<<" insert_time="<<saving_fid.insert_time);
	return true;
}

bool Master::RemoveSavingTask(string &fid)
{
	map<string, list<SavingFid>::iterator>::iterator it = m_SavingTaskMap.find(fid);
	if(it == m_SavingTaskMap.end())  //不存在
		return true;
	m_SavingFidList.erase(it->second);
	m_SavingTaskMap.erase(it);
	return true;
}

void Master::RemoveSavingTaskTimeout()
{
	int now = (int)time(NULL);
	list<SavingFid>::iterator it;
	while(m_SavingFidList.size() > 0)
	{
		it = m_SavingFidList.end();
		--it;
		if(now-it->insert_time < m_SavingTaskTimeoutSec)
			break;
		LOG_DEBUG(logger, "saving task timeout and delete:fid="<<it->fid<<" instert_time="<<it->insert_time<<" now="<<now);
		m_SavingTaskMap.erase(it->fid);
		m_SavingFidList.erase(it);
	}
}

bool Master::DBSaveFileInfo(FileInfo &fileinfo)
{
	if(m_DBConnection == NULL)
		return false;
	char sql_str[1024];
	ChunkPath &chunk_path = fileinfo.GetChunkPath(0);
	snprintf(sql_str, 1024, "insert into SFS.fileinfo_%s (fid, name, size, chunkid, chunkip, chunkport, findex, foffset) "
			"values('%s', '%s', %d, '%s', '%s', %d, %d, %d);"
			,fileinfo.fid.substr(0,2).c_str(), fileinfo.fid.c_str(), fileinfo.name.c_str(), fileinfo.size
			,chunk_path.id.c_str() ,chunk_path.ip.c_str(), chunk_path.port
			,chunk_path.index, chunk_path.offset);
	Query query = m_DBConnection->query(sql_str);
	return query.exec();
}
