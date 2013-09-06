/*
 * ChunkWorker.cpp
 *
 *  Created on: 2013-08-14
 *      Author: tim
 */

#include "ChunkWorker.h"
#include "SFDSProtocolFactory.h"
#include "KeyDefine.h"
#include "Protocol.h"
#include "Socket.h"
#include "DiskMgr.h"


#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>

IMPL_LOGGER(ChunkWorker, logger);

#define SerializeKVData(kvdata, send_context, info)  do{  \
send_context = NewProtocolContext();  \
assert(send_context != NULL);  \
send_context->type = DTYPE_BIN;  \
send_context->Info = info;  \
uint32_t header_size = m_ProtocolFactory->HeaderSize();  \
uint32_t body_size = kvdata.Size();  \
send_context->CheckSize(header_size+body_size);  \
m_ProtocolFactory->EncodeHeader(send_context->Buffer, body_size);  \
kvdata.Serialize(send_context->Buffer+header_size);  \
send_context->Size = header_size+body_size;  \
}while(0)

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

	//监听消息通知
	ListenMessage();

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
	case PROTOCOL_FILE:  //响应client请求存储文件
		OnSaveFile(fd, kv_data);
		break;
	case PROTOCOL_FILE_REQ:        //响应client请求获取文件
		OnGetFile(fd, kv_data);
		break;
	case PROTOCOL_FILE_INFO_SAVE_RESULT:  //master回复保存文件信息结果
		OnFileInfoSaveResult(fd, kv_data);
		break;
	default:
		LOG_WARN(logger, "recv_protocol:un-expect protocol_type="<<protocol_type<<" and ignore it.");
		break;
	}
	
	return true;
}

void ChunkWorker::OnSendSucc(int32_t fd, ProtocolContext *context)
{
	//Add Your Code Here
	LOG_DEBUG(logger, "send protocol succ on fd="<<fd<<", info='"<<context->Info<<"'");
	DeleteProtocolContext(context);
	return ;
}

void ChunkWorker::OnSendError(int32_t fd, ProtocolContext *context)
{
	//Add Your Code Here
	LOG_ERROR(logger, "send protocol failed on fd="<<fd<<", info='"<<context->Info<<"'");
	DeleteProtocolContext(context);
	return ;
}

void ChunkWorker::OnSendTimeout(int32_t fd, ProtocolContext *context)
{
	//Add Your Code Here
	LOG_WARN(logger, "send protocol timeout on fd="<<fd<<", info='"<<context->Info<<"'");
	DeleteProtocolContext(context);
	return ;
}

void ChunkWorker::OnSocketFinished(int32_t fd)
{
	//Add Your Code Here
	LOG_INFO(logger, "socket finished. fd="<<fd);
	
	//close it?
	Socket::Close(fd);

	if(fd == m_MasterSocket)
		m_MasterSocket = -1;
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
	{
		m_MasterSocket = Socket::Connect(m_MasterPort, m_MasterIP.c_str(), false);
		assert(m_MasterSocket != -1);
		IEventServer *event_server = GetEventServer();
		IEventHandler *event_handler = GetTransHandler();
		if(!event_server->SetEvent(m_MasterSocket, ET_PER_RD, event_handler, -1))
		{
			LOG_ERROR(logger, "add master fd to event_server failed.");
			assert(0);
		}
	}

	return m_MasterSocket;
}

void ChunkWorker::OnSaveFile(int fd, KVData *kv_data)
{
	FileData file_data;

	if(kv_data->GetValue(KEY_FILEDATA_FLAG, file_data.flag) == false)
	{
		LOG_ERROR(logger, "ONSaveFile: get flag failed.");
		return ;
	}
	if(kv_data->GetValue(KEY_FILEDATA_FID, file_data.fid) == false)
	{
		LOG_ERROR(logger, "ONSaveFile: get flag failed.");
		return ;
	}
	if(kv_data->GetValue(KEY_FILEDATA_FILE_NAME, file_data.name) == false)
	{
		LOG_ERROR(logger, "ONSaveFile: get flag failed.");
		return ;
	}
	if(kv_data->GetValue(KEY_FILEDATA_FILE_SIZE, file_data.filesize) == false)
	{
		LOG_ERROR(logger, "ONSaveFile: get flag failed.");
		return ;
	}

	KVData kvdata(true);

	switch(file_data.flag)
	{
		case FileData::FLAG_START:  //请求开始传输文件
		{
			LOG_INFO(logger, "receive FileData: flag="<<file_data.flag
						<<"(data_start),fid="<<file_data.fid
						<<",name="<<file_data.name
						<<",filesize="<<file_data.filesize);

			FileSaveResult save_result;
			save_result.fid = file_data.fid;
			if(FileTaskFind(file_data.fid))  //已经存在
			{
				LOG_WARN(logger, "file task already started. fid="<<file_data.fid);
				save_result.status = FileSaveResult::CREATE_FAILED;
			}
			else if(FileTaskCreate(fd, file_data) == false)  //创建任务失败
			{
				LOG_ERROR(logger, "create file task failed. fid="<<file_data.fid);
				save_result.status = FileSaveResult::CREATE_FAILED;
				//向master上报保存失败
				SendFailFileInfoToMaster(file_data.fid);
			}
			else  //成功
			{
				LOG_INFO(logger, "create file task succ. fid="<<file_data.fid<<",name="<<file_data.name<<",size="<<file_data.filesize);
				save_result.status = FileSaveResult::CREATE_SUCC;
			}

			kvdata.SetValue(KEY_PROTOCOL_TYPE, PROTOCOL_FILE_SAVE_RESULT);
			kvdata.SetValue(KEY_FILEDATA_RESULT, save_result.status);
			kvdata.SetValue(KEY_FILEDATA_FID, save_result.fid);

			break;
		}
		case FileData::FLAG_SEG:  //文件分片
		{
			if(kv_data->GetValue(KEY_FILEDATA_INDEX, file_data.index) == false)
			{
				LOG_ERROR(logger, "ONSaveFile: get data seg index failed.");
				return ;
			}
			if(kv_data->GetValue(KEY_FILEDATA_OFFSET, file_data.offset) == false)
			{
				LOG_ERROR(logger, "ONSaveFile: get data seg offset failed.");
				return ;
			}
			if(kv_data->GetValue(KEY_FILEDATA_SEG_SIZE, file_data.size) == false)
			{
				LOG_ERROR(logger, "ONSaveFile: get data seg size failed.");
				return ;
			}
			uint32_t data_len;
			if(kv_data->GetValue(KEY_FILEDATA_DATA, file_data.data, data_len) == false)
			{
				LOG_ERROR(logger, "ONSaveFile: get sed data failed.");
				return ;
			}

			LOG_INFO(logger, "receive FileData: flag="<<file_data.flag
						<<"(data_seg),fid="<<file_data.fid
						<<",name="<<file_data.name
						<<",filesize="<<file_data.filesize
						<<",index="<<file_data.index
						<<",seg info: offset="<<file_data.offset
						<<",size="<<file_data.size);

			FileSaveResult save_result;
			save_result.fid = file_data.fid;
			save_result.status = FileSaveResult::DATA_SAVE_SUCC;
			if(!FileTaskSave(file_data))  //失败
			{
				LOG_ERROR(logger, "save file seg failed. fid="<<file_data.fid);
				save_result.status = FileSaveResult::DATA_SAVE_FAILED;
				//向master上报保存失败
				SendFailFileInfoToMaster(file_data.fid);
				//删除正在保存的任务
				FileTaskDelete(file_data.fid);
			}

			kvdata.SetValue(KEY_PROTOCOL_TYPE, PROTOCOL_FILE_SAVE_RESULT);
			kvdata.SetValue(KEY_FILEDATA_RESULT, save_result.status);
			kvdata.SetValue(KEY_FILEDATA_FID, save_result.fid);
			kvdata.SetValue(KEY_FILEDATA_INDEX, save_result.index);

			break;
		}
		case FileData::FLAG_END:  //已经结束
		{
			LOG_INFO(logger, "receive FileData: flag="<<file_data.flag
						<<"(data_end).client send file finished. fid="<<file_data.fid);
			if(SaveFile(file_data.fid)) //保存成功,等待master回复保存结果
				return ;

			//保存失败
			LOG_ERROR(logger, "save file failed. fid="<<file_data.fid);
			//删除正在保存的任务
			FileTaskDelete(file_data.fid);

			kvdata.SetValue(KEY_PROTOCOL_TYPE, PROTOCOL_FILE_INFO);
			kvdata.SetValue(KEY_FILEINFO_RSP_RESULT, (int8_t)FileInfo::RESULT_FAILED);
			kvdata.SetValue(KEY_FILEINFO_RSP_FID, file_data.fid);

			break;
		}
	}//switch

	ProtocolContext *send_context = NULL;
	SerializeKVData(kvdata, send_context, "FileDataResult_To_Client");

	if(SendProtocol(fd, send_context) == false)
	{
		LOG_ERROR(logger, "send fileinfo resp failed. fd="<<fd);
		DeleteProtocolContext(send_context);
	}
}

void ChunkWorker::OnGetFile(int fd, KVData *kv_data)
{
	FileReq file_req ;
	if(kv_data->GetValue(KEY_FILEDATA_REQ_FID, file_req.fid) == false)
	{
		LOG_ERROR(logger, "OnGetFile: get fid failed.");
		return ;
	}
	if(kv_data->GetValue(KEY_FILEDATA_REQ_INDEX, file_req.index) == false)
	{
		LOG_ERROR(logger, "OnGetFile: get index failed.");
		return ;
	}
	if(kv_data->GetValue(KEY_FILEDATA_REQ_OFFSET, file_req.offset) == false)
	{
		LOG_ERROR(logger, "OnGetFile: get offset failed.");
		return ;
	}
	if(kv_data->GetValue(KEY_FILEDATA_REQ_SIZE, file_req.size) == false)
	{
		LOG_ERROR(logger, "OnGetFile: get size failed.");
		return ;
	}


	//1. 打开文件
	string local_file;
	struct stat file_stat;
	DiskMgr::GetInstance()->MakePath(local_file, file_req.fid, file_req.index);
	int fdsc = open(local_file.c_str(), O_RDONLY);
	bool fd_error = (fdsc==-1);
	bool fstat_error = (fstat(fdsc, &file_stat)==-1);
	bool data_error = (file_req.offset+file_req.size>file_stat.st_size);
	if(fd_error || fstat_error || data_error)
	{
		if(fd_error)
		{
			LOG_ERROR(logger, "open file error.file="<<local_file<<",errno="<<errno<<","<<strerror(errno));
		}
		else if(fstat_error)
		{
			LOG_ERROR(logger, "fstat error.file="<<local_file<<",errno="<<errno<<","<<strerror(errno));
			close(fdsc);
		}
		else if(data_error)
		{
			LOG_ERROR(logger, "file data error:file="<<local_file<<",req_offset="<<file_req.offset<<",req_size="<<file_req.size<<",file_size="<<file_stat.st_size);
			close(fdsc);
		}

		FileData filedata;
		filedata.flag = FileData::FLAG_INVALID;
		filedata.fid = file_req.fid;

		KVData kv_data(true);
		kv_data.SetValue(KEY_PROTOCOL_TYPE, PROTOCOL_FILE);
		kv_data.SetValue(KEY_FILEDATA_FLAG, filedata.flag);
		kv_data.SetValue(KEY_FILEDATA_FID, filedata.fid);

		ProtocolContext *send_context = NULL;
		SerializeKVData(kv_data, send_context, "FileData_To_Client");

		if(SendProtocol(fd, send_context) == false)
		{
			LOG_ERROR(logger, "send file_data failed. fid="<<file_req.fid);
			DeleteProtocolContext(send_context);
		}
		return ;
	}
	lseek(fdsc, file_req.offset, SEEK_SET);

	//2 发送文件
	uint32_t index = 0;
	uint32_t READ_SIZE = 4096;
	uint32_t seg_offset = 0;
	uint32_t seg_size = 0;
	bool result = true;
	while(seg_offset < file_req.size)
	{
		seg_size = file_req.size-seg_offset;
		if(seg_size > READ_SIZE)
			seg_size = READ_SIZE;

		FileData file_data;
		file_data.flag = FileData::FLAG_SEG;
		file_data.fid = file_req.fid;
		file_data.name = "";
		file_data.filesize = file_req.size;
		file_data.index = index++;
		file_data.offset = seg_offset;
		file_data.size = seg_size;
		seg_offset += seg_size;

		KVData kvdata(true);
		kvdata.SetValue(KEY_PROTOCOL_TYPE, PROTOCOL_FILE);

		kvdata.SetValue(KEY_FILEDATA_FLAG, file_data.flag);
		kvdata.SetValue(KEY_FILEDATA_FID, file_data.fid);
		kvdata.SetValue(KEY_FILEDATA_FILE_NAME, file_data.name.c_str(), file_data.name.size()+1);
		kvdata.SetValue(KEY_FILEDATA_FILE_SIZE, file_data.filesize);
		kvdata.SetValue(KEY_FILEDATA_INDEX, file_data.index);
		kvdata.SetValue(KEY_FILEDATA_OFFSET, file_data.offset);
		kvdata.SetValue(KEY_FILEDATA_SEG_SIZE, file_data.size);

		ProtocolContext *send_context = NULL;
		send_context = NewProtocolContext();
		assert(send_context != NULL);
		send_context->type = DTYPE_BIN;
		send_context->Info = "FileData_To_Client";

		uint32_t header_size = m_ProtocolFactory->HeaderSize();
		uint32_t body_size = kvdata.Size();
		send_context->CheckSize(header_size+body_size);
		kvdata.Serialize(send_context->Buffer+header_size);
		send_context->Size = header_size+body_size;

		//读文件数据
		send_context->CheckSize(KVData::SizeBytes(file_data.size));
		char *data_buffer = send_context->Buffer+send_context->Size;
		KVBuffer kv_buffer = KVData::BeginWrite(data_buffer, KEY_FILEDATA_DATA, true);

		if(read(fdsc, kv_buffer.second, file_data.size) != file_data.size)
		{
			LOG_ERROR(logger, "read file error. fid="<<file_data.fid<<",errno="<<errno<<","<<strerror(errno));
			DeleteProtocolContext(send_context);
			result = false;
			break;
		}
		send_context->Size += KVData::EndWrite(kv_buffer, file_data.size);

		//编译头部
		m_ProtocolFactory->EncodeHeader(send_context->Buffer, send_context->Size-header_size);
		if(SendProtocol(fd, send_context) == false)
		{
			LOG_ERROR(logger, "send file_data failed.fid="<<file_data.fid);
			DeleteProtocolContext(send_context);
			result = false;
			break;
		}
	}
	close(fdsc);

	if(result == false)
		return;

	FileData file_data;
	file_data.flag = FileData::FLAG_END;
	file_data.fid = file_req.fid;
	file_data.name = "";
	KVData kvdata(true);
	kvdata.SetValue(KEY_FILEDATA_FLAG, file_data.flag);
	kvdata.SetValue(KEY_FILEDATA_FID, file_data.fid);
	kvdata.SetValue(KEY_FILEDATA_FILE_NAME, file_data.name.c_str(), file_data.name.size()+1);

	ProtocolContext *send_context = NULL;
	SerializeKVData(kvdata, send_context, "FileDataEnd_To_Client");

	if(SendProtocol(fd, send_context) == false)
	{
		LOG_ERROR(logger, "send file_data_end failed.fid="<<file_data.fid);
		DeleteProtocolContext(send_context);
	}
}

void ChunkWorker::OnFileInfoSaveResult(int fd, KVData *kv_data)
{
	FileInfoSaveResult saveresult;

	if(kv_data->GetValue(KEY_FILEINFO_SAVE_RSP_RESULT, saveresult.result) == false)
	{
		LOG_ERROR(logger, "OnFileInfoSaveResult: get result failed. fd="<<fd);
		return ;
	}
	if(kv_data->GetValue(KEY_FILEINFO_SAVE_RSP_FID, saveresult.fid) == false)
	{
		LOG_ERROR(logger, "OnFileInfoSaveResult: get fid failed. fd="<<fd);
		return ;
	}
	LOG_INFO(logger, "OnFileInfoSaveResult: save result="<<saveresult.result<<",fid="<<saveresult.fid);

	//pthread_mutex_lock(&m_filetask_lock);
	FileTaskMap::iterator it = m_FileTaskMap.find(saveresult.fid);
	if(it != m_FileTaskMap.end())
	{
		FileTask &file_task = it->second;
		FileInfo &file_info = file_task.file_info;

		KVData kvdata(true);
		kvdata.SetValue(KEY_PROTOCOL_TYPE, PROTOCOL_FILE_INFO);
		kvdata.SetValue(KEY_FILEINFO_RSP_FILE_NAME, file_info.name);
		kvdata.SetValue(KEY_FILEINFO_RSP_FILE_SIZE, file_info.size);
		kvdata.SetValue(KEY_FILEINFO_RSP_FID, file_info.fid);

		KVData sub_kvdata(true);
		if(saveresult.result == FileInfoSaveResult::RESULT_SUCC)  //master保存成功
		{
			file_info.result = FileInfo::RESULT_SUCC;
			kvdata.SetValue(KEY_FILEINFO_RSP_RESULT, file_info.result);
			kvdata.SetValue(KEY_FILEINFO_RSP_CHUNK_NUM, 1);

			ChunkPath &chunk_path = file_info.GetChunkPath(0);
			LOG_DEBUG(logger, "chunk[0]:id="<<chunk_path.id
						<<",ip="<<chunk_path.ip
						<<",port="<<chunk_path.port
						<<",index="<<chunk_path.index
						<<",offset="<<chunk_path.offset);

			sub_kvdata.SetValue(KEY_FILEINFO_RSP_CHUNK_ID, chunk_path.id);
			sub_kvdata.SetValue(KEY_FILEINFO_RSP_CHUNK_IP, chunk_path.id);
			sub_kvdata.SetValue(KEY_FILEINFO_RSP_CHUNK_PORT, chunk_path.port);
			sub_kvdata.SetValue(KEY_FILEINFO_RSP_CHUNK_INDEX, chunk_path.index);
			sub_kvdata.SetValue(KEY_FILEINFO_RSP_CHUNK_OFFSET, chunk_path.offset);

			kvdata.SetValue(KEY_FILEINFO_RSP_CHUNK_PATH0, &sub_kvdata);
		}
		else
		{
			file_info.result = FileInfo::RESULT_FAILED;
			kvdata.SetValue(KEY_FILEINFO_RSP_RESULT, file_info.result);
			LOG_WARN(logger, "master save file info failed. fid="<<saveresult.fid);
		}

		ProtocolContext *send_context = NULL;
		SerializeKVData(kvdata, send_context, "FileInfo");

		if(!SendProtocol(file_task.fd, send_context))
		{
			LOG_ERROR(logger, "send file info to client failed. fd="<<file_task.fd<<",fid="<<file_task.fid);
			DeleteProtocolContext(send_context);
		}
	}
	else
	{
		LOG_WARN(logger, "can't find file task. fid="<<saveresult.fid);
		saveresult.result = FileInfo::RESULT_FAILED;
	}
	//pthread_mutex_unlock(&m_filetask_lock);

	//删除任务
	FileTaskDelete(saveresult.fid);
}


//查找文件任务
bool ChunkWorker::FileTaskFind(string &fid)
{
	bool find;
	//pthread_mutex_lock(&m_filetask_lock);
	find = m_FileTaskMap.find(fid)!=m_FileTaskMap.end();
	//pthread_mutex_unlock(&m_filetask_lock);
	return find;
}

//创建一个文件任务
bool ChunkWorker::FileTaskCreate(int32_t fd, FileData &filedata)
{
	bool result = false;
	//pthread_mutex_lock(&m_filetask_lock);

	FileTaskMap::iterator it = m_FileTaskMap.find(filedata.fid);
	if(it == m_FileTaskMap.end())
	{
		FileTask file_task;
		file_task.fd = fd;
		file_task.fid = filedata.fid;
		file_task.name = filedata.name;
		file_task.size = filedata.filesize;
		file_task.buf = (char*)malloc(filedata.filesize);
		if(file_task.buf == NULL)
		{
			LOG_ERROR(logger, "create file task failed:no memory.fid="<<filedata.fid
						<<",file_name="<<filedata.name
						<<",file_size="<<filedata.filesize
						<<",seg_size="<<filedata.size);
		}
		else
		{
			result = true;
			m_FileTaskMap.insert(std::make_pair(file_task.fid, file_task));  //保存任务
			LOG_DEBUG(logger, "insert new file task to map succ.fid="<<filedata.fid
						<<",file_name="<<filedata.name
						<<",file_size="<<filedata.filesize
						<<",seg_size="<<filedata.size);
		}
	}
	else
		LOG_WARN(logger, "file task already exists. fid="<<filedata.fid);

	//pthread_mutex_unlock(&m_filetask_lock);
	return result;
}
//删除一个文件任务
void ChunkWorker::FileTaskDelete(string &fid)
{
	//pthread_mutex_lock(&m_filetask_lock);
	FileTaskMap::iterator it = m_FileTaskMap.find(fid);
	if(it != m_FileTaskMap.end())
	{
		FileTask &file_task = it->second;
		LOG_DEBUG(logger, "delete file task succ:fid="<<file_task.fid
					<<",name="<<file_task.name
					<<",size="<<file_task.size);
		free(file_task.buf);
		m_FileTaskMap.erase(it);
	}
	else
		LOG_WARN(logger, "delete file task failed:can't find task. fid="<<fid);

	//pthread_mutex_unlock(&m_filetask_lock);
}
//保存文件分片数据
bool ChunkWorker::FileTaskSave(FileData &filedata)
{
	bool result = false;
	//pthread_mutex_lock(&m_filetask_lock);

	FileTaskMap::iterator it = m_FileTaskMap.find(filedata.fid);
	if(it != m_FileTaskMap.end())
	{
		FileTask &file_task = it->second;
		LOG_DEBUG(logger, "file_task:fid="<<file_task.fid
					<<",size="<<file_task.size
					<<",filedata:total_size="<<filedata.filesize
					<<",offset="<<filedata.offset
					<<",size="<<filedata.size);
		if(filedata.offset+filedata.size <= file_task.size)
		{
			result = true;
			memcpy(file_task.buf+filedata.offset, filedata.data, filedata.size);
		}
	}
	else
		LOG_WARN(logger, "can't find file task:fid="<<filedata.fid);

	//pthread_mutex_unlock(&m_filetask_lock);
	return result;
}

//文件已经传送完毕,保存到系统中
bool ChunkWorker::SaveFile(string &fid)
{
	//pthread_mutex_lock(&m_filetask_lock);
	bool result = false;
	FileTaskMap::iterator it = m_FileTaskMap.find(fid);
	if(it != m_FileTaskMap.end())
	{
		//向master上报file_info
		FileTask &file_task = it->second;
		FileInfo &fileinfo = file_task.file_info;
		fileinfo.fid = fid;
		fileinfo.name = file_task.name;
		fileinfo.size = file_task.size;

		KVData kvdata(true);
		kvdata.SetValue(KEY_PROTOCOL_TYPE, PROTOCOL_FILE_INFO);    //协议类型
		kvdata.SetValue(KEY_FILEINFO_SAVE_FID, fileinfo.fid);
		kvdata.SetValue(KEY_FILEINFO_SAVE_FILE_NAME, fileinfo.name);
		kvdata.SetValue(KEY_FILEINFO_SAVE_FILE_SIZE, fileinfo.size);

		//保存到磁盘
		ChunkPath chunk_path;
		result = DiskMgr::GetInstance()->SaveFileToDisk(fid, file_task.buf, file_task.size, chunk_path);
		if(result != false)
		{
			fileinfo.AddChunkPath(chunk_path);
			fileinfo.result = FileInfo::RESULT_SUCC;

			kvdata.SetValue(KEY_FILEINFO_SAVE_CHUNK_ID, chunk_path.id);
			kvdata.SetValue(KEY_FILEINFO_SAVE_CHUNK_IP, chunk_path.ip);
			kvdata.SetValue(KEY_FILEINFO_SAVE_CHUNK_PORT, chunk_path.port);
			kvdata.SetValue(KEY_FILEINFO_SAVE_CHUNK_INDEX,chunk_path.index);
			kvdata.SetValue(KEY_FILEINFO_SAVE_CHUNK_OFFSET,chunk_path.offset);
		}
		else
		{
			LOG_ERROR(logger, "save data to file failed. fid="<<fid);
			fileinfo.result = FileInfo::RESULT_FAILED;

		}
		kvdata.SetValue(KEY_FILEINFO_SAVE_RESULT, fileinfo.result);

		ProtocolContext *send_context = NULL;
		SerializeKVData(kvdata, send_context, "FileInfo2Master");

		if(!SendProtocol(GetMasterConnect(), send_context))
		{
			result = false;
			DeleteProtocolContext(send_context);
			LOG_ERROR(logger, "send file info to master failed. fid="<<fid);
		}
	}
	else
		LOG_WARN(logger, "save file task failed:can't find task. fid="<<fid);

	//pthread_mutex_unlock(&m_filetask_lock);

	return result;
}

void ChunkWorker::SendFailFileInfoToMaster(string &fid)
{
	KVData kvdata(true);
	kvdata.SetValue(KEY_PROTOCOL_TYPE, PROTOCOL_FILE_INFO);
	kvdata.SetValue(KEY_FILEINFO_SAVE_RESULT, (int32_t)FileInfo::RESULT_FAILED);
	kvdata.SetValue(KEY_FILEINFO_SAVE_FID, fid);

	ProtocolContext *send_context = NULL;
	SerializeKVData(kvdata, send_context, "FileInfo2Master");
	if(!SendProtocol(GetMasterConnect(), send_context))
	{
		LOG_ERROR(logger, "send faile_file_info to master failed. fid="<<fid);
		DeleteProtocolContext(send_context);
	}
}
