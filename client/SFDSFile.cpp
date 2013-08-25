/*
 * SFSClient.cpp
 *
 *  Created on: 2012-11-8
 *      Author: LiuYongJin
 */

#include "SFDSFile.h"
#include "sha1.h"
#include "KeyDefine.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

using namespace SFDS;

IMPL_LOGGER(File, logger);

File::File(string &master_addr, uint32_t master_port, uint32_t n_replica)
	:m_master_addr(master_addr)
	,m_master_port(master_port)
	,m_n_replica(n_replica)
{}

bool File::GetFileInfo(string &fid, FileInfo &file_info)
{
	return _get_file_info(fid, false, file_info);
}

bool File::SaveFile(FileInfo &file_info, string &local_file)
{
	string fid;
	int retry_time = 0;

	if(!SHA1::hash(local_file, fid))
	{
		LOG_ERROR(logger, "get fid failed. file="<<local_file);
		return false;
	}
	LOG_DEBUG(logger, "get fid: file="<<local_file<<",fid="<<fid);

	while(true)
	{
		if(!_get_file_info(fid, true, file_info))
		{
			LOG_DEBUG(logger, "query master error.");
			return false;
		}

		switch(file_info.result)
		{
		case FileInfo::RESULT_FAILED:  //失败
			{
				LOG_DEBUG(logger, "get file info failed. fid="<<fid);
				return true;
			}
		case FileInfo::RESULT_SUCC:  //已经保存过
			{
				LOG_DEBUG(logger, "file already exist. fid="<<file_info.fid<<",name="<<file_info.name<<",size="<<file_info.size);
				return true;
			}
		case FileInfo::RESULT_CHUNK:  //分配chunk成功
			{
				int i;
				for(i=0; i<file_info.GetChunkPathCount(); ++i)
				{
					ChunkPath &chunk_path = file_info.GetChunkPath(i);
					LOG_INFO(logger, "request chunk to store file. fid="<<fid<<",chunk_ip="<<chunk_path.ip<<",chunk_port="<<chunk_path.port);
					FileInfo temp_file_info;
					if(!_send_file_to_chunk(local_file, fid, chunk_path.ip, chunk_path.port, temp_file_info))
					{
						file_info.result = FileInfo::RESULT_FAILED;
						break;
					}
					else
						file_info = temp_file_info;
				}
				return true;
			}
		case FileInfo::RESULT_SAVING:  //正在保存
			{
				++retry_time;
				if(retry_time > 3)
				{
					LOG_DEBUG(logger, "fid="<<fid<<" is saving. reach the max retry_times="<<retry_time);
					return true;
				}
				LOG_DEBUG(logger, "fid="<<fid<<" is saving, waiting for file_info. retry_time="<<retry_time);
				sleep(2);  //等1s后重新请求master获取文件信息
				break;
			}
		default:
			{
				LOG_WARN(logger, "unknown result value:result="<<file_info.result);
				return false;
			}
		}
	}

	return true;
}

bool File::GetFile(string &fid, string &local_file)
{
	FileInfo file_info;
	file_info.result = FileInfo::RESULT_INVALID;
	if(!_get_file_info(fid, false, file_info) || file_info.result!=FileInfo::RESULT_SUCC)
	{
		LOG_DEBUG(logger, "get file info failed. fid="<<fid<<",result="<<file_info.result);
		return false;
	}

	int chunk_count = file_info.GetChunkPathCount();
	assert(chunk_count > 0);
	int i;
	bool finish = false;
	ByteBuffer byte_buffer(10240);
	KVData kvdata(true);

	for(i=0; i<chunk_count&&!finish; ++i)
	{
		ChunkPath &chunkpath = file_info.GetChunkPath(i);
		int32_t chunk_fd = Socket::Connect(chunkpath.port, chunkpath.ip.c_str());
		if(chunk_fd < 0)
		{
			LOG_ERROR(logger, "can't connect chunk:ip="<<chunk_path.ip<<",port="<<chunk_path.port);
			continue;
		}

		FileReq file_req;
		file_req.fid = file_info.fid;
		file_req.size = file_info.size;
		file_req.index = chunkpath.index;
		file_req.offset = chunkpath.offset;

		kvdata.Clear();
		kvdata.SetValue(KEY_PROTOCOL_TYPE, PROTOCOL_FILE_REQ);
		kvdata.SetValue(KEY_FILEDATA_REQ_FID, file_req.fid);
		kvdata.SetValue(KEY_FILEDATA_REQ_SIZE, file_req.size);
		kvdata.SetValue(KEY_FILEDATA_REQ_INDEX, file_req.index);
		kvdata.SetValue(KEY_FILEDATA_REQ_OFFSET, file_req.offset);

		if(SendData(chunk_fd, kvdata) == false)
		{
			LOG_ERROR(logger, "send data to chunk failed.");
			Socket::Close(chunk_fd);
			continue;
		}

		int fd = open(local_file.c_str(), O_WRONLY|O_CREAT);
		if(fd == -1)
		{
			LOG_ERROR(logger, "open file failed. file="<<local_file<<",errno="<<errno<<","<<strerror(errno));
			Socket::Close(chunk_fd);
			return false;
		}

		//接收数据
		while(1)
		{
			if(RecvData(chunk_fd, byte_buffer, kvdata) == false)
			{
				LOG_ERROR(logger, "recv data from chunk failed.");
				break;
			}

			int32_t protocol_type;
			kvdata.GetValue(KEY_PROTOCOL_TYPE, protocol_type);
			if(protocol_type != PROTOCOL_FILE)
			{
				LOG_ERROR(logger, "protocol type invalid.");
				break;
			}

			FileData file_data;
			kvdata.GetValue(KEY_FILEDATA_FLAG, file_data.flag);
			kvdata.GetValue(KEY_FILEDATA_FID, file_data.fid);
			kvdata.GetValue(KEY_FILEDATA_FILE_NAME, file_data.name);

			if(file_data.flag == FileData::FLAG_END)  //完成
			{
				LOG_INFO(logger, "get data finished. fid="<<file_data.fid);
				finish = true;
				break;
			}

			else if(file_data.flag != FileData::FLAG_SEG)
			{
				LOG_ERROR(logger, "get data from chunk error. fid="<<file_info.fid);
				break;
			}

			kvdata.GetValue(KEY_FILEDATA_FILE_SIZE, file_data.filesize);
			kvdata.GetValue(KEY_FILEDATA_INDEX, file_data.index);
			kvdata.GetValue(KEY_FILEDATA_OFFSET, file_data.offset);
			kvdata.GetValue(KEY_FILEDATA_SEG_SIZE, file_data.size);

			if(!_save_filedata_to_file(fd, file_data))
			{
				LOG_ERROR(logger, "save data to file error.");
				break;
			}
		}

		Socket::Close(chunk_fd);
		close(fd);
	}

	return true;
}

bool File::_get_file_info(string &fid, bool query_chunk, FileInfo &fileinfo)
{
	//连接master
	int master_fd = Socket::Connect(m_master_port, m_master_addr.c_str());
	if(master_fd == -1)
		return false;

	//协议数据
	FileInfoReq fileinfo_req;
	fileinfo_req.fid = fid;
	fileinfo_req.query_chunkpath = query_chunk?1:0;

	KVData kvdata(true);
	kvdata.SetValue(KEY_PROTOCOL_TYPE, PROTOCOL_FILE_INFO_REQ);
	kvdata.SetValue(KEY_FILEINFO_REQ_FID, fileinfo_req.fid);
	kvdata.SetValue(KEY_FILEINFO_REQ_CHUNKPATH, fileinfo_req.query_chunkpath);

	if(SendData(master_fd, kvdata) == false)
	{
		LOG_ERROR(logger, "send data to master failed.");
		Socket::Close(master_fd);
		return false;
	}

	ByteBuffer byte_buffer(10240);
	if(RecvData(master_fd, byte_buffer, kvdata) == false)
	{
		LOG_ERROR(logger, "recv data from master failed.");
		return false;
	}

	uint32_t protocol_type;
	if(!kvdata.GetValue(KEY_PROTOCOL_TYPE, protocol_type) || protocol_type!=KEY_FILEINFO_RSP_RESULT)
	{
		Socket::Close(master_fd);
		return false;
	}
	if(!kvdata.GetValue(KEY_PROTOCOL_TYPE, fileinfo.result))
	{
		Socket::Close(master_fd);
		return false;
	}
	if(!kvdata.GetValue(KEY_FILEINFO_RSP_FID, fileinfo.fid))
	{
		Socket::Close(master_fd);
		return false;
	}

	if(fileinfo.result == FileInfo::RESULT_SUCC || fileinfo.result==FileInfo::RESULT_CHUNK)
	{
		kvdata.GetValue(KEY_FILEINFO_RSP_FILE_NAME, fileinfo.name);
		kvdata.GetValue(KEY_FILEINFO_RSP_FILE_SIZE, fileinfo.size);

		uint32_t chunk_count;
		if(!kvdata.GetValue(KEY_FILEINFO_RSP_CHUNK_NUM, chunk_count) || chunk_count<=0)
		{
			Socket::Close(master_fd);
			return false;
		}
		for(int i=0; i<chunk_count; ++i)
		{
			KVData kv_chunkpath(true);
			if(!kvdata.GetValue(KEY_FILEINFO_RSP_CHUNK_PATH0, kv_chunkpath))
			{
				Socket::Close(master_fd);
				return false;
			}

			ChunkPath chunkpath;
			kv_chunkpath.GetValue(KEY_FILEINFO_RSP_CHUNK_ID, chunkpath.id);
			kv_chunkpath.GetValue(KEY_FILEINFO_RSP_CHUNK_IP, chunkpath.ip);
			kv_chunkpath.GetValue(KEY_FILEINFO_RSP_CHUNK_PORT, chunkpath.port);
			kv_chunkpath.GetValue(KEY_FILEINFO_RSP_CHUNK_INDEX, chunkpath.index);
			kv_chunkpath.GetValue(KEY_FILEINFO_RSP_CHUNK_OFFSET, chunkpath.offset);

			fileinfo.AddChunkPath(chunkpath);
		}
	}

	Socket::Close(master_fd);
	return true;
}

bool File::_send_file_to_chunk(string &local_file, string &fid, string &chunk_addr, int chunk_port, FileInfo &fileinfo)
{
	int32_t chunk_fd = Socket::Connect(chunk_port, chunk_addr.c_str());
	if(chunk_fd == -1)
	{
		LOG_ERROR(logger, "connect chunk failed.");
		return false;
	}

	//1. 获取文件大小
	FILE *fd = fopen(local_file.c_str(), "rb");
	if(fd == NULL)
	{
		LOG_ERROR(logger, "open file error.");
		Socket::Close(chunk_fd);
		return false;
	}
	struct stat file_stat;
	if(fstat(fileno(fd), &file_stat) == -1)
	{
		LOG_ERROR(logger, "stat file error. errno="<<errno<<","<<strerror(errno));
		fclose(fd);
		Socket::Close(chunk_fd);
		return false;
	}

	//发送文件
	string filename = local_file.substr(local_file.find_last_of('/')+1);
	uint32_t filesize = file_stat.st_size;
	bool result = true;

	ByteBuffer byte_buffer(10240);
	KVData kvdata(true);

	//1 发送开始协议
	FileData file_data;
	file_data.fid = fid;
	file_data.name = filename;
	file_data.filesize = filesize;

	file_data.flag = FileData::FLAG_START;
	kvdata.SetValue(KEY_PROTOCOL_TYPE, PROTOCOL_FILE);
	kvdata.SetValue(KEY_FILEDATA_FID, file_data.fid);
	kvdata.SetValue(KEY_FILEDATA_FILE_NAME, file_data.name);
	kvdata.SetValue(KEY_FILEDATA_FILE_SIZE, file_data.filesize);
	kvdata.SetValue(KEY_FILEDATA_FLAG, file_data.flag);

	if(SendData(chunk_fd, kvdata) == false)
	{
		LOG_ERROR(logger, "send file data start to chunk failed.");
		fclose(fd);
		Socket::Close(chunk_fd);
		return false;
	}
	if(RecvData(chunk_fd, byte_buffer, kvdata) == false)
	{
		LOG_ERROR(logger, "recv data to chunk failed.");
		fclose(fd);
		Socket::Close(chunk_fd);
		return false;
	}

	FileSaveResult save_result;
	int32_t protocol_type;
	if(kvdata.GetValue(KEY_PROTOCOL_TYPE, protocol_type)==false
		|| kvdata.GetValue(KEY_FILEDATA_RESULT, save_result.status) == false
		|| kvdata.GetValue(KEY_FILEDATA_FID, save_result.fid))
	{
		LOG_ERROR(logger, "get protocol data failed.");
		fclose(fd);
		Socket::Close(chunk_fd);
		return false;
	}

	if(protocol_type!=PROTOCOL_FILE_SAVE_RESULT)
	{
		LOG_ERROR(logger, "protocol type invalid.");
		fclose(fd);
		Socket::Close(chunk_fd);
		return false;
	}

	if(save_result.status == FileSaveResult::CREATE_FAILED) //存储失败
	{
		LOG_ERROR(logger, "chunk create file failed. fid="<<fid);
		fclose(fd);
		Socket::Close(chunk_fd);
		return false;
	}

	uint32_t seg_offset = 0;
	int seg_size = 0;
	//2 发送分片
	file_data.index = 0;
	file_data.offset = 0;

	int READ_SIZE = 4096;
	char buffer[5000];
	while(file_data.offset < filesize)
	{
		seg_size = filesize-seg_offset;
		if(seg_size > READ_SIZE)
			seg_size = READ_SIZE;
		file_data.size = seg_size;
		file_data.flag = FileData::FLAG_SEG;
		if(fread(buffer, 1, file_data.size, fd) != file_data.size)
		{
			LOG_ERROR(logger, "read file error. error="<<strerror(errno));
			fclose(fd);
			Socket::Close(chunk_fd);
			return false;
		}

		kvdata.Clear();
		kvdata.SetValue(KEY_PROTOCOL_TYPE, PROTOCOL_FILE);
		kvdata.SetValue(KEY_FILEDATA_FID, file_data.fid);
		kvdata.SetValue(KEY_FILEDATA_FILE_NAME, file_data.name);
		kvdata.SetValue(KEY_FILEDATA_FILE_SIZE, file_data.filesize);
		kvdata.SetValue(KEY_FILEDATA_FLAG, file_data.flag);
		kvdata.SetValue(KEY_FILEDATA_INDEX, file_data.index);
		kvdata.SetValue(KEY_FILEDATA_SEG_SIZE, file_data.size);
		kvdata.SetValue(KEY_FILEDATA_OFFSET, file_data.offset);
		kvdata.SetValue(KEY_FILEDATA_DATA, buffer, file_data.size);

		if(SendData(chunk_fd, kvdata) == false)
		{
			LOG_ERROR(logger, "send file data failed.");
			fclose(fd);
			Socket::Close(chunk_fd);
			return false;
		}

		++file_data.index;
		file_data.offset += seg_size;
	}
	fclose(fd);

	file_data.flag = FileData::FLAG_END;
	kvdata.Clear();
	kvdata.SetValue(KEY_PROTOCOL_TYPE, PROTOCOL_FILE);
	kvdata.SetValue(KEY_FILEDATA_FLAG, file_data.flag);
	kvdata.SetValue(KEY_FILEDATA_FID, file_data.fid);
	kvdata.SetValue(KEY_FILEDATA_FILE_NAME, file_data.name);
	kvdata.SetValue(KEY_FILEDATA_FILE_SIZE, file_data.filesize);
	if(SendData(chunk_fd, kvdata) == false)
	{
		LOG_ERROR(logger, "send file data end to chunk failed.");
		Socket::Close(chunk_fd);
		return false;
	}

	//等待chunk回复file_info信息
	if(RecvData(chunk_fd, byte_buffer, kvdata) == false)
	{
		LOG_ERROR(logger, "recv file info from chunk failed.");
		fclose(fd);
		Socket::Close(chunk_fd);
		return false;
	}

	if(!kvdata.GetValue(KEY_PROTOCOL_TYPE, protocol_type) || protocol_type!=KEY_FILEINFO_RSP_RESULT)
	{
		LOG_ERROR(logger, "recv file info from chunk failed. invalid protocol_type");
		Socket::Close(chunk_fd);
		return false;
	}
	if(!kvdata.GetValue(KEY_PROTOCOL_TYPE, fileinfo.result))
	{
		LOG_ERROR(logger, "recv file info from chunk failed. get result failed");
		Socket::Close(chunk_fd);
		return false;
	}
	if(!kvdata.GetValue(KEY_FILEINFO_RSP_FID, fileinfo.fid))
	{
		LOG_ERROR(logger, "recv file info from chunk failed. get fid failed");
		Socket::Close(chunk_fd);
		return false;
	}

	if(fileinfo.result == FileInfo::RESULT_SUCC || fileinfo.result==FileInfo::RESULT_CHUNK)
	{
		kvdata.GetValue(KEY_FILEINFO_RSP_FILE_NAME, fileinfo.name);
		kvdata.GetValue(KEY_FILEINFO_RSP_FILE_SIZE, fileinfo.size);

		uint32_t chunk_count;
		if(!kvdata.GetValue(KEY_FILEINFO_RSP_CHUNK_NUM, chunk_count) || chunk_count<=0)
		{
			Socket::Close(chunk_fd);
			return false;
		}
		for(int i=0; i<chunk_count; ++i)
		{
			KVData kv_chunkpath(true);
			if(!kvdata.GetValue(KEY_FILEINFO_RSP_CHUNK_PATH0, kv_chunkpath))
			{
				Socket::Close(chunk_fd);
				return false;
			}

			ChunkPath chunkpath;
			kv_chunkpath.GetValue(KEY_FILEINFO_RSP_CHUNK_ID, chunkpath.id);
			kv_chunkpath.GetValue(KEY_FILEINFO_RSP_CHUNK_IP, chunkpath.ip);
			kv_chunkpath.GetValue(KEY_FILEINFO_RSP_CHUNK_PORT, chunkpath.port);
			kv_chunkpath.GetValue(KEY_FILEINFO_RSP_CHUNK_INDEX, chunkpath.index);
			kv_chunkpath.GetValue(KEY_FILEINFO_RSP_CHUNK_OFFSET, chunkpath.offset);

			fileinfo.AddChunkPath(chunkpath);
		}
	}

	Socket::Close(chunk_fd);
	return true;
}

bool File::_save_filedata_to_file(int fd, FileData &file_data)
{
	lseek(fd, file_data.offset, SEEK_SET);
	ssize_t size = write(fd, file_data.data, file_data.size);
	return size == file_data.size;
}

bool File::SendData(int fd, KVData &kvdata)
{
	SFDSProtocolFactory protocol_factory;
	uint32_t header_size = protocol_factory.HeaderSize();
	uint32_t body_size = kvdata.Size();

	ByteBuffer byte_buffer(10240);
	protocol_factory.EncodeHeader(byte_buffer.Buffer, body_size);
	kvdata.Serialize(byte_buffer.Buffer+header_size);

	return Socket::SendAll(fd, byte_buffer.Buffer, header_size+body_size)==header_size+body_size;
}

bool File::RecvData(int fd, ByteBuffer &byte_buffer, KVData &kvdata)
{
	kvdata.Clear();

	SFDSProtocolFactory protocol_factory;
	uint32_t header_size = protocol_factory.HeaderSize();

	if(Socket::RecvAll(fd, byte_buffer.Buffer, header_size) != header_size)
		return false;

	DataType type;
	uint32_t body_size;
	if(protocol_factory.DecodeHeader(byte_buffer.Buffer, type, body_size) == DECODE_ERROR)
		return false;

	if(Socket::RecvAll(fd, byte_buffer.Buffer+header_size, body_size) != body_size)
			return false;
	return kvdata.UnSerialize(byte_buffer.Buffer+header_size, body_size);
}
