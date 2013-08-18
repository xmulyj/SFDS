/*
 * SFSClient.cpp
 *
 *  Created on: 2012-11-8
 *      Author: LiuYongJin
 */

#include "SFSFile.h"
#include "sha1.h"

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
		KVData kvdata(true);
		kvdata.SetValue(KEY_PROTOCOL_TYPE, PROTOCOL_FILE_REQ);
		kvdata.SetValue(KEY_FILEDATA_REQ_FID, file_req.fid);
		kvdata.SetValue(KEY_FILEDATA_REQ_SIZE, file_req.size);
		kvdata.SetValue(KEY_FILEDATA_REQ_INDEX, file_req.index);
		kvdata.SetValue(KEY_FILEDATA_REQ_OFFSET, file_req.offset);

		SFDSProtocolFactory protocol_factory;
		uint32_t header_size = protocol_factory.HeaderSize();

		char buffer[1024];
		assert(kvdata.Size()+header_size < 1024);
		//编码协议体
		uint32_t body_size = kvdata.Serialize(buffer+header_size);
		//编码协议头
		protocol_factory.EncodeHeader(buffer, kvdata.Size());

		if(header_size+body_size != Socket::SendAll(chunk_fd, buffer, body_size))
		{
			Socket::Close(chunk_fd);
			return false;
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
			//接收数据
			if(header_size != Socket::RecvAll(chunk_fd, buffer, header_size))
			{
				Socket::Close(master_fd);
				return false;
			}
			DataType type;
			if(protocol_factory.DecodeHeader(buffer, type, body_size) == DECODE_ERROR)
			{
				Socket::Close(master_fd);
				return false;
			}
			if(body_size != Socket::RecvAll(master_fd, buffer, body_size))
			{
				Socket::Close(master_fd);
				return false;
			}

			kvdata.Clear();
			if(kvdata.UnSerialize(buffer, body_size) == false)
			{
				Socket::Close(master_fd);
				return false;
			}


			ProtocolFile *protocol_file = (ProtocolFile*)m_protocol_family.create_protocol(PROTOCOL_FILE);
			assert(protocol_file != NULL);
			if(!TransProtocol::recv_protocol(&trans_socket, protocol_file))
			{
				m_protocol_family.destroy_protocol(protocol_file);
				SLOG_ERROR("receive file_seg failed. fid=%s.", file_info.fid.c_str());
				break;
			}
			FileSeg &file_seg = protocol_file->get_file_seg();
			if(file_seg.flag == FileSeg::FLAG_END)  //完成
			{
				SLOG_INFO("get data finished. fid=%s.", file_info.fid.c_str());
				finish = true;
				break;
			}
			else if(file_seg.flag != FileSeg::FLAG_SEG)
			{
				SLOG_ERROR("get data from chunk error. fid=%s.", file_info.fid.c_str());
				m_protocol_family.destroy_protocol(protocol_file);
				break;
			}

			if(!_save_fileseg_to_file(fd, file_seg))
			{
				LOG_ERROR(logger, "save data to file error.");
				m_protocol_family.destroy_protocol(protocol_file);
				break;
			}
			m_protocol_family.destroy_protocol(protocol_file);
		}

		close(fd);
	}

	return true;
}

///////////////////////////////////////////////////////
bool File::_query_master(ProtocolFileInfoReq *protocol, FileInfo &file_info)
{
	TransSocket trans_socket(m_master_addr.c_str(), m_master_port);
	if(!trans_socket.open(1000))
	{
		SLOG_ERROR("connect master failed.");
		return false;
	}
	//发送协议
	if(!TransProtocol::send_protocol(&trans_socket, protocol))
		return false;
	//接收协议
	ProtocolFileInfo *protocol_fileinfo = (ProtocolFileInfo *)m_protocol_family.create_protocol(PROTOCOL_FILE_INFO);
	assert(protocol_fileinfo != NULL);
	bool temp = TransProtocol::recv_protocol(&trans_socket, protocol_fileinfo);
	if(temp)  //请求成功
		file_info = protocol_fileinfo->get_fileinfo();
	m_protocol_family.destroy_protocol(protocol_fileinfo);

	return temp;
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

	SFDSProtocolFactory protocol_factory;
	uint32_t header_size = protocol_factory.HeaderSize();

	char buffer[1024];
	assert(kvdata.Size()+header_size < 1024);
	//编码协议体
	uint32_t body_size = kvdata.Serialize(buffer+header_size);
	//编码协议头
	protocol_factory.EncodeHeader(buffer, kvdata.Size());

	if(header_size+body_size != Socket::SendAll(master_fd, buffer, body_size))
	{
		Socket::Close(master_fd);
		return false;
	}

	//接收数据
	if(header_size != Socket::RecvAll(master_fd, buffer, header_size))
	{
		Socket::Close(master_fd);
		return false;
	}

	DataType type;
	if(protocol_factory.DecodeHeader(buffer, type, body_size) == DECODE_ERROR)
	{
		Socket::Close(master_fd);
		return false;
	}
	if(body_size != Socket::RecvAll(master_fd, buffer, body_size))
	{
		Socket::Close(master_fd);
		return false;
	}

	kvdata.Clear();
	if(kvdata.UnSerialize(buffer, body_size) == false)
	{
		Socket::Close(master_fd);
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
	return true;
}

bool File::_send_file_protocol_to_chunk(TransSocket* trans_socket, ProtocolFile *protocol_file, ByteBuffer *byte_buffer, int fd)
{
	byte_buffer->clear();

	//1 预留头部空间
	int header_length;
	ProtocolHeader *header = protocol_file->get_protocol_header();
	header_length = header->get_header_length();
	byte_buffer->reserve(header_length);
	//2 编码协议体
	if(!protocol_file->encode_body(byte_buffer))
	{
		SLOG_ERROR("encode body error");
		return false;
	}
	//3 添加数据
	if(fd > 0)
	{
		FileSeg& file_seg = protocol_file->get_file_seg();
		char *data_buffer = byte_buffer->get_append_buffer(file_seg.size);
		if(read(fd, data_buffer, file_seg.size) != file_seg.size)
		{
			LOG_ERROR(logger, "read file error. errno="<<errno<<","<<strerror(errno));
			return false;
		}
		byte_buffer->set_append_size(file_seg.size);
	}
	//4. 编码协议头
	int body_length = byte_buffer->size()-header_length;
	char *header_buffer = byte_buffer->get_data(0, header_length);
	if(!header->encode(header_buffer, body_length))
	{
		LOG_ERROR(logger, "encode header error");
		return false;
	}
	//5. 发送数据
	if(trans_socket->send_data_all(byte_buffer->get_data(), byte_buffer->size()) == TRANS_ERROR)
	{
		LOG_ERROR(logger, "send data error");
		return false;
	}

	return true;
}

bool File::_send_file_to_chunk(string &local_file, string &fid, string &chunk_addr, int chunk_port, FileInfo &file_info)
{
	TransSocket trans_socket(chunk_addr.c_str(), chunk_port);
	if(!trans_socket.open(1000))
	{
		LOG_ERROR(logger, "connect sfs failed.");
		return false;
	}
	//1. 获取文件大小
	int fd = open(local_file.c_str(), O_RDONLY);
	if(fd == -1)
	{
		LOG_ERROR(logger, "open file error.");
		return false;
	}
	struct stat file_stat;
	if(fstat(fd, &file_stat) == -1)
	{
		LOG_ERROR(logger, "stat file error. errno="<<errno<<","<<strerror(errno));
		close(fd);
		return false;
	}

	//发送文件
	int READ_SIZE = 4096;
	string filename = local_file.substr(local_file.find_last_of('/')+1);
	uint32_t filesize = file_stat.st_size;
	uint32_t seg_offset = 0;
	int seg_size = 0;
	bool result = true;

	ByteBuffer byte_buffer(2048);
	ProtocolFile *protocol_file = (ProtocolFile *)m_protocol_family.create_protocol(PROTOCOL_FILE);
	assert(protocol_file != NULL);
	ProtocolFileSaveResult *protocol_save_result = (ProtocolFileSaveResult*)m_protocol_family.create_protocol(PROTOCOL_FILE_SAVE_RESULT);
	assert(protocol_save_result != NULL);

	//1 发送开始协议
	FileSeg &start_file_seg = protocol_file->get_file_seg();
	start_file_seg.flag = FileSeg::FLAG_START;
	start_file_seg.fid = fid;
	start_file_seg.name = filename;
	start_file_seg.filesize = filesize;
	start_file_seg.size = 0;
	if(!_send_file_protocol_to_chunk(&trans_socket, protocol_file, &byte_buffer, -1))
	{
		LOG_ERROR(logger, "send start_file_seg failed. fid="<<fid);
		m_protocol_family.destroy_protocol(protocol_file);
		return false;
	}
	if(!TransProtocol::recv_protocol(&trans_socket, protocol_save_result))
	{
		LOG_ERROR(logger, "receive start_file_seg resp failed. fid="<<fid);
		m_protocol_family.destroy_protocol(protocol_file);
		m_protocol_family.destroy_protocol(protocol_save_result);
		return false;
	}
	FileSaveResult &save_result = protocol_save_result->get_save_result();
	if(save_result.status == FileSaveResult::CREATE_FAILED) //存储失败
	{
		LOG_ERROR("chunk create file failed. fid="<<fid);
		m_protocol_family.destroy_protocol(protocol_file);
		m_protocol_family.destroy_protocol(protocol_save_result);
		return false;
	}
	m_protocol_family.destroy_protocol(protocol_file);
	m_protocol_family.destroy_protocol(protocol_save_result);

	//2 发送分片
	while(seg_offset < filesize)
	{
		byte_buffer.clear();
		seg_size = filesize-seg_offset;
		if(seg_size > READ_SIZE)
			seg_size = READ_SIZE;

		//设置协议字段
		protocol_file = (ProtocolFile *)m_protocol_family.create_protocol(PROTOCOL_FILE);
		assert(protocol_file != NULL);
		FileSeg &file_seg = protocol_file->get_file_seg();
		file_seg.set(FileSeg::FLAG_SEG, fid, filename, filesize, seg_offset, 0, seg_size);
		seg_offset += seg_size;

		if(!_send_file_protocol_to_chunk(&trans_socket, protocol_file, &byte_buffer, fd))
		{
			result = false;
			m_protocol_family.destroy_protocol(protocol_file);

			LOG_ERROR("send file_seg failed.fid="<<fid);
			break;
		}

		//接收存储结果
		ProtocolFileSaveResult *protocol_save_result = (ProtocolFileSaveResult*)m_protocol_family.create_protocol(PROTOCOL_FILE_SAVE_RESULT);
		assert(protocol_save_result != NULL);
		if(!TransProtocol::recv_protocol(&trans_socket, protocol_save_result))
		{
			result = false;
			m_protocol_family.destroy_protocol(protocol_file);
			m_protocol_family.destroy_protocol(protocol_save_result);

			LOG_ERROR("receive file_seg_save_result failed. fid="<<fid);
			break;
		}

		FileSaveResult &save_result = protocol_save_result->get_save_result();
		if(save_result.status == FileSaveResult::SEG_FAILED) //存储失败
		{
			result = false;
			m_protocol_family.destroy_protocol(protocol_save_result);

			LOG_ERROR(logger, "chunk save file_seg failed. fid="<<fid);
			break;
		}

		m_protocol_family.destroy_protocol(protocol_file);
		m_protocol_family.destroy_protocol(protocol_save_result);
	}
	close(fd);

	if(result == false)
		return false;

	//3 发送结束协议(该协议没有回复)
	protocol_file = (ProtocolFile *)m_protocol_family.create_protocol(PROTOCOL_FILE);
	assert(protocol_file != NULL);
	FileSeg &end_file_seg = protocol_file->get_file_seg();
	end_file_seg.flag = FileSeg::FLAG_END;
	end_file_seg.fid = fid;
	end_file_seg.filesize = filesize;
	end_file_seg.size = 0;
	if(!_send_file_protocol_to_chunk(&trans_socket, protocol_file, &byte_buffer, -1))
	{
		LOG_ERROR("send end_file_seg failed. fid="<<fid);
		m_protocol_family.destroy_protocol(protocol_file);
		return false;
	}
	m_protocol_family.destroy_protocol(protocol_file);

	//等待chunk回复file_info信息
	ProtocolFileInfo *protocol_file_info = (ProtocolFileInfo*)m_protocol_family.create_protocol(PROTOCOL_FILE_INFO);
	assert(protocol_file_info != NULL);
	if(!TransProtocol::recv_protocol(&trans_socket, protocol_file_info))
	{
		LOG_ERROR("receive file_info failed. fid="<<fid);
		m_protocol_family.destroy_protocol(protocol_file_info);
		return false;
	}
	file_info = protocol_file_info->get_fileinfo();
	m_protocol_family.destroy_protocol(protocol_file_info);

	return true;
}

bool File::_save_fileseg_to_file(int fd, FileSeg &file_seg)
{
	lseek(fd, file_seg.offset, SEEK_SET);
	ssize_t size = write(fd, file_seg.data, file_seg.size);
	return size == file_seg.size;
}
