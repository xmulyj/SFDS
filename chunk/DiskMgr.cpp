/*
 * DiskMgr.cpp
 *
 *  Created on: Aug 16, 2013
 *      Author: tim
 */
#include "DiskMgr.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <errno.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include <sys/statfs.h>

IMPL_LOGGER(ChunkWorker, logger);

#define LOCK(lock) pthread_mutex_lock(&lock)
#define UNLOCK(lock) pthread_mutex_unlock(&lock)

void DiskMgr::MakePath(string &path, string &fid, int index)
{
	char buf[256];
	if(index >= 0)
		sprintf(buf, "%s/%c%c/%02X", m_disk_path.c_str(), fid[0], fid[1], index);
	else
		sprintf(buf, "%s/%c%c", m_disk_path.c_str(), fid[0], fid[1]);
	path = buf;
}

void DiskMgr::Init(ConfigReader *config)
{
	m_Config = config;
	assert(m_Config != NULL);
	m_disk_path = m_Config->GetValue("DiskDir", "/tmp/sfs_chunk"); //数据存放路径
	m_disk_space = 0L;
	m_disk_used = 0L;

	pthread_mutex_init(&m_disk_lock, NULL);
	LoadDisk(); //加载磁盘
	Update(); //更新磁盘信息
}

void DiskMgr::Uninit()
{
	pthread_mutex_destroy(&m_disk_lock);
	UnloadDisk();
}

void DiskMgr::LoadDisk()
{
	int i;
	char pre_fix[3];
	struct stat path_stat;

	//目录不存在
	if(stat(m_disk_path.c_str(), &path_stat)==-1 && errno==ENOENT)
	{
	int result = mkdir(m_disk_path.c_str(), S_IRWXU);
	assert(result == 0);
	}

	//加载00,01,...,FF 256个子目录
	for(i=0; i<DIR_NUM; ++i)
	{
		int index = 0;
		sprintf(pre_fix, "%02X", i);
		m_disk_files[i].pre_fix = pre_fix;
		m_disk_files[i].fp = NULL;
		m_disk_files[i].index = -1;
		m_disk_files[i].cur_pos = 0;
		pthread_mutex_init(&m_disk_files[i].lock, NULL);

		string sub_dir = m_disk_path+"/"+pre_fix;
		LOG_DEBUG(logger, "checking chunk_dir="<<sub_dir);
		if(stat(sub_dir.c_str(), &path_stat)==-1 && errno==ENOENT) //子目录不存在
		{
			LOG_INFO(logger, "sub dir="<<sub_dir<<" not exists. create it.");
			if(mkdir(sub_dir.c_str(), S_IRWXU) == -1)
			{
				LOG_ERROR(logger, "make dir failed.sub_dir="<<sub_dir<<" error=<<strerror(errno)");
				continue;
			}
		}
		else if(!S_ISDIR(path_stat.st_mode)) //不是目录
		{
			LOG_ERROR(logger, "not dir.sub_dir="<<sub_dir);
			continue;
		}

		//open file
		struct dirent* ent = NULL;
		DIR *dir;
		if((dir=opendir(sub_dir.c_str())) == NULL)
		{
			LOG_ERROR(logger, "open dir error. sub_dir="<<sub_dir" error="<<strerror(errno));
			continue;
		}
		while((ent=readdir(dir)) != NULL) //计算文件数
		{
			if(strcmp( ".",ent->d_name) == 0 || strcmp( "..",ent->d_name) == 0)
				continue;
			++index;
		}
		closedir(dir);
		if(index > 0) //最后一个文件
			--index;

		char name[256];
		sprintf(name, "%s/%02X", sub_dir.c_str(), index);
		m_disk_files[i].fp= fopen(name, "a");
		assert(m_disk_files[i].fp != NULL);
		//fseek(m_disk_files[i].fp, 0L, SEEK_END);
		m_disk_files[i].cur_pos = (uint32_t)ftell(m_disk_files[i].fp);
		m_disk_files[i].index = index;
		//LOG_DEBUG(logger, "load file succ. name="<<name<<" size="<<m_disk_files[i].cur_pos);
	}
}
void DiskMgr::UnloadDisk()
{
	int i;
	for(i=0; i<DIR_NUM; ++i)
	{
		if(m_disk_files[i].fp != NULL)
			fclose(m_disk_files[i].fp);
		pthread_mutex_destroy(&m_disk_files[i].lock);
	}
}

bool DiskMgr::SaveFileToDisk(string &fid, char *buf, uint32_t size, ChunkPath &chunkpath)
{
	const char *temp = fid.c_str();
	int i=0, index= 0;
	for(; i<2; ++i) //get index
	{
		index *= 16;
		if(temp[i]>='A' && temp[i]<='F')
			index += temp[i]-'A'+10;
		else if(temp[i]>='a' && temp[i]<='f')
			index += temp[i]-'a'+10;
		else if(temp[i]>='0' && temp[i]<='9')
			index += temp[i]-'0';
		else
			return false;
	}
	assert(index>=0 && index<DIR_NUM);
	DiskFile &disk_file = m_disk_files[index];
	assert(disk_file.fp != NULL);

	LOCK(disk_file.lock);

	if(disk_file.cur_pos+size > MAX_FLESIZE) //重新打开文件
	{
		fclose(disk_file.fp);
		string path;
		MakePath(path, fid, ++disk_file.index);
		disk_file.fp = fopen(path.c_str(), "w");
		assert(disk_file.fp != NULL);
		disk_file.cur_pos = 0;
	}

	chunkpath.id = m_Config->GetValue("ChunkID", "");
	assert(chunkpath.id != "");
	chunkpath.ip = m_Config->GetValue("ChunkIP", "");
	assert(chunkpath.ip != "");
	chunkpath.port = m_Config->GetValue("ChunkPort", -1);
	assert(chunkpath.port != -1);

	chunkpath.index = disk_file.index;
	chunkpath.offset = disk_file.cur_pos;

	fwrite(buf, 1, size, disk_file.fp);
	fflush(disk_file.fp);
	disk_file.cur_pos += size;

	UNLOCK(disk_file.lock);
	return true;
}

void DiskMgr::Update()
{
	LOCK(m_disk_lock);

	LOG_DEBUG(logger, "start update disk manager.");
	m_disk_space = 0L;
	m_disk_used = 0L;
	struct statfs disk_statfs;
	if(statfs(m_disk_path.c_str(), &disk_statfs) >= 0)
	{
		m_disk_space = ((uint64_t)disk_statfs.f_bsize*(uint64_t)disk_statfs.f_blocks)>>10; //KB
		m_disk_used = m_disk_space - (((uint64_t)disk_statfs.f_bsize*(uint64_t)disk_statfs.f_bfree)>>10); //KB
	}
	else
		LOG_ERROR(logger, "statfs error. errno="<<errno<<"("<<strerror(errno)<<". set total_space=0, used_space=0.");

	UNLOCK(m_disk_lock);
}

void DiskMgr::GetDiskSpace(uint64_t &total, uint64_t &used)
{
	LOCK(m_disk_lock);

	total = m_disk_space;
	used = m_disk_used;

	UNLOCK(m_disk_lock);
}



