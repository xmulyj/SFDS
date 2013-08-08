/*
 * sha1.h
 *
 *  Created on: 2012-11-6
 *      Author: LiuYongJin
 */

#ifndef _LIB_SHA1_H_
#define _LIB_SHA1_H_

#include <openssl/sha.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

#include <string>
using std::string;

class SHA1
{
public:
	//计算文件file的sha1值sha.成功返回0,失败返回-1
	static bool hash(string &file, string &sha);
};

bool SHA1::hash(string &file, string &sha)
{
	unsigned char buf[1024];
	int fd = open(file.c_str(), O_RDONLY);
	if(fd == -1)
		return -1;

	SHA_CTX sha_ctx;
	unsigned char sha_buf[SHA_DIGEST_LENGTH] = {0};
	int result = 0;
	int size = 0;

	result = SHA1_Init(&sha_ctx);
	while(result==1 && (size = read(fd, buf, 1024)) >0)
		result = SHA1_Update(&sha_ctx, buf, size);
	if(result == 1)
	{
		if((result=SHA1_Final(sha_buf, &sha_ctx)) == 1)
		{
			char *ptr, temp[SHA_DIGEST_LENGTH*2];
			int i = 0;
			for(ptr=temp; i<SHA_DIGEST_LENGTH; i++, ptr+=2)
				sprintf(ptr, "%02X", (int)sha_buf[i]);
			sha.assign(temp, SHA_DIGEST_LENGTH*2);
		}
	}

	close(fd);
	return result==1?true:false;
}

#endif //_LIB_SHA1_H_



