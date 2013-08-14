/*
 * ChunkInterfaceMain.cpp
 *
 *  Created on: 2013-08-14
 *      Author: tim
 */

#include "ChunkInterface.h"

int main(int argc, char *argv[])
{
	INIT_LOGGER("../config/log4cplus.conf");

	ChunkInterface application;
	application.Start();
	
	return 0;
}

