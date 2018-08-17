#include "pfm.h"

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
	ifstream pageFileRead(fileName.c_str(),ios::binary);
	if(!pageFileRead.good())
	{
		ofstream pageFileWrite1;
		pageFileWrite1.open(fileName.c_str(),ios::binary);
		return 0;
	}
    return -1;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
	ifstream pageFileRead(fileName.c_str(),ios::binary);
	if(pageFileRead.good())
	{
		remove(fileName.c_str());
		return 0;
	}
    return -1;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	ifstream pageFileRead(fileName.c_str(),ios::binary);
	if(pageFileRead.good())
	{
		std::fstream pageFileStream(fileName.c_str());
		if (!pageFileStream.is_open())
			return -1;
		pageFileStream.open(fileName.c_str(),ios::in | ios::out | ios::app | ios::binary);
		if (pageFileStream.is_open())
		{
			fileHandle.setFileName(fileName.c_str());
			return 0;
		}
	}

    return -1;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	string fileName = fileHandle.getFileName();
	ifstream pageFileRead(fileName,ios::binary);
	if(pageFileRead.good())
	{
		fstream pageFileStream;
		pageFileStream.open(fileName,ios::in | ios::out | ios::app | ios::binary);
		if (pageFileStream.is_open())
		{
			pageFileStream.close();
			fileHandle.setFileName("");
			return 0;
		}
	}
    return -1;
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
	fileName = "";
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
	if(pageNum<=getNumberOfPages())
	{
//	Shefali:pageNum=pageNum-1;
		string fileName = getFileName();
		fstream pageRead;
		pageRead.open(fileName,ios::in|ios::binary);
		pageRead.clear();
		pageRead.seekg(pageNum*PAGE_SIZE,pageRead.beg);
		if(pageRead.is_open())
		{
			pageRead.read((char *)data,PAGE_SIZE);
			readPageCounter+=1;
			pageRead.close();
			return 0;
		}
	}
    return -1;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	if(!pageNum || pageNum<=getNumberOfPages()) //if first page or page number is present already, then write
	{
		//Shefali:pageNum=pageNum-1;
	 	string fileName = getFileName();
		fstream pageWrite1;
		pageWrite1.open(fileName,ios::out|ios::in|ios::binary);
		if (pageWrite1.is_open())
		{
			pageWrite1.clear();
			pageWrite1.seekp(0,pageWrite1.beg);
			pageWrite1.seekp(pageNum*PAGE_SIZE,pageWrite1.beg);
			pageWrite1.write((char *)data,PAGE_SIZE);
			pageWrite1.flush();
			pageWrite1.close();
			writePageCounter+=1;
			return 0;
		}
	}
	return -1;
}


RC FileHandle::appendPage(const void *data)
{
	string fileName = getFileName();
	int pageNum1 = getNumberOfPages();
	pageNum1 = (pageNum1>0)?pageNum1+1:pageNum1;
	fstream pageRead(fileName,ios::in|ios::binary);
	if(pageRead.good())
	{
		fstream pageAppend1;
		pageAppend1.open(fileName,ios::app|ios::binary);
		if (pageAppend1.is_open())
		{

			pageAppend1.clear();
			pageAppend1.seekp(0,pageAppend1.end);
			pageAppend1.seekp(pageNum1*PAGE_SIZE,pageAppend1.beg);
			pageAppend1.write((char *)data,PAGE_SIZE);
			appendPageCounter+=1;
			pageAppend1.flush();
			pageAppend1.close();
			return 0;
		}
	}
    return -1;
}


unsigned FileHandle::getNumberOfPages()
{
	fstream pageAppend1;
	pageAppend1.open(fileName,ios::in|ios::binary);
	if (pageAppend1.is_open())
	{
		pageAppend1.clear();
		pageAppend1.seekg(0,pageAppend1.end);
		return pageAppend1.tellg()/PAGE_SIZE;
	}
	else
	{
		return 0;
	}
}

RC FileHandle::setFileName(const string &fileNameInput){
	fileName = fileNameInput;
	return 0;
}

string FileHandle::getFileName(){
	return fileName;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{	
	readPageCount = readPageCounter;
	writePageCount = writePageCounter;
	appendPageCount = appendPageCounter;
    return 0;
}
