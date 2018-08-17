#include <iostream>
#include <stdio.h>
#include "rbfm.h"
#include <math.h>
#include <string>
#include <iomanip>
#include <sstream>
#include <cstring>
using namespace std;

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance() {
	if (!_rbf_manager)
	_rbf_manager = new RecordBasedFileManager();

	return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() :
	pfm(*PagedFileManager::instance()) {

}
RBFM_ScanIterator::RBFM_ScanIterator() {
	rid_next.pageNum = 1;
	rid_next.slotNum = 1;
}

RBFM_ScanIterator::~RBFM_ScanIterator() {

}

RecordBasedFileManager::~RecordBasedFileManager() {
}
RC RecordBasedFileManager::createFile(const string &fileName) {
	RC cf = pfm.createFile(fileName.c_str());
	if (cf != 0)
	{return -1;}
	return 0;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	return PagedFileManager::instance()->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName,
	FileHandle &fileHandle) {
	RC of = pfm.openFile(fileName.c_str(), fileHandle);
	if (of != 0) {
	return of;
	}
	return 0;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
	return PagedFileManager::instance()->closeFile(fileHandle);
}
RC RecordBasedFileManager::findRecordLength(
	const vector<Attribute> &recordDescriptor, const void *data) {

	bool* nullArray = (bool*) malloc(recordDescriptor.size());
	memset(nullArray,0,recordDescriptor.size());
	int r = getNullBits(recordDescriptor, data,nullArray);
	int nullBitsIndicatorActualSize = ceil(recordDescriptor.size() / 8.0);
	int recordLength = nullBitsIndicatorActualSize;
	int dataIndex = nullBitsIndicatorActualSize;
	bool nullArraycheck=0;
	for (int i = 0; i < recordDescriptor.size(); i++) {
	int count = 0;
	if (recordDescriptor[i].type == TypeVarChar) {
	nullArraycheck = nullArray[i];
	if (nullArraycheck == 0) {
	memcpy(&count, (char*) data + dataIndex, sizeof(int));
	dataIndex += sizeof(int) + count;
	recordLength += sizeof(int) + count;
	}
	} else {
        nullArraycheck = nullArray[i];
        if (nullArraycheck == 0) {
	recordLength += sizeof(int);
	dataIndex += sizeof(int);
	}

	}
	}
	free(nullArray);
	return recordLength;
}
RC RecordBasedFileManager::getNullBits(
	const vector<Attribute> &recordDescriptor, const void *data,bool* nullarr) {

	int nullBitsIndicatorActualSize = ceil(recordDescriptor.size() / 8.0);
	unsigned char* bitArray = (unsigned char*) malloc(
	nullBitsIndicatorActualSize);
	memset(bitArray,0,nullBitsIndicatorActualSize);
	memcpy(bitArray, (char*) data, nullBitsIndicatorActualSize);
	int j=0;
	for (int i = 0; i < recordDescriptor.size(); i++) {
	int k = int(i / 8);
	nullarr[j] = bitArray[k] & (1 << (7 - i % 8));
	j++;
	}
	free(bitArray);
	return 0;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle,
	const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {

	int recordLength = findRecordLength(recordDescriptor, data);
	int numOfPages = (int) fileHandle.getNumberOfPages();
	void *pagebuffer = malloc(PAGE_SIZE);
	short freeSpace;
	short slots;
	//ADDED...TO CHECK....
//	printRecord(recordDescriptor,data);

	//------------------------//
	//If file is empty---No page
	if (numOfPages == 0) {
	int slotNumber = copyRecordToMemoryReusingSlots(pagebuffer, data,
	recordLength, PAGE_SIZE, 0);
	//Append page
	int appendPage = fileHandle.appendPage(pagebuffer);
	numOfPages = fileHandle.getNumberOfPages();
	//Update RID fields for record
//	cout<<"Case of fresh heap file."<<endl;
	rid.pageNum = numOfPages;
	rid.slotNum = slotNumber;
	}
	//Page already exists
	else {
	//Check if current/last page has sufficient space.
	int currentPageNumber = numOfPages;
	int readCurrentPage = fileHandle.readPage(currentPageNumber-1,
	pagebuffer);
	freeSpace = findFreeSpace(pagebuffer);
	slots = findSlotsCount(pagebuffer);

	if (freeSpace >= (recordLength + 4)) {
	//Insert record in current page. Call copyRecordToMemory method.
	int slotNumber = copyRecordToMemoryReusingSlots(pagebuffer, data,
	recordLength, freeSpace, slots);
	int writePage = fileHandle.writePage(currentPageNumber -1, pagebuffer);
	rid.pageNum = currentPageNumber;
	rid.slotNum = slotNumber;
	} else {
	//Iterate through pages from beginning of file to find sufficient freespace.
	//Insert record on whichever page has enough space.
	//If none has, append a page and insert record.
	int insertionDone = 0;
	for (int i = 1; i < numOfPages; i++) {
	int readCurrentPage = fileHandle.readPage(i-1, pagebuffer);
	freeSpace = findFreeSpace(pagebuffer);

	slots = findSlotsCount(pagebuffer);

	if (freeSpace >= recordLength + 4) {
	int slotNumber = copyRecordToMemoryReusingSlots(pagebuffer,
	data, recordLength, freeSpace, slots);
	int writePage = fileHandle.writePage(i -1, pagebuffer);
	insertionDone = 1;
	rid.slotNum = slotNumber;
	rid.pageNum = i;
	break;
	}

	}
	if (insertionDone == 0) {
	//None of the pages had sufficient space. Append new page.
	int slotNumber = copyRecordToMemoryReusingSlots(pagebuffer,
	data, recordLength, 4096, 0);
	int appendPage = fileHandle.appendPage(pagebuffer);
	numOfPages = fileHandle.getNumberOfPages();
	rid.pageNum = numOfPages;
	rid.slotNum = slotNumber;
	}
	}

	}
	free(pagebuffer);
//	cout <<"Page:"<<rid.pageNum <<", "<<rid.slotNum<<endl;
	return 0;
}

RC RecordBasedFileManager::findFreeSpace(const void *data) {
	short freespace;
	memcpy(&freespace, (char*) data + 4096 - 2, 2);
	return freespace;
}
RC RecordBasedFileManager::findSlotsCount(const void *data) {
	short slots;

	memcpy(&slots, (char*) data + PAGE_SIZE - 4, 2);
	return slots;
}
RC RecordBasedFileManager::findOffsetToEndOfLastRecordOnPage(
	const void *pagebuffer) {
	int freespace = findFreeSpace(pagebuffer);

	int slotDirectorySize = findSlotDirectorySize(pagebuffer);

	int offsetOfLastRecordOnPage = PAGE_SIZE - freespace - slotDirectorySize;
	return offsetOfLastRecordOnPage;
}

RC RecordBasedFileManager::findSlotDirectorySize(const void *pagebuffer) {
	int numOfSlots = findSlotsCount(pagebuffer);
	int slotDirectorySize = 2 + 2 + numOfSlots * 4;
	return slotDirectorySize;
}
RC RecordBasedFileManager::copyRecordToMemory(const void* pagebuffer,
	const void* record, short recordLength, short freespace, short slots) //NOT IN USE
	{

	short lengthOfLastRecord = 0;
	short offsetofLastRecord = 0;
	if (slots > 0) {

	memcpy(&offsetofLastRecord,
	(char*) pagebuffer + 4096 - 2 - 2 - slots * 4, 2); // 4096-freespaceMemory-slotsMemory-NoOfSlots*4(to reach length value location of previous/last record
	memcpy(&lengthOfLastRecord,
	(char*) pagebuffer + 4096 - 2 - 2 - slots * 4 + 2, 2); // 4096-freespaceMemory-slotsMemory-NoOfSlots*4+2(to reach offset of previous/last record
	}

	//step -1 - copy data
	short newOffset = offsetofLastRecord + lengthOfLastRecord;
	memcpy((char*) pagebuffer + newOffset, (char*) record, recordLength);

	// Step -2 -- freespace - 2 bytes
	if (slots > 0)
	freespace = freespace - recordLength - 4;
	else
	freespace = freespace - recordLength - 8;

	memcpy((char*) pagebuffer + 4096 - 2, &freespace, 2);

	//Step-3 -- slot - 2 bytes
	slots = slots + 1;
	memcpy((char*) pagebuffer + 4096 - 4, &slots, 2);

	//step-4 -- offset
	memcpy((char*) pagebuffer + 4096 - 2 - 2 - slots * 4, &newOffset, 2);

	//step-5 -- length of the record  - 4 bytes
	memcpy((char*) pagebuffer + 4096 - 2 - 2 - slots * 4 + 2, &recordLength, 2);

}
RC RecordBasedFileManager::copyRecordToMemoryReusingSlots(
	void* pagebuffer, const void* record, short recordLength,
	short freespace, short slots) //Returns slot#
	{

	short newSlot = 0;
	short newOffset = 0;
	if (slots < 1) //Fresh page
	{
	newSlot = 1;
	memcpy((char*) pagebuffer + newOffset, (char*) record, recordLength);
		string str1 = string((char*) record, recordLength);
	freespace = freespace - recordLength - 8;
	memcpy((char*) pagebuffer + PAGE_SIZE - 2, &freespace, 2);
	memcpy((char*) pagebuffer + PAGE_SIZE - 4, &newSlot, 2);
	memcpy((char*) pagebuffer + PAGE_SIZE - 2 - 2 - newSlot * 4, &newOffset,
	2);
	memcpy((char*) pagebuffer + PAGE_SIZE - 2 - 2 - newSlot * 4 + 2,
	&recordLength, 2);
	} else {
//Occupied page. Find slot where some record had been deleted (RL=INVALID) or UPDATED.If deleted case, Reuse the slot.
//Iterate through slot directory to find a slot with Record length as INVALID. Reuse this slot, and insert record here.
//Change the offsets of all the records to the right by record length of new record.
//Update freespace and slots.
	int emptySlotPresent = 0;
	for (int i = 1; i <= slots; i++) {
	short recordLengthAtI = 0;
	memcpy(&recordLengthAtI,(char*) pagebuffer + PAGE_SIZE - 4 - i * 4 + 2, 2);
	if(recordLengthAtI == TOMBSTONE_SIZE)
	{
	i=i+1; //continue.
	}
	else if (recordLengthAtI == INVALID ) {
	emptySlotPresent = 1;
	newSlot = i;
	newOffset = findOffsetToEndOfLastRecordOnPage(pagebuffer);
	memcpy((char*) pagebuffer + newOffset,
	(char*) record, recordLength);
	memcpy((char*) pagebuffer + PAGE_SIZE - 4 - i * 4,
	&newOffset, 2);
	memcpy((char*) pagebuffer + PAGE_SIZE - 4 - i * 4 + 2,
	&recordLength, 2);
	freespace = freespace - recordLength;
	memcpy((char*) pagebuffer + PAGE_SIZE - 2, &freespace, 2);
	i=slots; //To break for loop
	}
	}

	if (emptySlotPresent == 0) {
	newOffset = findOffsetToEndOfLastRecordOnPage(pagebuffer);
	//step -1 - copy data
	memcpy((char*) pagebuffer + newOffset, (char*) record,
	recordLength);

	// Step -2 -- freespace - 2 bytes
	freespace = freespace - recordLength - 4;
	memcpy((char*) pagebuffer + PAGE_SIZE - 2, &freespace, 2);

	//Step-3 -- slot - 2 bytes
	newSlot = slots + 1;
	memcpy((char*) pagebuffer + PAGE_SIZE - 4, &newSlot, 2);

	//step-4 -- offset
	memcpy((char*) pagebuffer + PAGE_SIZE - 2 - 2 - newSlot * 4,
	&newOffset, 2);

	//step-5 -- length of the record  - 4 bytes
	memcpy((char*) pagebuffer + PAGE_SIZE - 2 - 2 - newSlot * 4 + 2,
	&recordLength, 2);
	}
	}
	return newSlot;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle,
	const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {

	void *tmpdata = malloc(PAGE_SIZE);
	int rp=fileHandle.readPage(rid.pageNum-1, tmpdata);
	if(rp!=0)
{  return -1;
}
	short offset = 0;
	//Iterate from end of page(tmpdata) until record offset data and store value in variable offset.
	int offsetOffset = PAGE_SIZE - 4 - (rid.slotNum * 4);

	memcpy(&offset, (char*) tmpdata + offsetOffset, 2);
	short slotsInPage = findSlotsCount(tmpdata);
	short freespace = findFreeSpace(tmpdata);
	short recordLength = 0;
	memcpy(&recordLength,(char*) tmpdata + (PAGE_SIZE - 4 - (rid.slotNum) * 4 + 2), 2);
	if(recordLength==INVALID)
	{
	return -1; //Record does not exist.
	}
	else if (recordLength == TOMBSTONE_SIZE) {

	RID ridAtNextPage;
	ridAtNextPage.pageNum = 0;
	ridAtNextPage.slotNum = 0;
	memcpy(&ridAtNextPage.pageNum, (char*) tmpdata + offset, 2);
	memcpy(&ridAtNextPage.slotNum, (char*) tmpdata + offset + 2, 2);
	void *nextPage=malloc(PAGE_SIZE);
	int readPage=fileHandle.readPage(ridAtNextPage.pageNum -1,nextPage);
	short offsetAtNextPage=0;
	short lengthRec=0;
	memcpy(&offsetAtNextPage,(char*)nextPage+PAGE_SIZE-4-(ridAtNextPage.slotNum)*4,2);
	memcpy(&lengthRec,(char*)nextPage+PAGE_SIZE-4-(ridAtNextPage.slotNum*4)+2,2);
	memcpy((char*)data,(char*)nextPage+offsetAtNextPage,lengthRec);
	free(nextPage);
	return 0;
	}
	else
	{
	//Iterate from beginning of tmpdata until record offset, and store record in data
	memcpy((char*) data, (char*) tmpdata + offset, recordLength);
	free(tmpdata);
	return 0;
	}
}

RC RecordBasedFileManager::printRecord(
	const vector<Attribute> &recordDescriptor, const void *data) {


	int nullBitsIndicatorActualSize = ceil(recordDescriptor.size() / 8.0);
	bool* nullArray = (bool*) malloc(recordDescriptor.size());
	memset(nullArray, 0, recordDescriptor.size());
	int r = getNullBits(recordDescriptor, data,nullArray);
	int dataIndex = nullBitsIndicatorActualSize;
	bool nullArraycheck = 0;
	cout << "(";
	for (int i = 0; i < recordDescriptor.size(); i++) {
	int count = 0;
	if (recordDescriptor[i].type == TypeVarChar) {
	nullArraycheck = nullArray[i];
	if (nullArraycheck == 0) {
	memcpy(&count, (char*) data + dataIndex, sizeof(int));
	dataIndex += sizeof(int);
	string str1 = string((char*) data + dataIndex, count);
	cout << setw(20) << str1;
	dataIndex += count;
	}
	} else if (recordDescriptor[i].type == TypeReal) {
        nullArraycheck = nullArray[i];
        if (nullArraycheck == 0) {
	float val;
	memcpy(&val, (char*) data + dataIndex, sizeof(float));
	cout << setw(20) << val;
	dataIndex += sizeof(float);
	}

	} else if (recordDescriptor[i].type == TypeInt) {
        nullArraycheck = nullArray[i];
        if (nullArraycheck == 0) {
	int val;
	memcpy(&val, (char*) data + dataIndex, sizeof(int));
	cout << setw(20) << val;
	dataIndex += sizeof(int);
	}

	}

	}
	cout << ")";
	cout << endl;

	free(nullArray);
	return 0;

}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle,const vector<Attribute> &recordDescriptor, const RID &rid) {
	void *oldPagebuffer =(void*) malloc(PAGE_SIZE);
	void *newPagebuffer = (void*)malloc(PAGE_SIZE);
	int readPage = fileHandle.readPage(rid.pageNum -1, oldPagebuffer);
	if (readPage != 0) {
	return -1; //Page not read.
	}
	//Old page data
	short slotsInPage = findSlotsCount(oldPagebuffer);
	short freespaceInPage = findFreeSpace(oldPagebuffer);


	//Record data
	short offset = 0;
	short recordLength = 0;
	memcpy(&offset, (char*) oldPagebuffer + PAGE_SIZE - 4 - rid.slotNum * 4, 2);
	memcpy(&recordLength,(char*) oldPagebuffer + PAGE_SIZE - 4 - rid.slotNum * 4 + 2, 2);

	void *record=malloc(PAGE_SIZE);
	memcpy((char*)record,(char*)oldPagebuffer+offset,recordLength);
	int recordLength1=findRecordLength(recordDescriptor,record);
	free(record);
	if (recordLength == -1) {
	return -1;
	}
	else if (recordLength == TOMBSTONE_SIZE) {
	RID ridAtNextPage;
	ridAtNextPage.pageNum = 0;
	ridAtNextPage.slotNum = 0;

	memcpy(&ridAtNextPage.pageNum, (char*) oldPagebuffer + offset, 2);
	memcpy(&ridAtNextPage.slotNum, (char*) oldPagebuffer + offset + 2,
	2);
	//Delete record residing on other page
	void *nextPageBuffer=malloc(PAGE_SIZE);
	int readpage=fileHandle.readPage(ridAtNextPage.pageNum -1,nextPageBuffer);
	short freespaceAtNextPage=findFreeSpace(nextPageBuffer);
	short slotsAtNextPage=findSlotsCount(nextPageBuffer);
	int slotDirectorySize=findSlotDirectorySize(nextPageBuffer);
	short offsetRec=0;
	short lengthRec=0;
	memcpy(&offsetRec, (char*) nextPageBuffer + PAGE_SIZE - 4 - ridAtNextPage.slotNum * 4, 2);
	memcpy(&lengthRec,(char*) nextPageBuffer + PAGE_SIZE - 4 - ridAtNextPage.slotNum * 4+2, 2);
	for(int i=1;i<=slotsAtNextPage;i++)
	{
	short offsetAtI;
	memcpy(&offsetAtI,(char*)nextPageBuffer+PAGE_SIZE-4-i*4,2);
	if(offsetAtI>offsetRec)
	{
	offsetAtI=offsetAtI-lengthRec;
	memcpy((char*)nextPageBuffer+PAGE_SIZE-4-i*4,&offsetAtI,2);
	}
	if (offsetAtI == offsetRec && i==ridAtNextPage.slotNum)
	{
	short rl=INVALID;
	memcpy((char*) nextPageBuffer + PAGE_SIZE - 4 - i * 4+2,&rl, 2);
	}
	}
	//Update freespace
	freespaceAtNextPage=freespaceAtNextPage+lengthRec;
	memcpy((char*)nextPageBuffer+PAGE_SIZE-2,&freespaceAtNextPage,2);
	void *tempNextPagebuffer=malloc(PAGE_SIZE);
	memcpy((char*)tempNextPagebuffer,(char*)nextPageBuffer,offsetRec);

	int slotDirSize = findSlotDirectorySize(oldPagebuffer);
	memcpy((char*) tempNextPagebuffer+offsetRec, (char*) nextPageBuffer + offsetRec+lengthRec,(PAGE_SIZE -(short) slotDirSize - freespaceInPage - offsetRec-lengthRec));
	memcpy((char*) tempNextPagebuffer + PAGE_SIZE - (short)slotDirSize,
	(char*) nextPageBuffer + PAGE_SIZE - (short)slotDirSize, slotDirSize);
	int wrPage = fileHandle.writePage(ridAtNextPage.pageNum -1,
	tempNextPagebuffer);
	free(nextPageBuffer);
	free(tempNextPagebuffer);

	//OLD PAGE
	//---update offsets for all remaining records
	for (int i = 1; i <= slotsInPage; i++) {
	short offsetAtI = 0;
	memcpy(&offsetAtI, (char*) oldPagebuffer + PAGE_SIZE - 4 - i * 4,
	2);

	if (offsetAtI > offset) {
	offsetAtI = offsetAtI - 4;
	memcpy((char*) oldPagebuffer + PAGE_SIZE - 4 - i * 4,
	&offsetAtI, 2);
	}
	if (offsetAtI == offset  && i==rid.pageNum) {
	short rl=INVALID;
	memcpy((char*) oldPagebuffer + PAGE_SIZE - 4 - i * 4 + 2,
	&rl, 2);
	}
	}
	//Update freespace
	freespaceInPage=freespaceInPage+4;
	memcpy((char*)oldPagebuffer+PAGE_SIZE-2,&freespaceInPage,2);

	//Delete the 2 short variables from old page
	memcpy((char*) newPagebuffer, (char*) oldPagebuffer, offset);
	slotDirSize=findSlotDirectorySize(oldPagebuffer);
	memcpy((char*) newPagebuffer+offset,(char*) oldPagebuffer+offset+4,(PAGE_SIZE-(short)slotDirSize-freespaceInPage-offset-4));
	memcpy((char*) newPagebuffer+PAGE_SIZE-(short)slotDirSize,(char*)oldPagebuffer+PAGE_SIZE-(short)slotDirSize,slotDirSize);
	wrPage=fileHandle.writePage(rid.pageNum -1,newPagebuffer);
	if(wrPage!=0)
	return -1;
	}
	else
	{//NORMAL RECORD ON PAGE

	//---update offsets for all remaining records
	for (int i = 1; i <= slotsInPage; i++) {
	short offsetAtI = 0;
	memcpy(&offsetAtI, (char*) oldPagebuffer + PAGE_SIZE - 4 - i * 4,
	2);

	if (offsetAtI > offset) {
	offsetAtI = offsetAtI - recordLength;
	memcpy((char*) oldPagebuffer + PAGE_SIZE - 4 - i * 4,
	&offsetAtI, 2);
	}
	if (offsetAtI == offset && i==rid.slotNum) { //for record to be deleted
	short rl=-1;
	memcpy((char*) oldPagebuffer + PAGE_SIZE - 4 - i * 4 + 2,&rl, 2);
	short rlcheck=0;
	memcpy(&rlcheck,(char*)oldPagebuffer+PAGE_SIZE-4-i*4+2,sizeof(rlcheck));
	}
	}
	//Update freespace
	freespaceInPage = freespaceInPage + recordLength;
	memcpy((char*) oldPagebuffer + PAGE_SIZE - 2, &freespaceInPage, 2);

	//Copy data
	memcpy((char*) newPagebuffer, (char*) oldPagebuffer, offset);
	int slotDirSize=findSlotDirectorySize(oldPagebuffer);
	memcpy((char*) newPagebuffer+offset,(char*) oldPagebuffer+offset+recordLength,(PAGE_SIZE-(short)slotDirSize-freespaceInPage-recordLength));
	memcpy((char*)newPagebuffer+PAGE_SIZE-(short)slotDirSize,(char*)oldPagebuffer+PAGE_SIZE-(short)slotDirSize,slotDirSize);

	short temp3 = 0;
	memcpy(&temp3, (char*) newPagebuffer + PAGE_SIZE - 2, 2);

	short temp4 = 0;
	memcpy(&temp4, (char*) newPagebuffer + PAGE_SIZE - 4, 2);

	int wrPage=fileHandle.writePage(rid.pageNum -1 ,newPagebuffer);
	if(wrPage!=0)
	return -1;
	}

	free(oldPagebuffer);
	free(newPagebuffer);
	return 0;

}

// Assume the RID does not change after an update

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle,

const vector<Attribute> &recordDescriptor, const void *data,

const RID &rid) {

//Page information

	void *pagebuffer = malloc(PAGE_SIZE);

	int readPage = fileHandle.readPage(rid.pageNum -1, pagebuffer);

	short freeSpace = findFreeSpace(pagebuffer);

	short numofSlots = findSlotsCount(pagebuffer);

	int offsetOfLastRecord = 0;


//Old record information

	void *oldData = malloc(PAGE_SIZE);

	readRecord(fileHandle, recordDescriptor, rid, oldData);

	int oldRecordLength = findRecordLength(recordDescriptor, oldData);

	short oldRecordOffset = 0;

	memcpy(&oldRecordOffset,(char*) pagebuffer + PAGE_SIZE - 4 - rid.slotNum * 4,2);

//New record information

	int updatedRecordLength = findRecordLength(recordDescriptor, data);
	if (oldRecordLength == updatedRecordLength) {

	memcpy((char*) pagebuffer + oldRecordOffset, (char*) data,
	updatedRecordLength);

//Freespace on page, offset of other records, rid->page number and slot, all remain unchanged

	} else if (oldRecordLength < updatedRecordLength) {

	int differenceInLength = updatedRecordLength - oldRecordLength;

	if (freeSpace >= differenceInLength) {

	//Write on same page.
	int writePage = fileHandle.writePage(rid.pageNum -1, pagebuffer);


	int d=deleteRecord(fileHandle, recordDescriptor, rid);
	int readPage = fileHandle.readPage(rid.pageNum -1, pagebuffer);

	short newOffsetToInsert = findOffsetToEndOfLastRecordOnPage(
	pagebuffer);


	short updatedRecLen=(short)updatedRecordLength;
	memcpy((char*) pagebuffer + newOffsetToInsert, (char*) data,
	updatedRecordLength);

	memcpy((char*) pagebuffer + PAGE_SIZE - 4 - rid.slotNum * 4,
	&newOffsetToInsert, 2);

	memcpy((char*) pagebuffer + PAGE_SIZE - 4 - rid.slotNum * 4 + 2,
	&updatedRecLen, 2);

	freeSpace = freeSpace - (short)differenceInLength;

	memcpy((char*) pagebuffer + PAGE_SIZE - 2, &freeSpace, 2);

	} else {
	//INSERT IN A DIFFERENT PAGE. STORE TOMBSTONE.
	RID oldRid;

	oldRid.pageNum = rid.pageNum;

	oldRid.slotNum = rid.slotNum;

	RID ridOnNewPage;
	ridOnNewPage.pageNum = 0;
	ridOnNewPage.slotNum = 0;
	int writePage = fileHandle.writePage(rid.pageNum -1, pagebuffer);

	int ir = insertRecord(fileHandle, recordDescriptor, data,ridOnNewPage);
	int readPage = fileHandle.readPage(rid.pageNum -1, pagebuffer);

	if (ir == 0) {


	void *newPagebuffer = malloc(PAGE_SIZE);

	//---Copying data from old pagebuffer (0 to beginning of ToBeDeleted record) to new pagebuffer

	memcpy((char*) newPagebuffer, (char*) pagebuffer,oldRecordOffset);

	//---Creating tombstone.Inserting pointer to new page where updated record resides

	short newPagenum = ridOnNewPage.pageNum;

	short newSlotNum = ridOnNewPage.slotNum;

	memcpy((char*) newPagebuffer + oldRecordOffset, &newPagenum, 2);

	memcpy((char*) newPagebuffer + oldRecordOffset + 2, &newSlotNum,2);

	//Insert remaining data.
	int slotDirSize=findSlotDirectorySize(pagebuffer);
	memcpy((char*)newPagebuffer+oldRecordOffset+4,(char*)pagebuffer+oldRecordOffset+oldRecordLength,(PAGE_SIZE-slotDirSize-freeSpace-oldRecordOffset-oldRecordLength));

	//Changes in slot directory of pagebuffer
	//Change all offsets to right of OldRecordoffset .offset and RL at slot rid.slotNum
	for(int i=1;i<numofSlots;i++){
	short offsetAtI;
	memcpy(&offsetAtI,(char*)pagebuffer+PAGE_SIZE-4-i*4,2);
	if(offsetAtI>oldRecordOffset){
	offsetAtI=offsetAtI-oldRecordLength+4;
	}
	if (offsetAtI == oldRecordOffset && i == rid.slotNum) { //for record to be deleted
	//SET RL=-1

	short rl = TOMBSTONE_SIZE;
	memcpy((char*) pagebuffer + PAGE_SIZE - 4 - i * 4 + 2,&rl, 2);
	short rlcheck = 0;
	memcpy(&rlcheck,(char*) pagebuffer + PAGE_SIZE - 4 - i * 4 + 2,sizeof(rlcheck));
	}
	}

	//Update freespace on page.
	freeSpace=freeSpace+(short)oldRecordLength-4;
	memcpy((char*)pagebuffer+PAGE_SIZE-2,&freeSpace,2);

	//Copy slots directory
	memcpy((char*)newPagebuffer+PAGE_SIZE-slotDirSize,(char*)pagebuffer+PAGE_SIZE-slotDirSize,slotDirSize);

	//Copy newpagebuffer entirely in page buffer.
	memcpy((char*)pagebuffer,(char*)newPagebuffer,PAGE_SIZE);
	free(newPagebuffer);
	}

//RID->page number and slot remain unchanged

	}

	} else {  // CASE 3 Shrinking

//old record length > new record length

	int differenceInLength = updatedRecordLength - oldRecordLength;

	void *tempbuffer = malloc(PAGE_SIZE);

	memcpy((char*) tempbuffer, (char*) pagebuffer, oldRecordOffset);

	memcpy((char*) tempbuffer + oldRecordOffset, (char*) data,updatedRecordLength);

	int slotDirSize=findSlotDirectorySize(pagebuffer);
	memcpy((char*)tempbuffer+oldRecordOffset+(short)updatedRecordLength,(char*) pagebuffer+oldRecordOffset+(short)oldRecordLength,(PAGE_SIZE-(short)slotDirSize-freeSpace-oldRecordOffset-(short)oldRecordLength));



//-----Update freespace on page

	freeSpace = freeSpace + (short)differenceInLength;

	memcpy((char*) pagebuffer + PAGE_SIZE - 2, &freeSpace, 2);

//----Update slot directory for new record length
	short updatedRecLength=(short)updatedRecordLength;

	memcpy((char*) pagebuffer + PAGE_SIZE - 4 - (rid.slotNum) * 4 + 2,

	&updatedRecLength, 2);

//------Update offsets of all records on right of inserted record by Record length

	for (int j =  1; j <= numofSlots; j++) {

	short currentOffsetAtJ = 0;

	memcpy(&currentOffsetAtJ,
	(char*) pagebuffer + PAGE_SIZE - 4 - j * 4, 2);
	if(currentOffsetAtJ>oldRecordOffset)
	{
	short newOffsetAtJ = currentOffsetAtJ - (short)differenceInLength;

	memcpy((char*) pagebuffer + PAGE_SIZE - 4 - (short)j * 4, &newOffsetAtJ,
	2);

	}
	}

//-----Copy slot directory data from pagebuffer to temp buffer

	memcpy((char*)tempbuffer+PAGE_SIZE-(short)slotDirSize,(char*)pagebuffer+PAGE_SIZE-(short)slotDirSize,slotDirSize);

//-----Replace pagebuffer entirely with tempbuffer

	memcpy((char*) pagebuffer, (char*) tempbuffer, PAGE_SIZE);

	free(tempbuffer);

	}

//---Write pagebuffer onto disk

	int writePage = fileHandle.writePage(rid.pageNum -1, pagebuffer);

	free(pagebuffer);
	free(oldData);
	return 0;
}
RC RecordBasedFileManager::readAttributeFromRecord(void *record,const vector<Attribute> &recordDescriptor,const string &attributeName,void *data)
{ //
//	cout<<"Attribute name:"<<attributeName<<endl;
	bool nullArraycheck=0;
	bool* nullArray = (bool*) malloc(recordDescriptor.size());
	memset(nullArray, 0, recordDescriptor.size());
	int r = getNullBits(recordDescriptor, record,nullArray);
	//New bit array, to be returned with data
	unsigned char* bitArrayNew = (unsigned char *) malloc(1);
	memset(bitArrayNew,0,1);
	int nullBitsIndicatorActualSize = ceil(recordDescriptor.size() / 8.0);
	int dataIndex = nullBitsIndicatorActualSize;
		for (int i = 0; i < recordDescriptor.size(); i++) {
		int count = 0;
		if (recordDescriptor[i].type == TypeVarChar)
		{
		nullArraycheck = nullArray[i];
		if (nullArraycheck == 0) {
		memcpy(&count, (char*) record + dataIndex, sizeof(int));
		dataIndex += sizeof(int);
		if (recordDescriptor[i].name == attributeName) {
		bitArrayNew[0] = 0; //00000000
		memcpy((char*) data, &bitArrayNew, 1);
		memcpy((char*) data + 1, &count, sizeof(int));
		memcpy((char*) data + 1+sizeof(int), (char*) record + dataIndex, count);
		string s = string((char*) data + 5, count);
		}
		dataIndex += count;
		}
		else {
		if (recordDescriptor[i].name == attributeName) {
		bitArrayNew[0] = 128; //10000000
		memcpy((char*) data, &bitArrayNew, 1);
		}
		}
		}
		else { //TypeInt and TypeReal
		nullArraycheck = nullArray[i];
	    if (nullArraycheck == 0) {
		if (recordDescriptor[i].name == attributeName) {
		bitArrayNew[0] = 0; //00000000
		memcpy((char*) data, &bitArrayNew, 1);
		memcpy((char*) data + 1, (char*) record + dataIndex,
		sizeof(int));
		}
		dataIndex += sizeof(int);
		} else {
		if (recordDescriptor[i].name == attributeName) {
		bitArrayNew[0] = 128; //10000000
		memcpy((char*) data, &bitArrayNew, 1); //store NULL
		}
		}
		}

		}
		free(bitArrayNew);
		free(nullArray);
		return 0;
}
RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle,
	const vector<Attribute> &recordDescriptor, const RID &rid,
	const string &attributeName, void *data) {

	bool nullArraycheck=0;
	void *record = malloc(PAGE_SIZE);
	int rrec=readRecord(fileHandle, recordDescriptor, rid, record); //Record stored in 'record'
	if(rrec==-1)
	{
		return -1;
	}
	int nullBitsIndicatorActualSize = ceil(recordDescriptor.size() / 8.0);
	bool* nullArray=(bool*)malloc(recordDescriptor.size());
	memset(nullArray,0,recordDescriptor.size());
	int r=getNullBits(recordDescriptor,record,nullArray);

	//New bit array, to be returned with data
	unsigned char* bitArrayNew = (unsigned char *) malloc(1);
	memset(bitArrayNew,0,1);
	bitArrayNew[0] = 0; //00000000

	int dataIndex = nullBitsIndicatorActualSize;
	for (int i = 0; i < recordDescriptor.size(); i++) {
	int count = 0;
	if (recordDescriptor[i].type == TypeVarChar) {
	nullArraycheck = nullArray[i];
	if (nullArraycheck == 0) {
	memcpy(&count, (char*) record + dataIndex, sizeof(int));
	dataIndex += sizeof(int);
	if (recordDescriptor[i].name == attributeName) {
	bitArrayNew[0] = 0; //00000000
	memcpy((char*) data, &bitArrayNew, 1);
	memcpy((char*) data + 1, &count, sizeof(int));
	memcpy((char*) data + 1+sizeof(int), (char*) record + dataIndex, count);
	string s = string((char*) data + 5, count);
	}
	dataIndex += count;
	} else {
	if (recordDescriptor[i].name == attributeName) {
	bitArrayNew[0] = 128; //10000000
	memcpy((char*) data, &bitArrayNew, 1);
	}
	}
	} else { //TypeInt and TypeReal
        nullArraycheck = nullArray[i];
        if (nullArraycheck == 0) {
	if (recordDescriptor[i].name == attributeName) {
	bitArrayNew[0] = 0; //00000000
	memcpy((char*) data, &bitArrayNew, 1);
	memcpy((char*) data + 1, (char*) record + dataIndex,
	sizeof(int));

	}
	dataIndex += sizeof(int);
	} else {
	if (recordDescriptor[i].name == attributeName) {
	bitArrayNew[0] = 128; //10000000
	memcpy((char*) data, &bitArrayNew, 1); //store NULL
	}
	}
	}

	}
	free(bitArrayNew);
	free(record);
	return 0;
}
RC RecordBasedFileManager::compareAttributes(void *leftValue,void *rightValue, Attribute attr,CompOp condOp){
	int result=-1;
	int num1=0;
	int num2=0;
	float n1=0.0;
	float n2=0.0;
	int count1=0;
	int count2=0;
	switch(attr.type)
	{
	case TypeInt:

		memcpy(&num1,(char*)leftValue,sizeof(int));
		memcpy(&num2,(char*)rightValue,sizeof(int));
//		cout <<"Comparing: "<<num1<<", "<<num2<<endl;
		if(condOp==EQ_OP)
				{if(num1==num2)
				result=0;
				}
		if(condOp==LT_OP)
				{if(num1<num2)
				result=0;
				}
		if(condOp==LE_OP)
				{
				if(num1<=num2)
				result=0;
				}
		if(condOp==GT_OP)
				{
				if(num1>num2)
				result=0;
				}
		if(condOp==GE_OP)
				{
				if(num1>=num2)
				result=0;
				}
		if(condOp==NE_OP)
				{
				if(num1!=num2)
				result=0;
				}
		if(condOp==NO_OP)
				{
				result=0;
				}
		break;
	case TypeReal:
		memcpy(&n1,(char*)leftValue,sizeof(float));
		memcpy(&n2,(char*)rightValue,sizeof(float));
//		cout <<"Comparing: "<<n1<<", "<<n2<<endl;
				if(condOp==EQ_OP)
						{if(n1==n2)
						result=0;
						}
				if(condOp==LT_OP)
						{if(n1<n2)
						result=0;
						}
				if(condOp==LE_OP)
						{
						if(n1<=n2)
						result=0;
						}
				if(condOp==GT_OP)
						{
						if(n1>n2)
						result=0;
						}
				if(condOp==GE_OP)
						{
						if(n1>=n2)
						result=0;
						}
				if(condOp==NE_OP)
						{
						if(n1!=n2)
						result=0;
						}
				if(condOp==NO_OP)
						{
						result=0;
						}
		break;
	case TypeVarChar:

		memcpy(&count1, (char*)leftValue, sizeof(int));
		string str1 = string((char*)leftValue + sizeof(int), count1);
		memcpy(&count2,(char*)rightValue,sizeof(int));
		string str2 = string((char*)rightValue+sizeof(int),count2);
//		cout <<str1 <<",    "<<str2<<endl;
		if (condOp == EQ_OP) {
			if (str1.compare(str2)==0)
				result = 0;
		}
		if (condOp == LT_OP) {
			if (str1.compare(str2)<0)
				result = 0;
		}
		if (condOp == LE_OP) {
			if (str1.compare(str2)<0||str1.compare(str2)==0)
				result = 0;
		}
		if (condOp == GT_OP) {
			if (str1.compare(str2)>0)
				result = 0;
		}
		if (condOp == GE_OP) {
			if (str1.compare(str2)>0||str1.compare(str2)==0)
				result = 0;
		}
		if (condOp == NE_OP) {
			if (str1.compare(str2)!=0)
				result = 0;
		}
		if (condOp == NO_OP) {
				result = 0;
		}
		break;
	}
//	cout<<"Result in compare attributes: "<<result <<endl;
return result;
}
// Scan returns an iterator to allow the caller to go through the results one by one.
RC RecordBasedFileManager::scan(FileHandle &fileHandle,
	const vector<Attribute> &recordDescriptor,
	const string &conditionAttribute, const CompOp compOp, // comparision type such as "<" and "="
	const void *value,                    // used in the comparison
	const vector<string> &attributeNames, // a list of projected attributes
	RBFM_ScanIterator &rbfm_ScanIterator) {
	rbfm_ScanIterator.fileHandle = fileHandle;
	rbfm_ScanIterator.recordDescriptor = recordDescriptor;
	rbfm_ScanIterator.conditionAttribute = conditionAttribute;
	rbfm_ScanIterator.compOp = compOp;
	rbfm_ScanIterator.value = value;
	rbfm_ScanIterator.attributeNames = attributeNames;
	return 0;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
		int recordSatisfiesCondition = 0;
		while (true) { //while loop
		if (rid_next.pageNum == INVALID || rid_next.slotNum == INVALID) {
			return -1;
		}
		RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
		//Store page information for updating rid_next values accordingly.
		void *pagebuffer = malloc(PAGE_SIZE);
		int readPage = fileHandle.readPage(rid_next.pageNum -1, pagebuffer);
		int numOfPages = fileHandle.getNumberOfPages();
		if(numOfPages==0)
			return -1;
		short slotsInPage = rbfm->findSlotsCount(pagebuffer);

		void *record = malloc(PAGE_SIZE);
		int rr=rbfm->readRecord(fileHandle, recordDescriptor, rid_next, record);
		if(rr==-1)
		{
			recordSatisfiesCondition=0;
		}
		if(rr!=-1){
		int nullBitsIndicatorActualSize = ceil(recordDescriptor.size() / 8.0);

		bool* nullArray = (bool*) malloc(recordDescriptor.size());
		memset(nullArray, 0, recordDescriptor.size());
		int r = rbfm->getNullBits(recordDescriptor, record,nullArray);
		int dataIndex = nullBitsIndicatorActualSize;
		recordSatisfiesCondition = 0;

		if (conditionAttribute == "" && (char*) this->value == NULL) {
			recordSatisfiesCondition = 1;}

			else{

		for (int i = 0; i < recordDescriptor.size(); i++) {
			int count = 0;

			if (recordDescriptor[i].type == TypeVarChar) {

				if (recordDescriptor[i].name == conditionAttribute) {
					memcpy(&count, (char*) record + dataIndex, sizeof(int));
					dataIndex += sizeof(int);
					string str = string((char*) record + dataIndex, count);

					char *strvalCursor = (char*) this->value;
					string strval = string(strvalCursor);

					//Now check which operator is present. Apply switch case.
					switch (compOp) {
					case EQ_OP:
						if (str.compare(strval) == 0) {
							recordSatisfiesCondition = 1;
						}
						break;
					case LT_OP:
						if (str.compare(strval) < 0) {
							recordSatisfiesCondition = 1;
						}
						break;
					case LE_OP:
						if (str.compare(strval) < 0
								|| str.compare(strval) == 0) {
							recordSatisfiesCondition = 1;
						}
						break;
					case GT_OP:
						if (str.compare(strval) > 0) {
							recordSatisfiesCondition = 1;
						}
						break;
					case GE_OP:
						if (str.compare(strval) > 0
								|| str.compare(strval) == 0) {
							recordSatisfiesCondition = 1;
						}
						break;
					case NE_OP:
						if (!(str.compare(strval) == 0)) {
							recordSatisfiesCondition = 1;
						}
						break;
					case NO_OP:
						recordSatisfiesCondition = 1;
						break;
					}

				}
                                int nullArraycheck = nullArray[i];

                                if (nullArraycheck == 0)

					dataIndex = dataIndex + count;
			}

			else if (recordDescriptor[i].type == TypeInt) { //for TypeInt
				if (recordDescriptor[i].name == conditionAttribute) {

					int recordVal=0;
					memcpy(&recordVal, (char*) record + dataIndex, sizeof(int));

					int intvalue=0;
					memcpy(&intvalue, (char*) value, sizeof(int));

					switch (compOp) {
					case EQ_OP:
						if (recordVal == intvalue)
							recordSatisfiesCondition = 1;
						break;
					case LT_OP:
						if (recordVal < intvalue)
							recordSatisfiesCondition = 1;
						break;
					case LE_OP:
						if (recordVal <= intvalue)
							recordSatisfiesCondition = 1;
						break;
					case GT_OP:
						if (recordVal > intvalue)
							recordSatisfiesCondition = 1;
						break;
					case GE_OP:
						if (recordVal >= intvalue)
							recordSatisfiesCondition = 1;
						break;
					case NE_OP:
						if (recordVal != intvalue)
							recordSatisfiesCondition = 1;
						break;
					case NO_OP:
						recordSatisfiesCondition = 1;
						break;

					}
				}
				int nullArraycheck = nullArray[i];
				if (nullArraycheck == 0)
					dataIndex += sizeof(int);

			} else if (recordDescriptor[i].type == TypeReal) { //for TypeInt
				if (recordDescriptor[i].name == conditionAttribute) {
					float recordVal=0.0;
					memcpy(&recordVal, (char*) record + dataIndex, sizeof(int));
					float floatvalue=0.0;
					memcpy(&floatvalue, (char*) value, sizeof(int));
					switch (compOp) {
					case EQ_OP:
						if (recordVal == floatvalue)
							recordSatisfiesCondition = 1;
						break;
					case LT_OP:
						if (recordVal < floatvalue)
							recordSatisfiesCondition = 1;
						break;
					case LE_OP:
						if (recordVal <= floatvalue)
							recordSatisfiesCondition = 1;
						break;
					case GT_OP:
						if (recordVal > floatvalue)
							recordSatisfiesCondition = 1;
						break;
					case GE_OP:
						if (recordVal >= floatvalue)
							recordSatisfiesCondition = 1;
						break;
					case NE_OP:
						if (recordVal != floatvalue)
							recordSatisfiesCondition = 1;
						break;
					case NO_OP:
						recordSatisfiesCondition = 1;
						break;

					}
				}
                                int nullArraycheck = nullArray[i];
                                if (nullArraycheck == 0)
									dataIndex += sizeof(int);
			}
		}

			}
		free(nullArray);
			}
		free(record);



		free(pagebuffer);
		//------------------------------------------------------------------------
		if (recordSatisfiesCondition == 1 ) {
			char *tempValue = (char*) malloc(attributeNames.size() * 200);
			int offset_tempvalue = 0;
			int sizeOfAttributesVector = attributeNames.size();
			int nullBitsIndicatorActualSize = ceil(attributeNames.size() / 8.0);
			int j = 7;
			bool* finalBitsArray = (bool*) malloc(sizeOfAttributesVector);
//			memset(finalBitsArray,0,sizeOfAttributesVector);
			int p = nullBitsIndicatorActualSize * 8 - 1;
			int nullsBitsIndicatorForFinalResult = 0;
			for (int k = 0; k < sizeOfAttributesVector; k++) {
				void *attributeData = malloc(200);

				rbfm->readAttribute(fileHandle, recordDescriptor, rid_next,
						attributeNames[k], attributeData);
				unsigned char* byteArray = (unsigned char*) malloc(
						nullBitsIndicatorActualSize);
				memset(byteArray,0,nullBitsIndicatorActualSize);
				memcpy(byteArray, (char*) attributeData, 1);
				bool nullBit = byteArray[0] & 1 << j;
				finalBitsArray[k] = nullBit;
				for (int l = 0; l < recordDescriptor.size(); l++) {
					if (recordDescriptor[l].name == attributeNames[k]) {
						//Types match
						if (recordDescriptor[l].type == TypeVarChar) {
							int offset = 1;
							int count;
							memcpy(&count, (char*) attributeData + offset,
									sizeof(int));
							memcpy((char*) tempValue + offset_tempvalue, &count,
									sizeof(int));
							offset_tempvalue += sizeof(int);
							offset += sizeof(int);
							memcpy((char*) tempValue + offset_tempvalue,
									(char*) attributeData + offset, count);
							offset_tempvalue += count;
						} else {
							int offset = 1;
							memcpy((char*) tempValue + offset_tempvalue,
									(char*) attributeData + offset,
									sizeof(int));
							offset_tempvalue += sizeof(int);
						}

					}
				}
				free(byteArray);
				free(attributeData);
			}
			char* finalByteArray = (char *) malloc(
					nullBitsIndicatorActualSize);
			memset(finalByteArray,0,nullBitsIndicatorActualSize);
			//Bits array to byte data
			int i = 0;
		//	finalByteArray[i] = 0;
			for (int j = 0; j < nullBitsIndicatorActualSize * 8; j++) {
				if (j >= sizeOfAttributesVector)
					break;
				if (finalBitsArray[j])
					finalByteArray[i] |= 1 << j;
				if(j%8==0)
					i=i+1;
			}

			memcpy((char*) data, (char*)finalByteArray, nullBitsIndicatorActualSize);
			memcpy((char*) data + nullBitsIndicatorActualSize,
					(char*) tempValue, offset_tempvalue);
			free(finalBitsArray);
			free(finalByteArray);
			free(tempValue);


			rid.slotNum = rid_next.slotNum;
			rid.pageNum = rid_next.pageNum;
			if (rid_next.slotNum < slotsInPage) {

				rid_next.slotNum = rid_next.slotNum + 1;
			} else if (rid_next.slotNum == slotsInPage) {
				if (rid_next.pageNum == numOfPages) {
					rid_next.pageNum = INVALID;
					rid_next.slotNum = INVALID;
				}
			} else {
				rid_next.pageNum = rid_next.pageNum + 1;
				rid_next.slotNum = 1;
			}

			break;

		}

		//------------------------------------------------------------------------
		else {
			if (rid_next.slotNum < slotsInPage) {
				rid.slotNum = rid_next.slotNum;
				rid.pageNum = rid_next.pageNum;
				rid_next.slotNum = rid_next.slotNum + 1;
				continue;
			} else if (rid_next.slotNum == slotsInPage) {
				if (rid_next.pageNum == numOfPages) {

					rid.slotNum = rid_next.slotNum;
					rid.pageNum = rid_next.pageNum;
					rid_next.pageNum = INVALID;
					rid_next.slotNum = INVALID;
					//break;
					break;
				} else {

					rid.slotNum = rid_next.slotNum;
					rid.pageNum = rid_next.pageNum;
					rid_next.pageNum = rid_next.pageNum + 1;
					rid_next.slotNum = 1;
					continue;
				}
			}

		}
	}

	if (recordSatisfiesCondition == 1) {
		return 0;
	} else
		return -1;
}
