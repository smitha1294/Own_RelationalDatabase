
#include "rm.h"
#include <iostream>
#include <stdio.h>
#include <math.h>
#include <string>
#include <iomanip>
#include <fstream>
#include<cstring>
using namespace std;

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
	tableId=1;
	index_attr_tableID = 0;
	Tables = "Tables";
	Columns = "Columns";

	Attribute attr;
	attr.name = "table-id";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	Tables_data.push_back(attr);

	Attribute attr1;
	attr1.name = "table-name";
	attr1.type = TypeVarChar;
	attr1.length = (AttrLength)30;
	Tables_data.push_back(attr1);

	Attribute attr2;
	attr2.name = "file-name";
	attr2.type = TypeVarChar;
	attr2.length = (AttrLength)30;
	Tables_data.push_back(attr2);

	Attribute attr3;

	attr3.name = "table-id";
	attr3.type = TypeInt;
	attr3.length = (AttrLength)4;
	Columns_data.push_back(attr3);
	Attribute attr4;

	attr4.name = "column-name";
	attr4.type = TypeVarChar;
	attr4.length = (AttrLength)30;
	Columns_data.push_back(attr4);

	Attribute attr5;

	attr5.name = "column-type";
	attr5.type = TypeInt;
	attr5.length = (AttrLength)4;
	Columns_data.push_back(attr5);

	Attribute attr6;

	attr6.name = "column-length";
	attr6.type = TypeInt;
	attr6.length = (AttrLength)4;
	Columns_data.push_back(attr6);

	Attribute attr7;

	attr7.name = "column-position";
	attr7.type = TypeInt;
	attr7.length = (AttrLength)4;
	Columns_data.push_back(attr7);

	Attribute attr8;

	attr8.name = "Has-Index";
	attr8.type = TypeInt;
	attr8.length = (AttrLength)4;
	Columns_data.push_back(attr8);
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
    RC rc;
    rc = updateSystemTables(Tables, Tables_data, tableId);
	int newtableID = getTableIDForNewInsert();
    rc = updateSystemTables(Columns, Columns_data, newtableID);
    return rc;
}

RC RelationManager::deleteCatalog()
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc;
	rc = rbfm->destroyFile(Tables);
	rc = rbfm->destroyFile(Columns);
	return rc;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;
    rc = rbfm->createFile(tableName);
    	int newtableID = getTableIDForNewInsert();
    rc = updateSystemTables(tableName, attrs, newtableID);
    return rc;
}

RC RelationManager::deleteTable(const string &tableName)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc;
    vector<string> tablesProjections;
    RBFM_ScanIterator *itr = new RBFM_ScanIterator();
    RID rid;
    FileHandle fileHandle1;
    void* data=(void*)malloc(PAGE_SIZE);
    int returnedTableID;

    if (!std::ifstream(tableName)){return -1;}
    if(tableName==Tables || tableName==Columns){return -1;}
	rc = rbfm->destroyFile(tableName);

    rc = rbfm->openFile(Tables, fileHandle1);

    tablesProjections.push_back("table-id");
    tablesProjections.push_back("table-name");
    tablesProjections.push_back("file-name");

    rbfm->scan(fileHandle1, Tables_data, "table-name", EQ_OP, (void*)tableName.c_str(), tablesProjections, *itr);
    if(itr->getNextRecord(rid, data) != -1) {
    		if(rid.pageNum==INVALID || rid.slotNum==INVALID){return -1;}
			int offset = 0;
			bool nullBit = false;
			int nullFieldsIndicatorActualSize = ceil(2.0/ 8.0);
			unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
			memset(nullsIndicator, 0, 1);
			offset += nullFieldsIndicatorActualSize;

			nullBit = nullsIndicator[0] & (1 << 7);
			if (nullBit == 0) {
			memcpy(&returnedTableID,(char*) data + offset,sizeof(int));
			offset +=sizeof(int);
			}
			rc = rbfm->deleteRecord(fileHandle1,Tables_data, rid);
    }

    free(data);
    vector<string> columnsProjections;
    RBFM_ScanIterator *iter = new RBFM_ScanIterator();
    data=(void*)malloc(PAGE_SIZE);
    vector<RID> *matching_rids=new vector<RID>();
    rc = rbfm->closeFile(fileHandle1);

    FileHandle fileHandle2;
    rc = rbfm->openFile(Columns, fileHandle2);

    columnsProjections.push_back("table-id");
    columnsProjections.push_back("column-name");
    columnsProjections.push_back("column-type");
    columnsProjections.push_back("column-length");
    columnsProjections.push_back("column-position");
    columnsProjections.push_back("Has-Index");
    rbfm->scan(fileHandle2, Columns_data, "table-id", EQ_OP, &returnedTableID, columnsProjections, *iter);
    while(iter->getNextRecord(rid, data) != RBFM_EOF) {
    		if(rid.pageNum==INVALID || rid.slotNum==INVALID){return -1;}
    		matching_rids->push_back(rid);
    }
    for(int i=0;i<(int)matching_rids->size();i++)
	{
		rc = rbfm->deleteRecord(fileHandle2,Columns_data,matching_rids->at(i));
	}
    free(data);
    rbfm->closeFile(fileHandle2);
    return rc;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;
    vector<string> tablesProjections;
    RID rid;
    FileHandle fileHandle1;
    RBFM_ScanIterator *iter = new RBFM_ScanIterator();
    void* data=(void*)malloc(PAGE_SIZE);
    int returnedTableID =0;
    index_attr_tableID =0;
    string returnedString;
    rc = rbfm->openFile(Tables, fileHandle1);

    tablesProjections.push_back("table-id");
    tablesProjections.push_back("table-name");

    rbfm->scan(fileHandle1, Tables_data, "table-name", EQ_OP, (void*)tableName.c_str(), tablesProjections, *iter);
    if(iter->getNextRecord(rid, data) != -1) {

    	if(rid.pageNum==INVALID || rid.slotNum==INVALID){return -1;}
    		int count = 0;
    		int offset = 0;
        bool nullBit = false;
        int nullFieldsIndicatorActualSize = ceil(2.0/ 8.0);
        unsigned char* nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
	memset(nullsIndicator, 0, 1);
        offset += nullFieldsIndicatorActualSize;
    		nullBit = nullsIndicator[0] & (1 << 7);
    	    if (nullBit == 0) {
            memcpy(&returnedTableID,(char*) data + offset,sizeof(int));
            offset +=sizeof(int);
    	    }
    	    nullBit = nullsIndicator[0] & (1 << 6);
    	    if (nullBit == 0) {
    		memcpy(&count, (char*) data + offset, sizeof(int));
    		offset+=sizeof(int);
    		returnedString= string((char*) data + offset, count);
    		if (returnedString != tableName){ free(nullsIndicator);return -1;}
    	    }
    	    free(nullsIndicator);
    }
    free(data);
    rc = rbfm->closeFile(fileHandle1);
    iter->close();
    vector<string> columnsProjections;
    RBFM_ScanIterator *itr = new RBFM_ScanIterator();
    void* data1=(void*)malloc(PAGE_SIZE);
    int returnedInt =0;

    FileHandle fileHandle2;

    rc = rbfm->openFile(Columns, fileHandle2);
    columnsProjections.push_back("table-id");
    columnsProjections.push_back("column-name");
    columnsProjections.push_back("column-type");
    columnsProjections.push_back("column-length");
    rbfm->scan(fileHandle2, Columns_data, "table-id", EQ_OP, &returnedTableID, columnsProjections, *itr);
    while(itr->getNextRecord(rid, data1) != RBFM_EOF) {
     	if(rid.pageNum==(INVALID) || rid.slotNum==(INVALID)){return -1;}
		int count = 0;
		int offset = 0;
		bool nullBit = false;
		int nullFieldsIndicatorActualSize = ceil(4.0/ 8.0);
    		unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    		memset(nullsIndicator, 0, 1);
		offset += nullFieldsIndicatorActualSize;
    		Attribute returnedRecord;

		nullBit = nullsIndicator[0] & (1 << 7);
	    if (nullBit == 0) {
        memcpy(&returnedInt,(char*) data1 + offset,sizeof(int));
        offset +=sizeof(int);
	    }
	    if (returnedInt != returnedTableID){ free(nullsIndicator);return -1;}

	    nullBit = nullsIndicator[0] & (1 << 6);
	    if (nullBit == 0) {
		memcpy(&count, (char*) data1 + offset, sizeof(int));
		offset+=sizeof(int);
		returnedString= string((char*) data1 + offset, count);
		offset +=count;
	    }
	    returnedRecord.name=returnedString;

		nullBit = nullsIndicator[0] & (1 << 5);
	    if (nullBit == 0) {
        memcpy(&returnedInt,(char*) data1 + offset,sizeof(int));
        offset +=sizeof(int);
	    }
	    if(returnedInt == 0) {returnedRecord.type=(AttrType)TypeInt;}
	    if(returnedInt == 1) {returnedRecord.type=(AttrType)TypeReal;}
	    if(returnedInt == 2) {returnedRecord.type=(AttrType)TypeVarChar;}

		nullBit = nullsIndicator[0] & (1 << 4);
	    if (nullBit == 0) {
        memcpy(&returnedInt,(char*) data1 + offset,sizeof(int));
        offset +=sizeof(int);
	    }
	    returnedRecord.length=returnedInt;
	    attrs.push_back(returnedRecord);
	    index_attr_tableID = returnedTableID;
	    free(nullsIndicator);
   }
    free(data1);
    itr->close();
    rc = rbfm->closeFile(fileHandle2);
    return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;
    vector<Attribute> attrs;
    FileHandle fileHandle;
    if (!std::ifstream(tableName)){return -1;}
    rc = rbfm->openFile(tableName, fileHandle);
    rc = getAttributes(tableName, attrs);

    // Insert the Record
    rc = rbfm->insertRecord(fileHandle, attrs, data, rid); if (rc == -1) { return rc;}

    	// Insert the Index Entry
    	rc = insertOrDeleteIndexEntry(fileHandle, tableName, index_attr_tableID, "insert", rid, attrs);
	rc = rbfm->closeFile(fileHandle);
	attrs.clear();
    return rc;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;
    vector<Attribute> attrs;
    FileHandle fileHandle;
    if (!std::ifstream(tableName)){return -1;}
    rc = rbfm->openFile(tableName, fileHandle);
    rc = this->getAttributes(tableName, attrs);

    // Delete the Index Entry
    rc = insertOrDeleteIndexEntry(fileHandle, tableName, index_attr_tableID, "delete", rid, attrs);

    	// Delete the Record
    rc = rbfm->deleteRecord(fileHandle, attrs, rid);
    rbfm->closeFile(fileHandle);
    attrs.clear();
    return rc;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;
    vector<Attribute> attrs;
    FileHandle fileHandle;
    if (!std::ifstream(tableName)){return -1;}

    rc = rbfm->openFile(tableName, fileHandle);
    rc = this->getAttributes(tableName, attrs);

    // Delete the Index Entry
    rc = insertOrDeleteIndexEntry(fileHandle, tableName, index_attr_tableID, "delete", rid, attrs);

    //Update the record
    rc = rbfm->updateRecord(fileHandle, attrs, data, rid);

	// Insert the Index Entry
	rc = insertOrDeleteIndexEntry(fileHandle, tableName, index_attr_tableID, "insert", rid, attrs);

    	rc = rbfm->closeFile(fileHandle);
    	attrs.clear();
    return rc;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;
    vector<Attribute> attrs;
    FileHandle fileHandle;
    if (!std::ifstream(tableName)){return -1;}

	rc = rbfm->openFile(tableName, fileHandle);
    rc = this->getAttributes(tableName, attrs);
	rc = rbfm->readRecord(fileHandle, attrs, rid, data);
	rbfm->closeFile(fileHandle);
	return rc;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc;
	rc = rbfm->printRecord(attrs, data);
	return rc;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;
    vector<Attribute> attrs;
    FileHandle fileHandle;
    if (!std::ifstream(tableName)){return -1;}
    rc = rbfm->openFile(tableName, fileHandle);
    rc = this->getAttributes(tableName, attrs);
    rc = rbfm->readAttribute(fileHandle, attrs, rid, attributeName, data);
    rbfm->closeFile(fileHandle);
    return rc;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
	RC rc;
	vector<Attribute> attrs;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	rc = rbfm->openFile(tableName, fileHandle);
	rc = getAttributes(tableName,attrs);
	if (rc != 0){return -1;}
	rc = rbfm->scan(fileHandle,attrs,conditionAttribute,compOp,value,attributeNames,rm_ScanIterator.rbfm_ScanIterator);
	rbfm->closeFile(fileHandle);
	return rc;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
	RC rc;
	vector<Attribute> att;
	getAttributes(tableName, att);
	Attribute IndexAttr;
	for(int i=0;i<(int)(att.size());i++)
	{
		if(att.at(i).name==attributeName)
		{
			IndexAttr.name = att.at(i).name;
			IndexAttr.type = att.at(i).type;
			IndexAttr.length = att.at(i).length;
			break;
		}
	}
		rc = updateIndexColValue(tableName,attributeName, index_attr_tableID, IndexAttr, 1);
	    IndexManager *im=IndexManager::instance();
	    string table=tableName+"_index_"+attributeName;
	    if (!std::ifstream(table)){
	    	rc = im->createFile(table);
	    }
	    	RM_ScanIterator rmsi;
	    	vector<string> projections;
	    	projections.push_back(attributeName);
    		RID rid;
    		int count=0;
    		void *returnedData=(void*)malloc(PAGE_SIZE);
    		void *keyValue=(void*)malloc(PAGE_SIZE);
    		IXFileHandle ixFileHandle;
    		IndexManager *ix=IndexManager::instance();
    		ix->openFile(table, ixFileHandle);
	    	if(this->scan(tableName, "", NO_OP, NULL, projections, rmsi)==0)
	    	{
	    		while(rmsi.getNextTuple(rid, returnedData) != RM_EOF)
	    		{
	    			memcpy((char*)keyValue, (char*)returnedData+1, PAGE_SIZE);
	    			rc = ix->insertEntry(ixFileHandle,IndexAttr,keyValue,rid);
//	    			ix->printBtree(ixFileHandle,IndexAttr);
	    		}
	    		ix->closeFile(ixFileHandle);
	    		free(keyValue);
	    		free(returnedData);
	    	}
	    	projections.clear();
	    att.clear();
		return rc;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
	RC rc;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	vector<Attribute> att;
	getAttributes(tableName, att);
	Attribute IndexAttr;
	for(int i=0;i<(int)(att.size());i++)
	{
		if(att.at(i).name==attributeName)
		{
			IndexAttr.name = att.at(i).name;
			IndexAttr.type = att.at(i).type;
			IndexAttr.length = att.at(i).length;
			break;
		}
	}
		rc = updateIndexColValue(tableName,attributeName, index_attr_tableID, IndexAttr, 0);
		string table=tableName+"_index_"+attributeName;
		if (std::ifstream(table)){
		rc = rbfm->destroyFile(table);
		}
		att.clear();
		return rc;
}

RC RelationManager::indexScan(const string &tableName,
                      const string &attributeName,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      RM_IndexScanIterator &rm_IndexScanIterator)
{
	RC rc;
	vector<Attribute> att;
//	cout<<"tABLE TO BE SCANNED: "<<tableName.c_str()<<endl;
	rc = getAttributes(tableName,att);
	if (rc != 0)
	{
//		cout << rc<<"  Error in insert record" << endl;
		return -1;
	}
	Attribute IndexAttr;
	for(int i=0;i<(int)(att.size());i++)
	{
		if(att.at(i).name==attributeName)
		{
			IndexAttr.name = att.at(i).name;
			IndexAttr.type = att.at(i).type;
			IndexAttr.length = att.at(i).length;
//			cout<<"Attribute details in index scan:"<<att.at(i).name <<","<<att.at(i).type <<","<<att.at(i).length <<endl;
			break;
		}
	}
	IXFileHandle ixFileHandle;
	IndexManager *ix=IndexManager::instance();
	rc = ix->openFile(tableName+"_index_"+attributeName, ixFileHandle);
	string fn=tableName+"_index_"+attributeName;
//	cout<<"File opened:"<<fn <<":"<<rc<<endl;
	rc = ix->scan(ixFileHandle,IndexAttr,lowKey,highKey,lowKeyInclusive,highKeyInclusive,rm_IndexScanIterator.ix_scanner);
	att.clear();
	return rc;
}

//New Method
RC RelationManager::updateSystemTables(const string &tableName, const vector<Attribute> &attrs, const int passedTableID)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;
    RID rid;
    FileHandle fileHandle1;
    void *tables_buffer = malloc(PAGE_SIZE);

    if (!std::ifstream(Tables)) {rc = rbfm->createFile(Tables);}
    rc = rbfm->openFile(Tables, fileHandle1);

    // Null-indicators for Tables Data
	int offset = 0;
    bool nullBit = false;
    int nullFieldsIndicatorActualSize = ceil(Tables_data.size() / 8.0);
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, 1);
    memcpy((char *)tables_buffer + offset, nullsIndicator, nullFieldsIndicatorActualSize);
    offset += nullFieldsIndicatorActualSize;
    nullBit = nullsIndicator[0] & (1 << 7);
    if (!nullBit)
    {
	memcpy((char*)tables_buffer+ offset,&passedTableID,sizeof(int));
	offset +=sizeof(int);
    }
    nullBit = nullsIndicator[0] & (1 << 6);
    if (!nullBit)
    {
	int tableNameLength = (int)tableName.length();
	memcpy((char*)tables_buffer + offset,&tableNameLength,sizeof(int));
	offset+=sizeof(int);
	memcpy((char*)tables_buffer+offset,tableName.c_str(),tableNameLength);
	offset += tableNameLength;
    }
    nullBit = nullsIndicator[0] & (1 << 5);
    if (!nullBit)
    {
	int fileNameLength = (int)tableName.length();
	memcpy((char*)tables_buffer+offset,&fileNameLength,sizeof(int));
	offset +=sizeof(int);
	memcpy((char*)tables_buffer+offset,tableName.c_str(),fileNameLength);
	offset += fileNameLength;
    }
    rc = rbfm->insertRecord(fileHandle1, Tables_data, tables_buffer, rid);
	rbfm->readRecord(fileHandle1, Tables_data, rid, tables_buffer);
    rc = rbfm->closeFile(fileHandle1);
    free(nullsIndicator);
    free(tables_buffer);
    FileHandle fileHandle2;

    // Null-indicators for Columns Data
    if (!std::ifstream(Columns)) {rc = rbfm->createFile(Columns);}
    rc = rbfm->openFile(Columns, fileHandle2);
	int numOfCol = attrs.size();
	for (int i=0;i<numOfCol;i++)
	{
	    void *col_buffer = malloc(PAGE_SIZE);
	    prepareTupleForColumnsTable(passedTableID, i,attrs[i], 0, col_buffer);
		rc = rbfm->insertRecord(fileHandle2, Columns_data, col_buffer, rid);
		free(col_buffer);
	}
    rc = rbfm->closeFile(fileHandle2);
	return 0;
}

RC RelationManager::updateIndexColValue(const string &tableName, const string &attributeName, const int tableID, const Attribute &attrs, const int index){

	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;
	FileHandle fileHandle2;
		  string returnedString;
		  RBFM_ScanIterator *itr = new RBFM_ScanIterator();
		  vector<string> columnsProjections;
		  RID rid;
		  void* data=(void*)malloc(PAGE_SIZE);
		    rc = rbfm->openFile(Columns, fileHandle2);
		    columnsProjections.push_back("column-name");
		    columnsProjections.push_back("column-position");
		    rbfm->scan(fileHandle2, Columns_data, "table-id", EQ_OP,&tableID, columnsProjections, *itr);
		    while(itr->getNextRecord(rid, data) != RBFM_EOF) {
		     	if(rid.pageNum==(INVALID) || rid.slotNum==(INVALID)){return -1;}
				int count = 0;
				int offset = 0;
				int pos=0;
				bool nullBit = false;
				int nullFieldsIndicatorActualSize = ceil(columnsProjections.size()/ 8.0);
		    		unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
		    		memset(nullsIndicator, 0, 1);
				offset += nullFieldsIndicatorActualSize;
				nullBit = nullsIndicator[0] & (1 << 7);
			    if (nullBit == 0) {
				memcpy(&count, (char*) data + offset, sizeof(int));
				offset+=sizeof(int);
				returnedString= string((char*) data + offset, count);
				offset +=count;
			    }
			    if(returnedString != attributeName){
			    		continue;
			    }
			    nullBit = nullsIndicator[0] & (1 << 6);
			    if (!nullBit){
				memcpy(&pos,(char*) data + offset, sizeof(int));
				offset +=sizeof(int);
			    }
			    void *col_buffer = malloc(PAGE_SIZE);
			    prepareTupleForColumnsTable(tableID, pos,attrs, index, col_buffer);
				rc = rbfm->updateRecord(fileHandle2, Columns_data, col_buffer, rid);
				free(col_buffer);
				if(returnedString == attributeName){
						    		break;
				}
		    	}
		    itr->close();
			rc = rbfm->closeFile(fileHandle2);
			columnsProjections.clear();
			free(data);
			return 0;
}

void RelationManager::prepareTupleForColumnsTable(const int passedTableID, const int i, const Attribute &attrs, const int index, void *col_buffer){

    bool nullBit = false;
    int offset = 0;
	int nullFieldsIndicatorActualSize = ceil(Columns_data.size() / 8.0);
			unsigned char *nullsIndicatorForCol = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
		memset(nullsIndicatorForCol, 0, 1);
		memcpy((char *)col_buffer + offset, nullsIndicatorForCol, nullFieldsIndicatorActualSize);
		    offset += nullFieldsIndicatorActualSize;
			nullBit = nullsIndicatorForCol[0] & (1 << 7);
		    if (!nullBit) {
	        memcpy((char*)col_buffer+offset,&passedTableID,sizeof(int));
	        offset +=sizeof(int);
		    }
		    nullBit = nullsIndicatorForCol[0] & (1 << 6);
		    if (!nullBit) {
			int temp = attrs.name.length();
			memcpy((char *)col_buffer+offset,&temp,sizeof(int));
			offset +=sizeof(int);
			memcpy((char *)col_buffer+offset,attrs.name.c_str(),temp);
			offset +=temp;
		    }
		    nullBit = nullsIndicatorForCol[0] & (1 << 5);
		    if (!nullBit){
			memcpy((char *)col_buffer+offset,&attrs.type,sizeof(int));
			offset +=sizeof(int);
		    }
		    nullBit = nullsIndicatorForCol[0] & (1 << 4);
		    if (!nullBit){
			memcpy((char *)col_buffer+offset,&attrs.length,sizeof(int));
			offset +=sizeof(int);
		    }
		    nullBit = nullsIndicatorForCol[0] & (1 << 3);
		    if (!nullBit){
		    	int position = i+1;
			memcpy((char *)col_buffer+offset,&position,sizeof(int));
			offset +=sizeof(int);
		    }
		    nullBit = nullsIndicatorForCol[0] & (1 << 2);
		    if (!nullBit){
			memcpy((char *)col_buffer+offset,&index,sizeof(int));
			offset +=sizeof(int);
		    }
}

RC RelationManager::insertOrDeleteIndexEntry(FileHandle &fileHandle, const string &tableName, const int tableID, const string &insertOrdelete, const RID &rid, const vector<Attribute> &attrs){

	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;
	FileHandle fileHandle2;
	  string indexAttrName;
	  RBFM_ScanIterator *itr = new RBFM_ScanIterator();
	  vector<string> columnsProjections;
	  IndexManager *ix=IndexManager::instance();
	  RID rid1;
	  Attribute atr;
	  void* data1=(void*)malloc(PAGE_SIZE);
	  void *key=(void*)malloc(PAGE_SIZE);
	  void *keyValue=(void*)malloc(PAGE_SIZE);
	    rc = rbfm->openFile(Columns, fileHandle2);
	    columnsProjections.push_back("column-name");
	    columnsProjections.push_back("Has-Index");
	    rbfm->scan(fileHandle2, Columns_data, "table-id", EQ_OP,&tableID, columnsProjections, *itr);
	    while(itr->getNextRecord(rid1, data1) != RBFM_EOF) {
	     	if(rid1.pageNum==(INVALID) || rid1.slotNum==(INVALID)){return -1;}
			int count = 0;
			int offset = 0;
			int index=0;
			bool nullBit = false;
			int nullFieldsIndicatorActualSize = ceil(columnsProjections.size()/ 8.0);
	    		unsigned char *nullsIndicator = (unsigned char*) malloc(nullFieldsIndicatorActualSize);
	    		memset(nullsIndicator, 0, 1);
			offset += nullFieldsIndicatorActualSize;
			nullBit = nullsIndicator[0] & (1 << 7);
		    if (nullBit == 0) {
			memcpy(&count, (char*) data1 + offset, sizeof(int));
			offset+=sizeof(int);
			indexAttrName= string((char*) data1 + offset, count);
			offset +=count;
		    }
		    nullBit = nullsIndicator[0] & (1 << 6);
		    if (!nullBit){
			memcpy(&index,(char*) data1 + offset, sizeof(int));
			offset +=sizeof(int);
		    }
		    if(index == 1){
		    			    string table=tableName+"_index_"+indexAttrName;
		    		    		IXFileHandle ixFileHandle;
		    			    if (!std::ifstream(table)){return -1;}
		    			    		rc = ix->openFile(table,ixFileHandle);
		    			    		rc = rbfm->readAttribute(fileHandle, attrs, rid,indexAttrName , key);
		    			    		for(int i=0;i<(int)attrs.size();i++)
		    			    		    {
		    			    				if(attrs[i].name == indexAttrName){
		    			    					atr.name = attrs[i].name;
		    			    					atr.length = attrs[i].length;
		    			    					atr.type = attrs[i].type;
		    			    					break;
		    			    				}
		    			    		    }
		    			    		if (insertOrdelete == "insert"){
		    			    			memcpy((char*)keyValue, (char*)key+1, PAGE_SIZE);
		    			    			rc = ix->insertEntry(ixFileHandle,atr,keyValue,rid); if (rc == -1) { return rc;}}
		    			    		else if (insertOrdelete == "delete") {
		    			    			rc = ix->deleteEntry(ixFileHandle,atr,keyValue,rid); if (rc == -1) { return rc;}}
		    			    		rc = ix->closeFile(ixFileHandle);
		    			    	}
		    	}
	    itr->close();
		rc = rbfm->closeFile(fileHandle2);
		columnsProjections.clear();
		free(data1);
		free(key);
		free(keyValue);
		return 0;
}

int RelationManager::getTableIDForNewInsert(){
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;
    vector<string> tablesProjections;
    RBFM_ScanIterator *itr = new RBFM_ScanIterator();
    RID rid;
    FileHandle fileHandle1;
    int lastRecordtableID = 0;
    void* data=(void*)malloc(PAGE_SIZE);

    if (!std::ifstream(Tables)){return -1;}
    rc = rbfm->openFile(Tables, fileHandle1);

    tablesProjections.push_back("table-id");

    rbfm->scan(fileHandle1, Tables_data,"", NO_OP, NULL, tablesProjections, *itr);
    while(itr->getNextRecord(rid, data) != -1) {
    		lastRecordtableID = rid.slotNum;
    }
    lastRecordtableID=lastRecordtableID+1;
    rc = rbfm->closeFile(fileHandle1);
    itr->close();
    tablesProjections.clear();
    return lastRecordtableID;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {

	  if(rbfm_ScanIterator.getNextRecord(rid,data)!=RM_EOF)
	  {
		  return 0;
	  }
	  else
		  return RM_EOF;
}

RC RM_ScanIterator::close() {
	  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	  RC rc;

	  rc = rbfm->closeFile(rbfm_ScanIterator.fileHandle);
	  return 0;
  }
