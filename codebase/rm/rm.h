
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <map>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iterator to go through tuples
class RM_ScanIterator {
public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};

  RBFM_ScanIterator rbfm_ScanIterator;
  RC getNextTuple(RID &rid, void *data);
  RC close();
};


// RM_IndexScanIterator is an iterator to go through index entries
class RM_IndexScanIterator {
public:
  RM_IndexScanIterator() {};  	// Constructor
  ~RM_IndexScanIterator() {}; 	// Destructor
  IX_ScanIterator ix_scanner;
  // "key" follows the same format as in IndexManager::insertEntry()
  RC getNextEntry(RID &rid, void *key){
	  if(ix_scanner.getNextEntry(rid,key)!=RM_EOF)
		  return 0;
	  else
		  return RM_EOF;
  };  	// Get next matching entry

  RC close() {
	  IndexManager *ix=IndexManager::instance();
	  ix->closeFile(ix_scanner.ixHandle);
	  return 0;
  };            			// Terminate index scan
};

// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

  RC createIndex(const string &tableName, const string &attributeName);

  RC destroyIndex(const string &tableName, const string &attributeName);

  // indexScan returns an iterator to allow the caller to go through qualified entries in index
  RC indexScan(const string &tableName,
                        const string &attributeName,
                        const void *lowKey,
                        const void *highKey,
                        bool lowKeyInclusive,
                        bool highKeyInclusive,
                        RM_IndexScanIterator &rm_IndexScanIterator);

// Extra credit work (10 points)
public:
  RC addAttribute(const string &tableName, const Attribute &attr);

  RC dropAttribute(const string &tableName, const string &attributeName);


protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;

  //New Section added
  public:
    vector<Attribute> Tables_data;
    vector<Attribute> Columns_data;
    unsigned int tableId;
    unsigned int index_attr_tableID;
    string Tables;
    string Columns;
    RC updateSystemTables(const string &tableName, const vector<Attribute> &attrs, const int passedTableID);
    int getTableIDForNewInsert();
    void prepareTupleForColumnsTable(const int passedTableID, const int i, const Attribute &attrs, const int index, void *col_buffer);
    RC insertOrDeleteIndexEntry(FileHandle &fileHandle, const string &tableName, const int tableID, const string &insertOrdelete, const RID &rid, const vector<Attribute> &attrs);
    RC updateIndexColValue(const string &tableName, const string &attributeName, const int tableID, const Attribute &attrs, const int index);
};

#endif
