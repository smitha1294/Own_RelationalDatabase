#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan
//#define MAX 1000

typedef struct node
{
	int pagenum;
	int nextInsertOffset;
	int freespace;
	int isLeaf;
	int leftnode;
	int rightnode;
	int numOfKeys;
	int siblingNode;
//	int  *start;
//	int  *length;
	void * data;
}node;


class IX_ScanIterator;
class IXFileHandle;

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;
        //New..
        RC initialDirectory(IXFileHandle &ixfileHandle);
        int cut(int length);
        RC findKeyLength(const Attribute &attribute, const void *key);
        RC findRootPage(IXFileHandle &ixfileHandle) const;
        RC startNewTree(IXFileHandle &ixfileHandle,const Attribute &attribute,const void* key,const RID &rid);
        RC updateLeafSlotDirectory(node &leaf);
        RC findLeafNode(IXFileHandle &ixfileHandle,const Attribute &attribute,int root,const void *key);
        RC readNode(IXFileHandle &ixfileHandle,int pageNum,node &tempNode) const;
        RC insertIntoLeaf(IXFileHandle &ixfileHandle, node &leafNode, const Attribute &attribute, const void *key, const RID &rid);
        RC insertIntoLeafAfterSplitting(IXFileHandle &ixfileHandle,node &oldLeaf,const Attribute &attribute, const void *key, const RID &rid);
        RC findParent(IXFileHandle &ixfileHandle,int page,const Attribute &attribute);
        RC readFirstKey(node &node,void* key,const Attribute &attribute);
        RC createNewLeafNode(IXFileHandle &ixfileHandle,unsigned int page,node &node);
        RC shiftKeysToNewLeaf(node &oldLeaf,node &newLeaf,const Attribute &attribute,int &counter,int& dataSizeEnteredinNewLeaf,int cut);
        RC pushKeyToParentNode(IXFileHandle &ixfileHandle, const Attribute &attribute,void *key,int leftChildPage,int rightChildPage,node &parent);
        RC createNewIntermediateNode(IXFileHandle &ixfileHandle,unsigned int page,node &node,int leftChild,int rightChild);
        RC updateIntermediateNodeSlotDirectory(node &intermediateNode);
        RC insertIntoIntermediateNode(IXFileHandle &ixfileHandle,node &node, const Attribute &attribute,const void *key,int rightPointer);
        RC updateRoot(IXFileHandle &ixfileHandle,int newRoot);
        RC insertIntoIntermediateNodeAfterSplitting(IXFileHandle &ixfileHandle,int oldIntPage,const Attribute &attribute,const void *key,int rightPointer);
        RC deleteDataFromLeafNode(node &node,const Attribute &attribute,int cut);
        RC deleteDataFromIntermediateNode(node &node,const Attribute &attribute,int cut);
        RC shiftKeysToNewIntermediate(node &oldInt,node &newInt,const Attribute &attribute,int &counter,int& dataSizeEnteredinNewLeaf,int cut);
        RC readLastKeyAndLastPointerOfIntermediateNode(node &node,const Attribute &attribute,void *key,int &pointer);
        RC readFirstKeyAndFirstPointerOfIntermediateNode(node &node,const Attribute &attribute,void *key,int &pointer);
        RC deleteFirstKeyFromIntermediateNode(node &node,const Attribute &attribute);
        RC deleteKeyFromLeafNode(node &node,const Attribute &attribute, const void *key, const RID &rid);
        RC insertIntoRootAfterSplitting(IXFileHandle &ixfileHandle,const Attribute &attribute,const void *key,int rightPointer);
        RC getLeafNode(IXFileHandle &ixfileHandle,const Attribute &attribute,int root,const void *key,node &tempNode);
        RC getParent(IXFileHandle &ixfileHandle,int page,const Attribute &attribute,node &currentNode);

        //---print methods----
                void printRec(int pageNum, IXFileHandle &ixfileHandle, const Attribute &attribute) const;
                //myPrintMethod
                int printRIDAndUpdateOffset(int offsetToPrint, const Attribute &attribute, node &tempNode) const;
                int printKeyAndSetOffsetToKeyEnd(int offsetToPrint, const Attribute& attribute, const void* data) const;


    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
};

class IXFileHandle {
    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    FileHandle fileHandle;
    PagedFileManager *pfm;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

};

class IX_ScanIterator {
public:

	// Constructor
    IX_ScanIterator();

    // Destructor
    ~IX_ScanIterator();

    // Get next matching entry
    RC getNextEntry(RID &rid, void *key);

    // Terminate index scan
    RC close();
    IndexManager *ixManager;
            IXFileHandle ixHandle;
            Attribute attribute;
            const void *lowKey;
            const void *highKey;
            void * lowKeyStr;
            void * highKeyStr;
            bool lowKeyInclusive;
            bool highKeyInclusive;
            int matchingPage;
            void * checkVal;
            int nextOffset;
            int originalnextInsertOffset;

};





#endif
