#include "ix.h"
#include <cstring>

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
	RC rc;
	IXFileHandle ixfileHandle;
	rc=ixfileHandle.pfm->createFile(fileName);
    return rc;
}

RC IndexManager::destroyFile(const string &fileName)
{
	RC rc;
	IXFileHandle ixfileHandle;
	rc=ixfileHandle.pfm->destroyFile(fileName);
    return rc;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
	if (!std::ifstream(fileName))
		return -1;
	if(ixfileHandle.fileHandle.getFileName()==fileName)
		return -1; //File already open
	RC rc;
	rc=ixfileHandle.pfm->openFile(fileName,ixfileHandle.fileHandle);
	if(ixfileHandle.fileHandle.getNumberOfPages()==0)
			initialDirectory(ixfileHandle);
    return 0;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	RC rc;
	rc=ixfileHandle.pfm->closeFile(ixfileHandle.fileHandle);
    return rc;
}
RC IndexManager::findRootPage(IXFileHandle &ixfileHandle) const
{
		void* pageZero=(void*)malloc(PAGE_SIZE);
		int readPage=ixfileHandle.fileHandle.readPage(0,pageZero);
		int index=0;
		memcpy(&index,(char*)pageZero,sizeof(int));
		free(pageZero);
	return index;
	}
RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	int rootNodePage=findRootPage(ixfileHandle);
	int keyLength=findKeyLength(attribute,key);
	int ridLength=sizeof(unsigned)*2;
	if(rootNodePage==-1) //Fresh file,with no root
	{
		//Start new B+ tree
		int result=startNewTree(ixfileHandle,attribute,key,rid);
		updateRoot(ixfileHandle,1);
		return result;
	}
	//insert into existing nodes
	else
	{
//		int leafPage=findLeafNode(ixfileHandle,attribute,rootNodePage,key);
		node insertionNode;
		getLeafNode(ixfileHandle,attribute,rootNodePage,key,insertionNode);
//		readNode(ixfileHandle,leafPage,insertionNode);
		if(insertionNode.freespace>=(keyLength+ridLength))
		{ //Normal insertion
//			cout <<"Freespace in insertion leaf node:" <<insertionNode.freespace <<endl;

//			insertIntoLeaf(ixfileHandle,leafPage,attribute,key,rid);
			insertIntoLeaf(ixfileHandle,insertionNode,attribute,key,rid);
//			free(insertionNode.data);
			return 0;
		}
		else
		{//Splitting
//			cout<<"Leaf split case begins. !! "<<endl;
			insertIntoLeafAfterSplitting(ixfileHandle,insertionNode,attribute,key,rid);
			//free(insertionNode.data);
			return 0;
		}

	}
	return 0;
}
RC IndexManager::insertIntoLeafAfterSplitting(IXFileHandle &ixfileHandle,node &oldLeaf,const Attribute &attribute, const void *key, const RID &rid)
{
//	node oldLeaf;
//	readNode(ixfileHandle,oldLeafPage,oldLeaf);
//	cout <<"Append page counter value:"<<ixfileHandle.fileHandle.appendPageCounter<<endl;
//	int pageForNewLeaf=ixfileHandle.fileHandle.getNumberOfPages();
	int pageForNewLeaf=ixfileHandle.fileHandle.appendPageCounter;
//	cout<<"PAGE FOR NEW LEAF: "<<pageForNewLeaf<<endl;
	node newLeaf;
	createNewLeafNode(ixfileHandle,pageForNewLeaf,newLeaf);
	readNode(ixfileHandle,pageForNewLeaf,newLeaf);
	int splitKeys=cut(oldLeaf.numOfKeys);
	int counter=0;
	int dataSizeEnteredinNewLeaf=0;
	//Insert half data. Memcopies
	shiftKeysToNewLeaf(oldLeaf,newLeaf,attribute,counter,dataSizeEnteredinNewLeaf,splitKeys);
	//Delete first half from old buffer
	deleteDataFromLeafNode(oldLeaf,attribute,splitKeys);
	//Update leaf slot data
	newLeaf.freespace=newLeaf.freespace-dataSizeEnteredinNewLeaf;
	oldLeaf.freespace=oldLeaf.freespace+dataSizeEnteredinNewLeaf;
	newLeaf.numOfKeys+=counter;
	oldLeaf.numOfKeys-=counter;
	newLeaf.nextInsertOffset+=dataSizeEnteredinNewLeaf;
	oldLeaf.nextInsertOffset-=dataSizeEnteredinNewLeaf;
	newLeaf.siblingNode=oldLeaf.siblingNode;
	oldLeaf.siblingNode=newLeaf.pagenum;
//	cout<<"OLD LEAF PAGE: "<<oldLeaf.pagenum <<" NEW LEAF PAGE:" <<newLeaf.pagenum<<endl;
	updateLeafSlotDirectory(oldLeaf);
	updateLeafSlotDirectory(newLeaf);
	//Write leaf nodes to disk
	int wrPage=ixfileHandle.fileHandle.writePage(oldLeaf.pagenum,oldLeaf.data);
	wrPage=ixfileHandle.fileHandle.writePage(newLeaf.pagenum,newLeaf.data);
	//Insert lowest key of new leaf in parent intermediate node.
	void *firstKeyOfNewLeaf=malloc(PAGE_SIZE);
	memset(firstKeyOfNewLeaf,0,PAGE_SIZE);
	readFirstKey(newLeaf,firstKeyOfNewLeaf,attribute);
	free(oldLeaf.data);
	free(newLeaf.data);

	//---2 leaves written to disk successfully.

	//int parent=findParent(ixfileHandle,oldLeaf.pagenum,attribute);
	node parentNode;
//	parentNode.data=(void*)malloc(PAGE_SIZE);
	getParent(ixfileHandle,oldLeaf.pagenum,attribute,parentNode);
	pushKeyToParentNode(ixfileHandle,attribute,firstKeyOfNewLeaf,oldLeaf.pagenum,newLeaf.pagenum,parentNode);
	free(firstKeyOfNewLeaf);

	//Enter key to be inserted in one of the leaf nodes. Use findLeaf() and insert into leaf() function
	int keyLength=findKeyLength(attribute,key);
	int root=findRootPage(ixfileHandle);
//	int leafNode=findLeafNode(ixfileHandle,attribute,root,key);
	node leafNodeToInsert;
	getLeafNode(ixfileHandle,attribute,root,key,leafNodeToInsert);
//	readNode(ixfileHandle,leafNode,leafNodeToInsert);
	if(leafNodeToInsert.freespace>=(keyLength+sizeof(unsigned)*2))
	{
//		free(leafNodeToInsert.data);
//		cout <<"Inserting into leaf: "<<leafNode  <<endl;
		insertIntoLeaf(ixfileHandle,leafNodeToInsert,attribute,key,rid);
	}
	else
	{
		//CASE OF RECURSION
//		free(leafNodeToInsert.data);
		insertIntoLeafAfterSplitting(ixfileHandle,leafNodeToInsert,attribute,key,rid);
	}
	return 0;
}
RC IndexManager::deleteDataFromLeafNode(node &node,const Attribute &attribute,int cut){ //incase of split
	void *tempbuffer=malloc(PAGE_SIZE);
	memset(tempbuffer,0,PAGE_SIZE);
	int offset=0;
	for(int i=0;i<cut;i++)
	{
		if(attribute.type==TypeInt)
		{
			memcpy((char*)tempbuffer+offset,(char*)node.data+offset,sizeof(int));
			memcpy((char*)tempbuffer+offset+sizeof(int),(char*)node.data+offset+sizeof(int),sizeof(unsigned)*2);
			offset+=sizeof(int)+sizeof(unsigned)*2;
		}
		if(attribute.type==TypeReal)
		{
			memcpy((char*)tempbuffer+offset,(char*)node.data+offset,sizeof(float));
			memcpy((char*)tempbuffer+offset+sizeof(float),(char*)node.data+offset+sizeof(float),sizeof(unsigned)*2);
			offset+=sizeof(float)+sizeof(unsigned)*2;
		}
		if(attribute.type==TypeVarChar)
		{
			int sl;
			memcpy(&sl,(char*)node.data+offset,sizeof(int));
//			cout <<"String length: "<<sl <<endl;
			memcpy((char*)tempbuffer+offset,&sl,sizeof(int));
			memcpy((char*)tempbuffer+offset+sizeof(int),(char*)node.data+offset+sizeof(int),sl);
			memcpy((char*)tempbuffer+offset+sizeof(int)+sl,(char*)node.data+offset+sizeof(int)+sl,sizeof(unsigned)*2);
			offset+=sizeof(int)+sl+sizeof(unsigned)*2;
		}

	}
//	cout <<"Deletion offset:"<<offset<<endl;
	//Copy slot directory from old node data buffer to tempbuffer
	memcpy((char*)tempbuffer+PAGE_SIZE-sizeof(int)*6,(char*)node.data+PAGE_SIZE-sizeof(int)*6,sizeof(int)*6);
	//Copy tempbuffer entirely to node buffer
	memcpy((char*)node.data,(char*)tempbuffer,PAGE_SIZE);
	free(tempbuffer);
	return 0;
}
RC IndexManager::deleteDataFromIntermediateNode(node &node,const Attribute &attribute,int cut){
	void *tempbuffer=malloc(PAGE_SIZE);
	memset(tempbuffer,0,PAGE_SIZE);
	int offset=0;
	for(int i=-0;i<cut;i++)
	{
		if(attribute.type==TypeInt)
		{
			memcpy((char*)tempbuffer+offset,(char*)node.data+offset,sizeof(int));
			memcpy((char*)tempbuffer+offset+sizeof(int),(char*)node.data+offset+sizeof(int),sizeof(int));
			offset+=sizeof(int)*2;
		}
		if(attribute.type==TypeReal)
		{
			memcpy((char*)tempbuffer+offset,(char*)node.data+offset,sizeof(float));
			memcpy((char*)tempbuffer+offset+sizeof(float),(char*)node.data+offset+sizeof(float),sizeof(int));
			offset+=sizeof(float)+sizeof(int);
		}
		if(attribute.type==TypeVarChar)
		{
			int sl;
			memcpy(&sl,(char*)node.data+offset,sizeof(int));
//			cout<<"Del frm int node.String length: "<<sl<<endl;
			memcpy((char*)tempbuffer+offset,&sl,sizeof(int));
			memcpy((char*)tempbuffer+offset+sizeof(int),(char*)node.data+offset+sizeof(int),sl);
			memcpy((char*)tempbuffer+offset+sizeof(int)+sl,(char*)node.data+offset+sizeof(int)+sl,sizeof(int));
			offset+=sizeof(int)+sl+sizeof(int);
		}
	}
	//Copy slot directory from old node data buffer to tempbuffer
	memcpy((char*)tempbuffer+PAGE_SIZE-sizeof(int)*7,(char*)node.data+PAGE_SIZE-sizeof(int)*7,sizeof(int)*7);
	//Copy tempbuffer entirely to node buffer
	memcpy((char*)node.data,(char*)tempbuffer,PAGE_SIZE);
	free(tempbuffer);
}
RC IndexManager::pushKeyToParentNode(IXFileHandle &ixfileHandle,const Attribute &attribute,void *key,int leftChildPage,int rightChildPage,node &parent){

	int rootpage=findRootPage(ixfileHandle);
	//If parent node is leaf, then create new intermediate node, update root in index.
//	node parent;
//	readNode(ixfileHandle,parentPage,parent);
	int keyLength=findKeyLength(attribute,key);
	if(parent.isLeaf==1)
	{
		//Case of root node.Create new intermediate node
//		int pageNumForNewIntermediateNode=ixfileHandle.fileHandle.getNumberOfPages();
		int pageNumForNewIntermediateNode=ixfileHandle.fileHandle.appendPageCounter;
		node newIntermediateNode;
//		cout<<"PAGE FOR NEW INTERMEDIATE NODE/ROOT NODE: "<<pageNumForNewIntermediateNode<<endl;
		createNewIntermediateNode(ixfileHandle,pageNumForNewIntermediateNode,newIntermediateNode,leftChildPage,rightChildPage);
		readNode(ixfileHandle,pageNumForNewIntermediateNode,newIntermediateNode);
		//INSERT KEY
		insertIntoIntermediateNode(ixfileHandle,newIntermediateNode,attribute,key,rightChildPage);
		//Update root
		updateRoot(ixfileHandle,pageNumForNewIntermediateNode);
//		cout<<"New root updated: "<<pageNumForNewIntermediateNode <<endl;
		free(parent.data);

	}
	//Else insert in existing intermediate node. If intermediate node is full, then insert into intermediate node after splitting.
	else{
		if(parent.freespace>=(keyLength+sizeof(int)))
		{
		//	free(parent.data);
			node parentToSend;
			readNode(ixfileHandle,parent.pagenum,parentToSend);
			insertIntoIntermediateNode(ixfileHandle,parentToSend,attribute,key,rightChildPage);
		}
		else
		{
//			cout <<"CASE OF insertion to INTERMEDIATE NODE AFTER SPLITTING.........................."<<endl;
//			free(parent.data);
			insertIntoIntermediateNodeAfterSplitting(ixfileHandle,parent.pagenum,attribute,key,rightChildPage);

		}
	}
	return 0;
}
RC IndexManager::insertIntoIntermediateNodeAfterSplitting(IXFileHandle &ixfileHandle,int oldIntPage,const Attribute &attribute,const void *key,int rightPointer)
{
	int rootPage=findRootPage(ixfileHandle);
	if(oldIntPage==rootPage)
	{
		int result=insertIntoRootAfterSplitting(ixfileHandle,attribute,key,rightPointer);
		return result;
	}
	node oldIntermediate;
	readNode(ixfileHandle,oldIntPage,oldIntermediate);
//	int pageForNewIntermediateNode=ixfileHandle.fileHandle.getNumberOfPages();
	int pageForNewIntermediateNode=ixfileHandle.fileHandle.appendPageCounter;
	node newIntermediate;
	createNewIntermediateNode(ixfileHandle,pageForNewIntermediateNode,newIntermediate,-1,oldIntermediate.rightnode);
	readNode(ixfileHandle,pageForNewIntermediateNode,newIntermediate);
	int splitKeys=cut(oldIntermediate.numOfKeys);
	int counter=0;
	int dataSizeEnteredInNewIntermediateLeaf=0;
	shiftKeysToNewIntermediate(oldIntermediate,newIntermediate,attribute,counter,dataSizeEnteredInNewIntermediateLeaf,splitKeys);
	deleteDataFromIntermediateNode(oldIntermediate,attribute,splitKeys);
	newIntermediate.freespace-=dataSizeEnteredInNewIntermediateLeaf;
	oldIntermediate.freespace+=dataSizeEnteredInNewIntermediateLeaf;
	newIntermediate.numOfKeys+=counter;
	oldIntermediate.numOfKeys-=counter;
	newIntermediate.nextInsertOffset+=dataSizeEnteredInNewIntermediateLeaf;
	oldIntermediate.nextInsertOffset-=dataSizeEnteredInNewIntermediateLeaf;

	//Read first key,of new intermediate node, which is to be pushed up.
	void* keyToBepushedUp=malloc(PAGE_SIZE);
	memset(keyToBepushedUp,0,PAGE_SIZE);
	int rightPointerOfFirstKeyOfNewNode=0;
	readFirstKeyAndFirstPointerOfIntermediateNode(newIntermediate,attribute,keyToBepushedUp,rightPointerOfFirstKeyOfNewNode);

	//Delete key to be pushed from new Intermediate node buffer
	deleteFirstKeyFromIntermediateNode(newIntermediate,attribute);


	//Update left pointer of new int node, with right ptr of key pushed
	newIntermediate.leftnode=rightPointerOfFirstKeyOfNewNode;

	//Update right pointer of new node with old node's old right pointer
	newIntermediate.rightnode=oldIntermediate.rightnode;

	//Old node's new right pointer= New last element in new node's right pointer
	void* lastKeyOfOldIntermediateNode=malloc(PAGE_SIZE);
	memset(lastKeyOfOldIntermediateNode,0,PAGE_SIZE);
	int pointerOfLastKeyOfOldIntNode;
	readLastKeyAndLastPointerOfIntermediateNode(oldIntermediate,attribute,lastKeyOfOldIntermediateNode,pointerOfLastKeyOfOldIntNode);
	oldIntermediate.rightnode=pointerOfLastKeyOfOldIntNode;
	free(lastKeyOfOldIntermediateNode);
	//Finally,insert key in either of old or new node. if key < key pushed , insert into old node, else, insert in new node
	if(attribute.type==TypeInt || attribute.type==TypeReal)
	{
		int valueAtKeyPushed,valueKey;
		memcpy(&valueAtKeyPushed,(char*)keyToBepushedUp,sizeof(int));
		memcpy(&valueKey,(char*)key,sizeof(int));
		if(valueKey<valueAtKeyPushed)
		{
			insertIntoIntermediateNode(ixfileHandle,oldIntermediate,attribute,key,rightPointer);
			//Old int node already getting written inside insert, so just write new int node.
			updateIntermediateNodeSlotDirectory(newIntermediate);
			int writePage=ixfileHandle.fileHandle.writePage(newIntermediate.pagenum,newIntermediate.data);
			free(newIntermediate.data);
		}
		else
		{
			insertIntoIntermediateNode(ixfileHandle,newIntermediate,attribute,key,rightPointer);
			updateIntermediateNodeSlotDirectory(oldIntermediate);
			int writePage=ixfileHandle.fileHandle.writePage(oldIntermediate.pagenum,oldIntermediate.data);
			free(oldIntermediate.data);
		}
	}
	else //Case of TypeVarchar
	{
		int lengthOfKeyi;
		memcpy(&lengthOfKeyi, (char*)keyToBepushedUp,sizeof(int));
		string strKeyPushed = string((char*) keyToBepushedUp+sizeof(int),lengthOfKeyi);
		int lengthOfKey;
		memcpy(&lengthOfKey,(char*)key,sizeof(int));
		string strKey = string((char*)key+sizeof(int),lengthOfKey);
		if (strKey.compare(strKeyPushed) < 0)
		{
			insertIntoIntermediateNode(ixfileHandle,oldIntermediate,attribute,key,rightPointer);
			//Old int node already getting written inside insert, so just write new int node.
			int writePage=ixfileHandle.fileHandle.writePage(newIntermediate.pagenum,newIntermediate.data);
			free(newIntermediate.data);
		}
		else
		{
			insertIntoIntermediateNode(ixfileHandle,newIntermediate,attribute,key,rightPointer);
			int writePage=ixfileHandle.fileHandle.writePage(oldIntermediate.pagenum,oldIntermediate.data);
			free(oldIntermediate.data);
		}
	}
	//Push key and its pointer=new int node page# ,up to parent node
	//int parent=findParent(ixfileHandle,oldIntPage,attribute);
	node parentNode;
//	parentNode.data=malloc(PAGE_SIZE);
	getParent(ixfileHandle,oldIntPage,attribute,parentNode);
	pushKeyToParentNode(ixfileHandle,attribute,keyToBepushedUp,oldIntPage,newIntermediate.pagenum,parentNode);
	free(keyToBepushedUp);
	return 0;
}
RC IndexManager::insertIntoRootAfterSplitting(IXFileHandle &ixfileHandle,const Attribute &attribute,const void *key,int rightPointer)
{
//	cout <<"CASE OF INSERT INTO ROOT AFTER SPLIT."<<endl;
	int rootPage=findRootPage(ixfileHandle);
	node oldRootPage;
	readNode(ixfileHandle,rootPage,oldRootPage);
//	int pageForNewIntermediateNode=ixfileHandle.fileHandle.getNumberOfPages();
	int pageForNewIntermediateNode=ixfileHandle.fileHandle.appendPageCounter;
	node newIntermediate;
	createNewIntermediateNode(ixfileHandle,pageForNewIntermediateNode,newIntermediate,-1,oldRootPage.rightnode);
	readNode(ixfileHandle,pageForNewIntermediateNode,newIntermediate);
	int splitKeys=cut(oldRootPage.numOfKeys);
	int counter=0;
	int dataSizeEnteredInNewIntermediateLeaf=0;
	shiftKeysToNewIntermediate(oldRootPage,newIntermediate,attribute,counter,dataSizeEnteredInNewIntermediateLeaf,splitKeys);
	deleteDataFromIntermediateNode(oldRootPage,attribute,splitKeys);
	newIntermediate.freespace-=dataSizeEnteredInNewIntermediateLeaf;
	oldRootPage.freespace+=dataSizeEnteredInNewIntermediateLeaf;
	newIntermediate.numOfKeys+=counter;
	oldRootPage.numOfKeys-=counter;
	newIntermediate.nextInsertOffset+=dataSizeEnteredInNewIntermediateLeaf;
	oldRootPage.nextInsertOffset-=dataSizeEnteredInNewIntermediateLeaf;
	//Read first key,of new intermediate node, which is to be pushed up.
	void* keyToBepushedUp=malloc(PAGE_SIZE);
	memset(keyToBepushedUp,0,PAGE_SIZE);
	int rightPointerOfFirstKeyOfNewNode=0;
	readFirstKeyAndFirstPointerOfIntermediateNode(newIntermediate,attribute,keyToBepushedUp,rightPointerOfFirstKeyOfNewNode);
	//Delete key to be pushed from new Intermediate node buffer
	deleteFirstKeyFromIntermediateNode(newIntermediate,attribute);
	//Update left pointer of new int node, with right ptr of key pushed
	newIntermediate.leftnode=rightPointerOfFirstKeyOfNewNode;

	//Update right pointer of new node with old node's old right pointer
	newIntermediate.rightnode=oldRootPage.rightnode;
//	cout <<"New intermediate node : freespace, num keys, next insert offset, left pt, right ptr: "<<newIntermediate.freespace<<","<<newIntermediate.numOfKeys<<","<<newIntermediate.leftnode<<","<<newIntermediate.rightnode<<endl;

	//Old node's new right pointer= New last element in new node's right pointer
	void* lastKeyOfOldIntermediateNode=malloc(PAGE_SIZE);
	memset(lastKeyOfOldIntermediateNode,0,PAGE_SIZE);
	int pointerOfLastKeyOfOldIntNode;
	readLastKeyAndLastPointerOfIntermediateNode(oldRootPage,attribute,lastKeyOfOldIntermediateNode,pointerOfLastKeyOfOldIntNode);
	oldRootPage.rightnode=pointerOfLastKeyOfOldIntNode;

	//Finally,insert key in either of old or new node. if key < key pushed , insert into old node, else, insert in new node
		if(attribute.type==TypeInt || attribute.type==TypeReal)
		{
			int valueAtKeyPushed,valueKey;
			memcpy(&valueAtKeyPushed,(char*)keyToBepushedUp,sizeof(int));
			memcpy(&valueKey,(char*)key,sizeof(int));
			if(valueKey<valueAtKeyPushed)
			{
				insertIntoIntermediateNode(ixfileHandle,oldRootPage,attribute,key,rightPointer);
				//Old int node already getting written inside insert, so just write new int node.
				updateIntermediateNodeSlotDirectory(newIntermediate);
				int writePage=ixfileHandle.fileHandle.writePage(newIntermediate.pagenum,newIntermediate.data);
				free(newIntermediate.data);
			}
			else
			{
				insertIntoIntermediateNode(ixfileHandle,newIntermediate,attribute,key,rightPointer);
				updateIntermediateNodeSlotDirectory(oldRootPage);
				int writePage=ixfileHandle.fileHandle.writePage(oldRootPage.pagenum,oldRootPage.data);
				free(oldRootPage.data);
			}
		}
		else //Case of TypeVarchar
		{
			int lengthOfKeyi;
			memcpy(&lengthOfKeyi, (char*)keyToBepushedUp,sizeof(int));
			string strKeyPushed = string((char*) keyToBepushedUp+sizeof(int),lengthOfKeyi);
			int lengthOfKey;
			memcpy(&lengthOfKey,(char*)key,sizeof(int));
			string strKey = string((char*)key+sizeof(int),lengthOfKey);
			if (strKey.compare(strKeyPushed) < 0)
			{
				insertIntoIntermediateNode(ixfileHandle,oldRootPage,attribute,key,rightPointer);
				//Old int node already getting written inside insert, so just write new int node.
				int writePage=ixfileHandle.fileHandle.writePage(newIntermediate.pagenum,newIntermediate.data);
				free(newIntermediate.data);
			}
			else
			{
				insertIntoIntermediateNode(ixfileHandle,newIntermediate,attribute,key,rightPointer);
				int writePage=ixfileHandle.fileHandle.writePage(oldRootPage.pagenum,oldRootPage.data);
				free(oldRootPage.data);
			}
		}
		free(lastKeyOfOldIntermediateNode);


	//Add new root after writing the 2 nodes on disk. Update root in index.
//	int pageForNewRoot=ixfileHandle.fileHandle.getNumberOfPages();
	int pageForNewRoot=ixfileHandle.fileHandle.appendPageCounter;
	node newRoot;
	readNode(ixfileHandle,pageForNewRoot,newRoot);
	createNewIntermediateNode(ixfileHandle,pageForNewRoot,newRoot,rootPage,pageForNewIntermediateNode);
//	cout <<"New root created: "<<newRoot.pagenum <<endl;
	updateRoot(ixfileHandle,pageForNewRoot);
	node newRootRead;
	readNode(ixfileHandle,pageForNewRoot,newRootRead);
	//Push key to new root.
	insertIntoIntermediateNode(ixfileHandle,newRootRead,attribute,keyToBepushedUp,pageForNewIntermediateNode);
	free(keyToBepushedUp);
	return 0;
}
RC IndexManager::deleteFirstKeyFromIntermediateNode(node &node,const Attribute &attribute)
{
	void *tempbuffer=malloc(PAGE_SIZE);
	memset(tempbuffer,0,PAGE_SIZE);
	int offset=0;
		if(attribute.type==TypeInt || attribute.type==TypeReal)
		{
			offset=sizeof(int)+sizeof(int);
		}
		if(attribute.type==TypeVarChar)
		{
			int sl;
			memcpy(&sl,(char*)node.data,sizeof(int));
			offset=sizeof(int)+sl+sizeof(int);
		}
//		cout<<"First key del from int node, length="<<offset <<endl;
	memcpy((char*)tempbuffer,(char*)node.data+offset,(node.nextInsertOffset-offset));
	node.freespace+=offset;
	node.numOfKeys-=1;
	node.nextInsertOffset-=offset;
	return 0;
}
RC IndexManager::readLastKeyAndLastPointerOfIntermediateNode(node &node,const Attribute &attribute,void *key,int &pointer)
{
	int offset=0;
	for(int i=0;i<node.numOfKeys;i++)
	{
		if(attribute.type==TypeInt)
		{
			if(i==(node.numOfKeys-1))
			{
				memcpy((char*)key,(char*)node.data+offset,sizeof(int));
				memcpy(&pointer,(char*)node.data+offset+sizeof(int),sizeof(int));
			}
			offset+=sizeof(int)*2;
		}
		if(attribute.type==TypeReal)
		{
			if(i==(node.numOfKeys-1))
			{
				memcpy((char*)key,(char*)node.data+offset,sizeof(float));
				memcpy(&pointer,(char*)node.data+offset+sizeof(float),sizeof(int));
			}
			offset+=sizeof(float)+sizeof(int);
		}
		if(attribute.type==TypeVarChar)
		{
			int sl;
			memcpy(&sl,(char*)node.data+offset,sizeof(int));
//			cout<<"Last key of int node. length: "<<sl<<endl;
			if(i==(node.numOfKeys-1))
			{
				memcpy((char*)key,(char*)node.data+offset,sizeof(int));
				memcpy((char*)key+sizeof(int),(char*)node.data+offset+sizeof(int),sl);
				memcpy(&pointer,(char*)node.data+offset+sizeof(int)+sl,sizeof(int));
			}
			offset+=sizeof(int)+sl+sizeof(int);
		}
	}
	return 0;
}
RC IndexManager::shiftKeysToNewIntermediate(node &oldInt,node &newInt,const Attribute &attribute,int &counter,int& dataSizeEnteredinNewLeaf,int cut)
{
	int offset=0;
	for(int i=0;i<oldInt.numOfKeys;i++)
	{
		if(attribute.type==TypeInt)
		{
			if(i>=cut)
			{
				memcpy((char*)newInt.data+dataSizeEnteredinNewLeaf,(char*)oldInt.data+offset,sizeof(int));
				memcpy((char*)newInt.data+dataSizeEnteredinNewLeaf+sizeof(int),(char*)oldInt.data+offset+sizeof(int),sizeof(int));
				dataSizeEnteredinNewLeaf+=sizeof(int)*2;
				counter+=1;
			}offset+=sizeof(int)*2;
		}
		if(attribute.type==TypeReal)
		{
			if(i>=cut)
			{
				memcpy((char*)newInt.data+dataSizeEnteredinNewLeaf,(char*)oldInt.data+offset,sizeof(float));
				memcpy((char*)newInt.data+dataSizeEnteredinNewLeaf+sizeof(float),(char*)oldInt.data+offset+sizeof(float),sizeof(int));
				dataSizeEnteredinNewLeaf+=sizeof(int)+sizeof(float);
				counter+=1;
			}offset+=sizeof(float)+sizeof(int);
		}
		if(attribute.type==TypeVarChar)
		{
			int sl;
			memcpy(&sl,(char*)oldInt.data+offset,sizeof(int));
//			cout<<"Shift keys to int node: "<<sl<<endl;
			offset+=sizeof(int);
			if(i>=cut)
			{
				memcpy((char*)newInt.data+dataSizeEnteredinNewLeaf,&sl,sizeof(int));
				memcpy((char*)newInt.data+dataSizeEnteredinNewLeaf+sizeof(int),(char*)oldInt.data+offset,sl);
				memcpy((char*)newInt.data+dataSizeEnteredinNewLeaf+sizeof(int)+sl,(char*)oldInt.data+offset+sl,sizeof(int));
				dataSizeEnteredinNewLeaf+=sizeof(int)+sl+sizeof(int);
				counter+=1;
			}offset+=sl+sizeof(int);
		}
	}
	return 0;
}
RC IndexManager::updateRoot(IXFileHandle &ixfileHandle,int newRoot)
{
	void *indexPage=(void*)malloc(PAGE_SIZE);
	memset(indexPage,0,PAGE_SIZE);
	//int readPage=ixfileHandle.fileHandle.readPage(0,indexPage);
	memcpy((char*)indexPage,&newRoot,sizeof(int));
	int wrIndex=ixfileHandle.fileHandle.writePage(0,indexPage);
	free(indexPage);
	return wrIndex;
}
RC IndexManager::insertIntoIntermediateNode(IXFileHandle &ixfileHandle, node &node, const Attribute &attribute, const void *key,int rightPointer)
{
	int keyLength=findKeyLength(attribute,key);
	int offset=0;
	for(int i=0;i<node.numOfKeys;i++)
	{
		if(attribute.type==TypeInt)
		{	int valueAtKeyi,valueKey;
			memcpy(&valueAtKeyi,(char*)node.data+offset,sizeof(int));
			memcpy(&valueKey,(char*)key,sizeof(int));
			if(valueKey>=valueAtKeyi)
			{
				offset+=sizeof(int)*2; //Key and right pointer
				continue;
			}
			else
				break;
		}
		if(attribute.type==TypeReal)
		{	float valueAtKeyi,valueKey;
			memcpy(&valueAtKeyi,(char*)node.data+offset,sizeof(float));
			memcpy(&valueKey,(char*)key,sizeof(float));
			if(valueKey>=valueAtKeyi)
			{
				offset+=sizeof(float)+sizeof(int);
				continue;
			}
			else
				break;
		}
		if(attribute.type==TypeVarChar)
		{
			int lengthOfKeyi;
			memcpy(&lengthOfKeyi, (char*)node.data + offset, sizeof(int));
			offset += sizeof(int); //int-string length
			string strKeyi = string((char*) node.data + offset,lengthOfKeyi);
			int lengthOfKey;
			memcpy(&lengthOfKey,(char*)key,sizeof(int));
//			cout<<"Insert into int node: key length: "<<lengthOfKey<<endl;
			string strKey = string((char*)key+sizeof(int),lengthOfKey);
			if (strKey.compare(strKeyi) > 0 || strKey.compare(strKeyi) == 0) {
				offset += lengthOfKeyi+sizeof(int); //int-right pointer
				continue;
			} else {
				offset -= sizeof(int);
				break;
			}
		}
	}
	//Insert key at offset i
	void* tempbuffer=malloc(PAGE_SIZE);
	memset(tempbuffer,0,PAGE_SIZE);
	memcpy((char*)tempbuffer,(char*)node.data,offset);
	memcpy((char*)tempbuffer+offset,(char*)key,keyLength);
	memcpy((char*)tempbuffer+offset+keyLength,&rightPointer,sizeof(int));
	memcpy((char*)tempbuffer+offset+keyLength+sizeof(int),(char*)node.data+offset,node.nextInsertOffset-offset);
//	cout <<"Intermediate node data:"<<node.pagenum <<","<<node.freespace <<","<<node.nextInsertOffset <<","<<node.numOfKeys <<endl;

	//Update slot directory in intermediate node
	if(offset==node.nextInsertOffset)
	{
		node.rightnode=rightPointer;
	}
	node.freespace-=(keyLength+sizeof(int));
	node.numOfKeys+=1;
	node.nextInsertOffset+=(keyLength+sizeof(int));
	updateIntermediateNodeSlotDirectory(node);
	//write slot directory to tempbuffer
	memcpy((char*)tempbuffer+PAGE_SIZE-sizeof(int)*7,(char*)node.data+PAGE_SIZE-sizeof(int)*7,sizeof(int)*7);
	int wrPage=ixfileHandle.fileHandle.writePage(node.pagenum,tempbuffer);
//	cout <<"Intermediate node write done:" <<node.pagenum<<endl;
	free(tempbuffer);
	free(node.data);
	return 0;
}
RC IndexManager::createNewIntermediateNode(IXFileHandle &ixfileHandle,unsigned int page,node &node,int leftChild,int rightChild)
{
	node.isLeaf=0;
	node.freespace=PAGE_SIZE-sizeof(int)*7;
	node.numOfKeys=0;
	node.nextInsertOffset=0;
	node.pagenum=page;
	node.leftnode=leftChild;
	node.rightnode=rightChild;
	node.siblingNode=-1;
	node.data=malloc(PAGE_SIZE);
	memset(node.data,0,PAGE_SIZE);
	updateIntermediateNodeSlotDirectory(node);
//	cout <<"Node page number as received in arguments:"<<node.pagenum<<endl;
//	cout <<"Append page counter:"<<ixfileHandle.fileHandle.appendPageCounter<<endl;
	int ap=ixfileHandle.fileHandle.appendPage(node.data);
//	cout<<"New append page counter:"<<ixfileHandle.fileHandle.appendPageCounter<<endl;
//	int writePage=ixfileHandle.fileHandle.writePage(page,node.data);
	free(node.data);
	return 0;
}
RC IndexManager::updateIntermediateNodeSlotDirectory(node &intermediateNode){
	memcpy((char*)intermediateNode.data+PAGE_SIZE-sizeof(int),&intermediateNode.isLeaf,sizeof(int));
	memcpy((char*)intermediateNode.data+PAGE_SIZE-sizeof(int)*2,&intermediateNode.freespace,sizeof(int));
	memcpy((char*)intermediateNode.data+PAGE_SIZE-sizeof(int)*3,&intermediateNode.numOfKeys,sizeof(int));
	memcpy((char*)intermediateNode.data+PAGE_SIZE-sizeof(int)*4,&intermediateNode.nextInsertOffset,sizeof(int));
	memcpy((char*)intermediateNode.data+PAGE_SIZE-sizeof(int)*5,&intermediateNode.pagenum,sizeof(int));
	memcpy((char*)intermediateNode.data+PAGE_SIZE-sizeof(int)*6,&intermediateNode.leftnode,sizeof(int));
	memcpy((char*)intermediateNode.data+PAGE_SIZE-sizeof(int)*7,&intermediateNode.rightnode,sizeof(int));
	return 0;
}
RC IndexManager::shiftKeysToNewLeaf(node &oldLeaf,node &newLeaf,const Attribute &attribute,int &counter,int& dataSizeEnteredinNewLeaf,int cut)
{
	int offset=0;
	for(int i=0;i<oldLeaf.numOfKeys;i++)
	{
		if(attribute.type==TypeInt)
		{
			if(i>=cut)
			{
				memcpy((char*)newLeaf.data+dataSizeEnteredinNewLeaf,(char*)oldLeaf.data+offset,sizeof(int));
				memcpy((char*)newLeaf.data+dataSizeEnteredinNewLeaf+sizeof(int),(char*)oldLeaf.data+offset+sizeof(int),sizeof(unsigned)*2);
				dataSizeEnteredinNewLeaf+=sizeof(int)+sizeof(unsigned)*2;
				counter+=1;
			}
			offset+=sizeof(int)+sizeof(unsigned)*2;
		}
		if(attribute.type==TypeReal)
		{
			if(i>=cut)
			{
				memcpy((char*)newLeaf.data+dataSizeEnteredinNewLeaf,(char*)oldLeaf.data+offset,sizeof(float));
				memcpy((char*)newLeaf.data+dataSizeEnteredinNewLeaf+sizeof(float),(char*)oldLeaf.data+offset+sizeof(float),sizeof(unsigned)*2);
				dataSizeEnteredinNewLeaf+=sizeof(float)+sizeof(unsigned)*2;
				counter+=1;
			}
			offset+=sizeof(float)+sizeof(unsigned)*2;
		}
		if(attribute.type==TypeVarChar)
		{
			int sl;
			memcpy(&sl,(char*)oldLeaf.data+offset,sizeof(int));
//			cout<<"shift keys to new leaf: length:" <<sl<<endl;
			offset+=sizeof(int);
			if(i>=cut)
			{
				memcpy((char*)newLeaf.data+dataSizeEnteredinNewLeaf,&sl,sizeof(int));
				memcpy((char*)newLeaf.data+dataSizeEnteredinNewLeaf+sizeof(int),(char*)oldLeaf.data+offset,sl);
				memcpy((char*)newLeaf.data+dataSizeEnteredinNewLeaf+sizeof(int)+sl,(char*)oldLeaf.data+offset+sl,sizeof(unsigned)*2);
				dataSizeEnteredinNewLeaf+=sizeof(int)+sl+sizeof(unsigned)*2;
				counter+=1;
			}
			offset+=sl+sizeof(unsigned)*2;
		}
	}
	return 0;
}
RC IndexManager::createNewLeafNode(IXFileHandle &ixfileHandle,unsigned int page,node &node)
{
	node.isLeaf=1;
	node.freespace=PAGE_SIZE-sizeof(int)*6;
	node.numOfKeys=0;
	node.nextInsertOffset=0;
	node.pagenum=page;
	node.siblingNode=-1;
	node.data=malloc(PAGE_SIZE);
	memset(node.data,0,PAGE_SIZE);
	updateLeafSlotDirectory(node);
//	cout <<"Append page counter:"<<ixfileHandle.fileHandle.appendPageCounter<<endl;
	int ap=ixfileHandle.fileHandle.appendPage(node.data);
//	cout<<"New append page counter:" <<ixfileHandle.fileHandle.appendPageCounter<<endl;
//	int writePage=ixfileHandle.fileHandle.writePage(page,node.data);
	free(node.data);
	return 0;
}
RC IndexManager::findParent(IXFileHandle &ixfileHandle,int page,const Attribute &attribute) //find parent node for any node (leaf/intermediate)
{
	//Use Stack class to store recent visited pages.
	node childNode;
	readNode(ixfileHandle,page,childNode);
	void *key=malloc(PAGE_SIZE);
	memset(key,0,PAGE_SIZE);
	readFirstKey(childNode,key,attribute);
	int keyLength=findKeyLength(attribute,key);
	int parent=findRootPage(ixfileHandle);
//	cout <<"Root inside Find Parent: "<<parent<<endl;
	int tempPage=0;
	node currentNode;
	readNode(ixfileHandle,parent,currentNode);
	while (currentNode.pagenum != childNode.pagenum) {
		int i = 0;
		int offset = 0;
		while (i < currentNode.numOfKeys) {
			if (attribute.type == TypeInt) {
				int valueAtKeyi;
				int valueKey;
				memcpy(&valueAtKeyi, (char*) currentNode.data + offset,
						sizeof(int));
				memcpy(&valueKey, (char*) key, sizeof(int));
				if (valueKey >= valueAtKeyi) {
					i++;
					offset += sizeof(int);
					offset += sizeof(int);
					memcpy(&tempPage,(char*) currentNode.data + offset - sizeof(int),sizeof(int));
//					if (i == 0) {
//					memcpy(&tempPage,(char*) currentNode.data + PAGE_SIZE- sizeof(int) * 6, sizeof(int));}
//					else {
//					memcpy(&tempPage,(char*) currentNode.data + offset - sizeof(int),sizeof(int));
//						}
					continue;
				} else {
					if (i == 0) {
						memcpy(&tempPage,
								(char*) currentNode.data + PAGE_SIZE
										- sizeof(int) * 6, sizeof(int));
						break;
					} else {
						memcpy(&tempPage,
								(char*) currentNode.data + offset - sizeof(int),
								sizeof(int));
						break;
					}
				}
			}
			if (attribute.type == TypeReal) {
				float valueAtKeyi;
				float valueKey;
				memcpy(&valueAtKeyi, (char*) currentNode.data + offset,
						sizeof(float));
				memcpy(&valueKey, (char*) key, sizeof(float));
				if (valueKey >= valueAtKeyi) {
					i++;
					offset += sizeof(float);
					offset += sizeof(int);
					memcpy(&tempPage,(char*) currentNode.data + offset - sizeof(float),sizeof(int));
//					if (i == 0) {
//					memcpy(&tempPage,(char*) currentNode.data + PAGE_SIZE- sizeof(int) * 6, sizeof(int));}
//					else {
//					memcpy(&tempPage,(char*) currentNode.data + offset - sizeof(int),sizeof(int));
//					}
					continue;
				} else {
					if (i == 0) {
						memcpy(&tempPage,
								(char*) currentNode.data + PAGE_SIZE
										- sizeof(int) * 6, sizeof(int));
						break;
					} else {
						memcpy(&tempPage,
								(char*) currentNode.data + offset - sizeof(int),
								sizeof(int));
						break;
					}
				}
			}
			if (attribute.type == TypeVarChar) {
				int lengthOfKeyi;
				memcpy(&lengthOfKeyi, (char*) currentNode.data + offset,
						sizeof(int));
				offset += sizeof(int);
				string strKeyi = string((char*) currentNode.data + offset,
						lengthOfKeyi);
				int lengthOfKey;
				memcpy(&lengthOfKey,(char*)key,sizeof(int));
				string strKey = string((char*)key+sizeof(int),lengthOfKey);
				if (strKey.compare(strKeyi) > 0
						|| strKey.compare(strKeyi) == 0) {
					i++;
					offset += lengthOfKeyi;
					offset += sizeof(int);
					memcpy(&tempPage,(char*)currentNode.data+offset-sizeof(int),sizeof(int));
//					if (i == 0) {	//Left child pointer
//					memcpy(&tempPage,(char*) currentNode.data + PAGE_SIZE- sizeof(int) * 6, sizeof(int));
//					} else {
//					memcpy(&tempPage,(char*) currentNode.data + offset- sizeof(int) * 2, sizeof(int));
//					}
					continue;
				} else {
					if (i == 0) {	//Left child pointer
						memcpy(&tempPage,(char*) currentNode.data + PAGE_SIZE- sizeof(int) * 6, sizeof(int));
						break;
					} else {
						memcpy(&tempPage,(char*) currentNode.data + offset- sizeof(int) * 2, sizeof(int));
						break;
					}
				}
			}

		}
		parent=currentNode.pagenum;
		free(currentNode.data);
		readNode(ixfileHandle,tempPage,currentNode);
	}
	free(key);
	free(currentNode.data);
	free(childNode.data);
//	cout <<"Parent for child node : "<< page <<", is : "<<parent <<endl;
	return parent;
}
RC IndexManager::getParent(IXFileHandle &ixfileHandle,int page,const Attribute &attribute,node &parentNode)
{
		node childNode;
		readNode(ixfileHandle,page,childNode);
		void *key=malloc(PAGE_SIZE);
		memset(key,0,PAGE_SIZE);
		readFirstKey(childNode,key,attribute);
		int keyLength=findKeyLength(attribute,key);
		int parent=findRootPage(ixfileHandle);
		int tempPage=0;
		node currentNode;
		readNode(ixfileHandle,parent,currentNode);
		readNode(ixfileHandle,currentNode.pagenum,parentNode);
		while (currentNode.pagenum != childNode.pagenum) {
			int i = 0;
			int offset = 0;
			while (i < currentNode.numOfKeys) {
				if (attribute.type == TypeInt) {
					int valueAtKeyi;
					int valueKey;
					memcpy(&valueAtKeyi, (char*) currentNode.data + offset,
							sizeof(int));
					memcpy(&valueKey, (char*) key, sizeof(int));
					if (valueKey >= valueAtKeyi) {
						i++;
						offset += sizeof(int);
						offset += sizeof(int);
						memcpy(&tempPage,(char*) currentNode.data + offset - sizeof(int),sizeof(int));
	//					if (i == 0) {
	//					memcpy(&tempPage,(char*) currentNode.data + PAGE_SIZE- sizeof(int) * 6, sizeof(int));}
	//					else {
	//					memcpy(&tempPage,(char*) currentNode.data + offset - sizeof(int),sizeof(int));
	//						}
						continue;
					} else {
						if (i == 0) {
							memcpy(&tempPage,
									(char*) currentNode.data + PAGE_SIZE
											- sizeof(int) * 6, sizeof(int));
							break;
						} else {
							memcpy(&tempPage,
									(char*) currentNode.data + offset - sizeof(int),
									sizeof(int));
							break;
						}
					}
				}
				if (attribute.type == TypeReal) {
					float valueAtKeyi;
					float valueKey;
					memcpy(&valueAtKeyi, (char*) currentNode.data + offset,
							sizeof(float));
					memcpy(&valueKey, (char*) key, sizeof(float));
					if (valueKey >= valueAtKeyi) {
						i++;
						offset += sizeof(float);
						offset += sizeof(int);
						memcpy(&tempPage,(char*) currentNode.data + offset - sizeof(float),sizeof(int));
	//					if (i == 0) {
	//					memcpy(&tempPage,(char*) currentNode.data + PAGE_SIZE- sizeof(int) * 6, sizeof(int));}
	//					else {
	//					memcpy(&tempPage,(char*) currentNode.data + offset - sizeof(int),sizeof(int));
	//					}
						continue;
					} else {
						if (i == 0) {
							memcpy(&tempPage,
									(char*) currentNode.data + PAGE_SIZE
											- sizeof(int) * 6, sizeof(int));
							break;
						} else {
							memcpy(&tempPage,
									(char*) currentNode.data + offset - sizeof(int),
									sizeof(int));
							break;
						}
					}
				}
				if (attribute.type == TypeVarChar) {
					int lengthOfKeyi;
					memcpy(&lengthOfKeyi, (char*) currentNode.data + offset,
							sizeof(int));
					offset += sizeof(int);
					string strKeyi = string((char*) currentNode.data + offset,
							lengthOfKeyi);
					int lengthOfKey;
					memcpy(&lengthOfKey,(char*)key,sizeof(int));
					string strKey = string((char*)key+sizeof(int),lengthOfKey);
					if (strKey.compare(strKeyi) > 0
							|| strKey.compare(strKeyi) == 0) {
						i++;
						offset += lengthOfKeyi;
						offset += sizeof(int);
						memcpy(&tempPage,(char*)currentNode.data+offset-sizeof(int),sizeof(int));
	//					if (i == 0) {	//Left child pointer
	//					memcpy(&tempPage,(char*) currentNode.data + PAGE_SIZE- sizeof(int) * 6, sizeof(int));
	//					} else {
	//					memcpy(&tempPage,(char*) currentNode.data + offset- sizeof(int) * 2, sizeof(int));
	//					}
						continue;
					} else {
						if (i == 0) {	//Left child pointer
							memcpy(&tempPage,(char*) currentNode.data + PAGE_SIZE- sizeof(int) * 6, sizeof(int));
							break;
						} else {
							memcpy(&tempPage,(char*) currentNode.data + offset- sizeof(int) * 2, sizeof(int));
							break;
						}
					}
				}

			}
			parentNode=currentNode;
//			parentNode.data=currentNode.data;
//			updateIntermediateNodeSlotDirectory(parentNode);
			parent=currentNode.pagenum;
			free(currentNode.data);
			readNode(ixfileHandle,tempPage,currentNode);
		}
		free(key);
		free(childNode.data);
		free(currentNode.data);
//		cout <<"Parent for child node : "<< page <<", is : "<<parentNode.pagenum <<endl;

		return 0; //Updates currentNode with parent

}
RC IndexManager::readFirstKey(node &node,void* key,const Attribute &attribute)
{
	if(attribute.type==TypeInt || attribute.type==TypeReal)
		memcpy((char*)key,(char*)node.data,sizeof(int));
	else
	{
		int stringLength;
		memcpy(&stringLength,(char*)node.data,sizeof(int));
		memcpy((char*)key,&stringLength,sizeof(int));
		memcpy((char*)key+sizeof(int),(char*)node.data+sizeof(int),stringLength);
	}
	return 0;
}
RC IndexManager::readFirstKeyAndFirstPointerOfIntermediateNode(node &node,const Attribute &attribute,void *key,int &pointer)
{
	if(attribute.type==TypeInt || attribute.type==TypeReal)
		{
		memcpy((char*)key,(char*)node.data,sizeof(int));
		memcpy(&pointer,(char*)node.data+sizeof(int),sizeof(int));
		}
		else
		{
			int stringLength;
			memcpy(&stringLength,(char*)node.data,sizeof(int));
//			cout<<"First key of int node: length: "<<stringLength;
			memcpy((char*)key,&stringLength,sizeof(int));
			memcpy((char*)key+sizeof(int),(char*)node.data+sizeof(int),stringLength);
			memcpy(&pointer,(char*)node.data+sizeof(int)+stringLength,sizeof(int));
		}
	return 0;
}
RC IndexManager::insertIntoLeaf(IXFileHandle &ixfileHandle, node &leafNode, const Attribute &attribute, const void *key, const RID &rid)
{
//	node leafNode;
//	readNode(ixfileHandle,page,leafNode);
	int numOfKeys=leafNode.numOfKeys;
	int keyLength=findKeyLength(attribute,key);
	int ridLength=sizeof(unsigned)*2;
	int offset=0;
	for(int i=0;i<numOfKeys;i++)
	{
		if(attribute.type==TypeInt)
		{
			int valueAtKeyi;
			int valueKey;
			memcpy(&valueAtKeyi,(char*)leafNode.data+offset,sizeof(int));
			memcpy(&valueKey,(char*)key,sizeof(int));
//			cout<<"Value at KEY I:"<<valueAtKeyi <<"  Key value: "<<valueKey<<endl;
			if(valueKey>=valueAtKeyi)
			{
				offset+=sizeof(int);
				offset+=ridLength;
				continue;
			}
			else
			{
//				cout <<"Key inserted :"<<valueKey <<" at page : "<< leafNode.pagenum <<endl;
				break;
			}
		}
		if(attribute.type==TypeReal)
		{
			float valueAtKeyi;
			float valueKey;
			memcpy(&valueAtKeyi,(char*)leafNode.data+offset,sizeof(float));
			memcpy(&valueKey,(char*)key,sizeof(float));
//			cout <<"Key inserted :"<<valueKey <<" at page : "<< leafNode.pagenum <<endl;
			if(valueKey>=valueAtKeyi)
			{
				offset+=sizeof(float);
				offset+=ridLength;
				continue;
			}
			else
			{
				break;
			}
		}
		if(attribute.type==TypeVarChar)
		{
			int lengthOfKeyi;
			memcpy(&lengthOfKeyi,(char*)leafNode.data+offset,sizeof(int));
			offset+=sizeof(int);
			string strKeyi = string((char*)leafNode.data+offset,lengthOfKeyi);
			int lengthOfKey;
			memcpy(&lengthOfKey,(char*)key,sizeof(int));
			string strKey = string((char*)key+sizeof(int),lengthOfKey);
			if(strKey.compare(strKeyi)>0 || strKey.compare(strKeyi)==0)
			{
				offset+=lengthOfKeyi;
				offset+=ridLength;
				continue;
			}
			else
			{
				offset-=sizeof(int);
				break;
			}
		}
	}
	 void* tempbuffer=(void*)malloc(PAGE_SIZE);
	memset(tempbuffer,0,PAGE_SIZE);
	 if(offset>(PAGE_SIZE-6*sizeof(int)-keyLength-ridLength))
			 return -1;
	 //Copy data from 0 to offset
	 memcpy((char*)tempbuffer,(char*)leafNode.data,offset);
	 //copy Key
	 memcpy((char*)tempbuffer+offset,(char*)key,keyLength);
	 //copy RID
	 memcpy((char*)tempbuffer+offset+keyLength,&rid.pageNum,sizeof(unsigned));
	 memcpy((char*)tempbuffer+offset+keyLength+sizeof(unsigned),&rid.slotNum,sizeof(unsigned));
	 //Copy remaining data
//	 cout <<"Next insert offset: "<<leafNode.nextInsertOffset<<endl;
//	 cout <<"Remaining data size: "<< (leafNode.nextInsertOffset-offset) <<endl;
	 memcpy((char*)tempbuffer+offset+keyLength+sizeof(unsigned)*2,(char*)leafNode.data+offset,(leafNode.nextInsertOffset)-offset);
	 //Update slot directory in old node page
	 //Freespace
	 leafNode.freespace=leafNode.freespace-keyLength-ridLength;
	 leafNode.numOfKeys+=1;
	 leafNode.nextInsertOffset+=keyLength+ridLength;
	 updateLeafSlotDirectory(leafNode);
	 //Write slot directory to tempbuffer
	 memcpy((char*)tempbuffer+PAGE_SIZE-sizeof(int)*6,(char*)leafNode.data+PAGE_SIZE-sizeof(int)*6,sizeof(int)*6);
	 //Write tempbuffer to disk at at page 'page'.
	 int writePage=ixfileHandle.fileHandle.writePage(leafNode.pagenum,tempbuffer);
	 free(tempbuffer);
	 free(leafNode.data);
//	 cout<< "------------------------------------------------------------------------------------" <<endl;
	 return writePage;
}

RC IndexManager::readNode(IXFileHandle &ixfileHandle,int pageNum,node &tempNode) const
{
	tempNode.data=(void*)malloc(PAGE_SIZE);
	memset(tempNode.data,0,PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(pageNum,tempNode.data);
	tempNode.pagenum=pageNum;
	int temp=0;
	//isLeaf
	memcpy(&temp,(char*)tempNode.data+PAGE_SIZE-sizeof(int),sizeof(int));
	tempNode.isLeaf=temp;
	//Freespace
	memcpy(&temp,(char*)tempNode.data+PAGE_SIZE-sizeof(int)*2,sizeof(int));
	tempNode.freespace=temp;
	//No of keys
	memcpy(&temp,(char*)tempNode.data+PAGE_SIZE-sizeof(int)*3,sizeof(int));
	tempNode.numOfKeys=temp;
	//Next insert offset
	memcpy(&temp,(char*)tempNode.data+PAGE_SIZE-sizeof(int)*4,sizeof(int));
	tempNode.nextInsertOffset=temp;

	//Intermediate node or Leaf node : Right,Left / Sibling
	if(tempNode.isLeaf!=1)
	{
		//Left pointer of node
		memcpy(&temp,(char*)tempNode.data+PAGE_SIZE-sizeof(int)*6,sizeof(int));
		tempNode.leftnode=temp;
		//Right pointer of node
		memcpy(&temp,(char*)tempNode.data+PAGE_SIZE-sizeof(int)*7,sizeof(int));
		tempNode.rightnode=temp;
		tempNode.siblingNode=-1;
	}
	else
	{
		//Sibling pointer to right
		memcpy(&temp,(char*)tempNode.data+PAGE_SIZE-sizeof(int)*6,sizeof(int));
		tempNode.siblingNode=temp;
		tempNode.rightnode=-1;
		tempNode.leftnode=-1;
	}
	return 0;
}
RC IndexManager::findLeafNode(IXFileHandle &ixfileHandle,const Attribute &attribute,int root,const void *key){
	node tempNode;
	readNode(ixfileHandle,root,tempNode);
	int tempPage=root;
	if(key==NULL)
	{
		while(tempNode.isLeaf!=1)
		{
			int leftPage;
			memcpy(&leftPage,(char*)tempNode.data+PAGE_SIZE-sizeof(int)*6,sizeof(int));
			free(tempNode.data);
			readNode(ixfileHandle,leftPage,tempNode);
		}
		int result=tempNode.pagenum;
		free(tempNode.data);
		return result;
	}
	while(tempNode.isLeaf!=1)
	{
		int i=0;
		int offset=0;
		while(i<tempNode.numOfKeys)
		{
			if(attribute.type==TypeInt)
			{
				int valueAtKeyi;
				int valueKey;
				memcpy(&valueAtKeyi,(char*)tempNode.data+offset,sizeof(int));
				memcpy(&valueKey,(char*)key,sizeof(int));
				if(valueKey>=valueAtKeyi)
				{
					i++;
					offset+=sizeof(int);
					offset+=sizeof(int);
					memcpy(&tempPage,(char*) tempNode.data + offset - sizeof(int),sizeof(int));
//					if (i == 0) {
//						memcpy(&tempPage,
//								(char*) tempNode.data + PAGE_SIZE
//										- sizeof(int) * 6, sizeof(int));
//					} else {
//						memcpy(&tempPage,
//								(char*) tempNode.data + offset - sizeof(int),
//								sizeof(int));
//					}
					continue;
				}
				else
				{
					if(i==0)
					{
						memcpy(&tempPage,(char*)tempNode.data+PAGE_SIZE-sizeof(int)*6,sizeof(int));
						break;
					}
					else
					{
						memcpy(&tempPage,(char*)tempNode.data+offset-sizeof(int),sizeof(int));
						break;
					}
				}
			}
			if(attribute.type==TypeReal){
				float valueAtKeyi;
				float valueKey;
				memcpy(&valueAtKeyi,(char*)tempNode.data+offset,sizeof(float));
				memcpy(&valueKey,(char*)key,sizeof(float));
				if(valueKey>=valueAtKeyi)
				{
					i++;
					offset+=sizeof(float);
					offset+=sizeof(int);
					memcpy(&tempPage,(char*) tempNode.data + offset - sizeof(float),sizeof(int));
//					if (i == 0) {
//					memcpy(&tempPage,(char*) tempNode.data + PAGE_SIZE- sizeof(int) * 6, sizeof(int));
//					} else {
//					memcpy(&tempPage,(char*) tempNode.data + offset - sizeof(int),sizeof(int));
//						}
					continue;
				}
				else
				{
					if(i==0)
					{
						memcpy(&tempPage,(char*)tempNode.data+PAGE_SIZE-sizeof(int)*6,sizeof(int));
						break;
					}
					else
					{
						memcpy(&tempPage,(char*)tempNode.data+offset-sizeof(int),sizeof(int));
						break;
					}
				}
			}
			if(attribute.type==TypeVarChar){
				int lengthOfKeyi;
				memcpy(&lengthOfKeyi,(char*)tempNode.data+offset,sizeof(int));
				offset+=sizeof(int);
				string strKeyi = string((char*)tempNode.data+offset,lengthOfKeyi);
				int lengthOfKey;
				memcpy(&lengthOfKey,(char*)key,sizeof(int));
				string strKey = string((char*)key+sizeof(int),lengthOfKey);
				if(strKey.compare(strKeyi)>0 || strKey.compare(strKeyi)==0)
				{
					i++;
					offset+=lengthOfKeyi;
					offset+=sizeof(int);
					memcpy(&tempPage,(char*)tempNode.data+offset-sizeof(int),sizeof(int));
//					if(i==0)
//					{	//Left child pointer
//						memcpy(&tempPage,(char*)tempNode.data+PAGE_SIZE-sizeof(int)*6,sizeof(int));
//					}
//					else
//					{
//						memcpy(&tempPage,(char*)tempNode.data+offset-sizeof(int)*2,sizeof(int));
//					}
					continue;
				}
				else
				{
					if(i==0)
					{	//Left child pointer
						memcpy(&tempPage,(char*)tempNode.data+PAGE_SIZE-sizeof(int)*6,sizeof(int));
						break;
					}
					else
					{
						memcpy(&tempPage,(char*)tempNode.data+offset-sizeof(int)*2,sizeof(int));
						break;
					}
				}
			}
		}
		free(tempNode.data);
//		cout<<"READING TEMPPAGE: "<<tempPage<<endl;
		readNode(ixfileHandle,tempPage,tempNode);
	}

	int result=tempPage;
	free(tempNode.data);
	return result;
}
RC IndexManager::getLeafNode(IXFileHandle &ixfileHandle,const Attribute &attribute,int root,const void *key,node &tempNode){

		//	node tempNode;
		readNode(ixfileHandle,root,tempNode);
		int tempPage=root;
		if(key==NULL)
		{
			while(tempNode.isLeaf!=1)
			{
				int leftPage;
				memcpy(&leftPage,(char*)tempNode.data+PAGE_SIZE-sizeof(int)*6,sizeof(int));
				free(tempNode.data);
				readNode(ixfileHandle,leftPage,tempNode);
			}
			int result=tempNode.pagenum;
			free(tempNode.data);
			return result;
		}
		while(tempNode.isLeaf!=1)
		{
			int i=0;
			int offset=0;
			while(i<tempNode.numOfKeys)
			{
				if(attribute.type==TypeInt)
				{
					int valueAtKeyi;
					int valueKey;
					memcpy(&valueAtKeyi,(char*)tempNode.data+offset,sizeof(int));
					memcpy(&valueKey,(char*)key,sizeof(int));
					if(valueKey>=valueAtKeyi)
					{
						i++;
						offset+=sizeof(int);
						offset+=sizeof(int);
						memcpy(&tempPage,(char*) tempNode.data + offset - sizeof(int),sizeof(int));
	//					if (i == 0) {
	//						memcpy(&tempPage,
	//								(char*) tempNode.data + PAGE_SIZE
	//										- sizeof(int) * 6, sizeof(int));
	//					} else {
	//						memcpy(&tempPage,
	//								(char*) tempNode.data + offset - sizeof(int),
	//								sizeof(int));
	//					}
						continue;
					}
					else
					{
						if(i==0)
						{
							memcpy(&tempPage,(char*)tempNode.data+PAGE_SIZE-sizeof(int)*6,sizeof(int));
							break;
						}
						else
						{
							memcpy(&tempPage,(char*)tempNode.data+offset-sizeof(int),sizeof(int));
							break;
						}
					}
				}
				if(attribute.type==TypeReal){
					float valueAtKeyi;
					float valueKey;
					memcpy(&valueAtKeyi,(char*)tempNode.data+offset,sizeof(float));
					memcpy(&valueKey,(char*)key,sizeof(float));
					if(valueKey>=valueAtKeyi)
					{
						i++;
						offset+=sizeof(float);
						offset+=sizeof(int);
						memcpy(&tempPage,(char*) tempNode.data + offset - sizeof(float),sizeof(int));
	//					if (i == 0) {
	//					memcpy(&tempPage,(char*) tempNode.data + PAGE_SIZE- sizeof(int) * 6, sizeof(int));
	//					} else {
	//					memcpy(&tempPage,(char*) tempNode.data + offset - sizeof(int),sizeof(int));
	//						}
						continue;
					}
					else
					{
						if(i==0)
						{
							memcpy(&tempPage,(char*)tempNode.data+PAGE_SIZE-sizeof(int)*6,sizeof(int));
							break;
						}
						else
						{
							memcpy(&tempPage,(char*)tempNode.data+offset-sizeof(int),sizeof(int));
							break;
						}
					}
				}
				if(attribute.type==TypeVarChar){
					int lengthOfKeyi;
					memcpy(&lengthOfKeyi,(char*)tempNode.data+offset,sizeof(int));
					offset+=sizeof(int);
					string strKeyi = string((char*)tempNode.data+offset,lengthOfKeyi);
					int lengthOfKey;
					memcpy(&lengthOfKey,(char*)key,sizeof(int));
					string strKey = string((char*)key+sizeof(int),lengthOfKey);
					if(strKey.compare(strKeyi)>0 || strKey.compare(strKeyi)==0)
					{
						i++;
						offset+=lengthOfKeyi;
						offset+=sizeof(int);
						memcpy(&tempPage,(char*)tempNode.data+offset-sizeof(int),sizeof(int));
	//					if(i==0)
	//					{	//Left child pointer
	//						memcpy(&tempPage,(char*)tempNode.data+PAGE_SIZE-sizeof(int)*6,sizeof(int));
	//					}
	//					else
	//					{
	//						memcpy(&tempPage,(char*)tempNode.data+offset-sizeof(int)*2,sizeof(int));
	//					}
						continue;
					}
					else
					{
						if(i==0)
						{	//Left child pointer
							memcpy(&tempPage,(char*)tempNode.data+PAGE_SIZE-sizeof(int)*6,sizeof(int));
							break;
						}
						else
						{
							memcpy(&tempPage,(char*)tempNode.data+offset-sizeof(int)*2,sizeof(int));
							break;
						}
					}
				}
			}
			free(tempNode.data);
	//		cout<<"READING TEMPPAGE: "<<tempPage<<endl;
			readNode(ixfileHandle,tempPage,tempNode);
		}

		int result=tempPage;
//		free(tempNode.data);
		return 0;

}

RC IndexManager::startNewTree(IXFileHandle &ixfileHandle,const Attribute &attribute,const void* key,const RID &rid)
{
	node root;
	root.data=(void*)malloc(PAGE_SIZE);
	memset(root.data,0,PAGE_SIZE);
	root.isLeaf=1;
	root.numOfKeys=1;
	root.nextInsertOffset=0;
	root.leftnode=-1;
	root.rightnode=-1;
	root.siblingNode=-1;
	root.pagenum=1;
	int keyLength=findKeyLength(attribute,key);
	//Insert Key
	memcpy((char*)root.data,(char*)key,keyLength);
	//Insert RID
	memcpy((char*)root.data+keyLength,&rid.pageNum,sizeof(unsigned));
	memcpy((char*)root.data+keyLength+sizeof(unsigned),&rid.slotNum,sizeof(unsigned));
	//Update freespace
	root.freespace=PAGE_SIZE-sizeof(int)*6-keyLength-sizeof(unsigned)*2;
	root.nextInsertOffset=keyLength+sizeof(unsigned)*2;
	//UPDATE SLOT DIRECTORY
	updateLeafSlotDirectory(root);
	//Trying with append.
//	cout <<"Append page counter: "<<ixfileHandle.fileHandle.appendPageCounter <<endl;
	int ap=ixfileHandle.fileHandle.appendPage(root.data);
//	cout <<"New appendpage counter: "<<ixfileHandle.fileHandle.appendPageCounter <<endl;
//	cout <<"Number of pages :"<<ixfileHandle.fileHandle.getNumberOfPages()<<endl;
	//int ap=ixfileHandle.fileHandle.writePage(root.pagenum,root.data);
	free(root.data);
	if(ap==0)
	return 0;
	return -1;
}
RC IndexManager::updateLeafSlotDirectory(node &leaf)
{
	memcpy((char*)leaf.data+PAGE_SIZE-sizeof(int),&leaf.isLeaf,sizeof(int));
	memcpy((char*)leaf.data+PAGE_SIZE-sizeof(int)*2,&leaf.freespace,sizeof(int));
	memcpy((char*)leaf.data+PAGE_SIZE-sizeof(int)*3,&leaf.numOfKeys,sizeof(int));
	memcpy((char*)leaf.data+PAGE_SIZE-sizeof(int)*4,&leaf.nextInsertOffset,sizeof(int));
	memcpy((char*)leaf.data+PAGE_SIZE-sizeof(int)*5,&leaf.pagenum,sizeof(int));
	memcpy((char*)leaf.data+PAGE_SIZE-sizeof(int)*6,&leaf.siblingNode,sizeof(int));
	return 0;
}
RC IndexManager::findKeyLength(const Attribute &attribute, const void *key){
	int keyLength=0;
	switch(attribute.type){
			case TypeInt:
				keyLength=sizeof(int);
				break;
			case TypeReal:
				keyLength=sizeof(float);
				break;
			case TypeVarChar:
				int stringLength=0;
				memcpy(&stringLength,(char*)key,sizeof(int));
				keyLength=sizeof(int)+stringLength;
				break;
			}
	return keyLength;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	int root=findRootPage(ixfileHandle);
	//	int leaf=findLeafNode(ixfileHandle,attribute,root,key); //Leaf page from which key and RID has to be deleted
		node leafNode;
		getLeafNode(ixfileHandle,attribute,root,key,leafNode);
	//	readNode(ixfileHandle,leaf,leafNode);
		int result=deleteKeyFromLeafNode(leafNode,attribute,key,rid);
		int writePage=ixfileHandle.fileHandle.writePage(leafNode.pagenum,leafNode.data);
		free(leafNode.data);
		return result;
}
RC IndexManager::deleteKeyFromLeafNode(node &node,const Attribute &attribute, const void *key, const RID &rid)
{
	unsigned ridSize=sizeof(rid.pageNum)+sizeof(rid.slotNum);
	int keyLength=findKeyLength(attribute,key);
	int offset=0;
	for(int i=0;i<node.numOfKeys;i++)
	{
		if(attribute.type==TypeInt)
		{
		int valueAtKeyi=0;
		int valueKey=0;
		memcpy(&valueAtKeyi,(char*)node.data+offset,sizeof(int));
		memcpy(&valueKey,(char*)key,sizeof(int));
		if(valueKey==valueAtKeyi)
			break;
		offset+=sizeof(int)+ridSize;
		}
		if(attribute.type==TypeReal)
		{
		float valueAtKeyi=0.0;
		float valueKey=0.0;
		memcpy(&valueAtKeyi,(char*)node.data+offset,sizeof(float));
		memcpy(&valueKey,(char*)key,sizeof(float));
		if(valueKey==valueAtKeyi)
			break;
		offset+=sizeof(float)+ridSize;
		}
		if(attribute.type==TypeVarChar)
		{
			int lengthOfKeyi=0;
			memcpy(&lengthOfKeyi,(char*)node.data+offset,sizeof(int));
			string strKeyi = string((char*)node.data+offset+sizeof(int),lengthOfKeyi);
			int lengthOfKey;
			memcpy(&lengthOfKey,(char*)key,sizeof(int));
			string strKey = string((char*)key+sizeof(int),lengthOfKey);
			if(strKey.compare(strKeyi)==0)
				break;
			offset+=sizeof(int)+lengthOfKeyi+ridSize;
		}
	}
	if(offset==node.nextInsertOffset)
		return -1; //Key not found
	void *tempbuffer=malloc(PAGE_SIZE);
	memset(tempbuffer,0,PAGE_SIZE);
	memcpy((char*)tempbuffer,(char*)node.data,offset);
	memcpy((char*)tempbuffer+offset,(char*)node.data+offset+keyLength+ridSize,(node.nextInsertOffset-(offset+keyLength+ridSize)));
	node.freespace+=keyLength+ridSize;
	node.numOfKeys-=1;
	node.nextInsertOffset-=(keyLength+ridSize);
	updateLeafSlotDirectory(node);
	//Copy updated slot directory from node to tempbuffer
	memcpy((char*)tempbuffer+PAGE_SIZE-sizeof(int)*6,(char*)node.data+PAGE_SIZE-sizeof(int)*6,sizeof(int)*6);
	//Replace node data with tempbuffer
	memcpy((char*)node.data,(char*)tempbuffer,PAGE_SIZE);
	free(tempbuffer);
	return 0;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
	string fileName = ixfileHandle.fileHandle.fileName;
			if (!std::ifstream(fileName))
				return -1;

			ix_ScanIterator.ixManager=this;
			ix_ScanIterator.ixHandle=ixfileHandle;
			ix_ScanIterator.attribute=attribute;
			ix_ScanIterator.lowKey=lowKey;
			ix_ScanIterator.highKey=highKey;
			ix_ScanIterator.lowKeyInclusive=lowKeyInclusive;
			ix_ScanIterator.highKeyInclusive=highKeyInclusive;
			ix_ScanIterator.matchingPage=-1;
			ix_ScanIterator.checkVal=NULL;
			ix_ScanIterator.nextOffset=0;
			ix_ScanIterator.originalnextInsertOffset=0;
			ix_ScanIterator.lowKeyStr =NULL;
			ix_ScanIterator.highKeyStr =NULL;
		//		ix_ScanIterator.slot=-1;
			return 0;

}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const
{
	int rootIndex=findRootPage(ixfileHandle);
	cout <<endl<< "{";
    printRec(rootIndex, ixfileHandle, attribute);
    cout <<endl<< "}"<<endl;
}

void IndexManager::printRec(int pageNum, IXFileHandle &ixfileHandle, const Attribute &attribute) const {

	node parentNode;
	readNode(ixfileHandle,pageNum,parentNode);
    if (parentNode.isLeaf == 1) { // print Key,RID in 1 node
    		cout <<endl<< "{\"keys\": [";
        int offset = 0;
        while (true) {
        	int keyEndOffset = printKeyAndSetOffsetToKeyEnd(offset,attribute,parentNode.data);
        	int nextKeyOffset = printRIDAndUpdateOffset(keyEndOffset, attribute,parentNode);
            if (nextKeyOffset == parentNode.nextInsertOffset)
                break;
            offset=nextKeyOffset;
            cout << ", ";
        }
        cout << "]}";

    } else { // prints Key in 1 node
    		cout << endl<<"{\"keys\": [";
        int offsetP = 0;
        while (true) {
        	 cout << "\"";
            int keyEndOffset =	printKeyAndSetOffsetToKeyEnd(offsetP,attribute,parentNode.data);
//            cout << "\"";
            cout << "\""<<endl;
            int nextKeyOffset = printRIDAndUpdateOffset(keyEndOffset, attribute,parentNode);
               if (nextKeyOffset == parentNode.nextInsertOffset)
                   break;
               offsetP=nextKeyOffset;
            cout << ", ";
        }
        cout << "]," << endl;
        if(parentNode.leftnode != -1 && parentNode.rightnode != -1){
    		cout << " \"children\": [";
    }
        int right, left;
        int offset=0;
                for(int i=0;i<parentNode.numOfKeys;i++) {
                		if(offset == (parentNode.nextInsertOffset-sizeof(int)))
                			break;
                		if(i==0){
                			memcpy(&left, &parentNode.leftnode, sizeof(int));
                			printRec(left, ixfileHandle, attribute);
                		}
                		else {
                			if (attribute.type == TypeVarChar){
                				int size, leftPgPtr;
                				memcpy(&size, (char*)parentNode.data+offset, sizeof(int));
                				memcpy(&leftPgPtr, (char*)parentNode.data+offset + size + sizeof(int),sizeof(int));
                				offset = offset + size + sizeof(int)+sizeof(int);
                				printRec(leftPgPtr, ixfileHandle, attribute);
                				cout << "," << endl;
                			}
                			else {
                				int leftPgPtr;
                				memcpy(&leftPgPtr, (char*)parentNode.data+offset+sizeof(int),sizeof(int));
                				offset = offset + sizeof(int);
                				printRec(leftPgPtr, ixfileHandle, attribute);
                				cout << "," << endl;
                			}
                }
                }
                memcpy(&right, &parentNode.rightnode, sizeof(int));
                printRec(right, ixfileHandle, attribute);
                cout << endl << "]}";
    }
}

int IndexManager::printRIDAndUpdateOffset(int offsetToPrint, const Attribute &attribute, node &tempNode) const {

    cout << '\"';
    if(tempNode.isLeaf ==1){
    	unsigned rid_page,rid_slot;
    	memcpy(&rid_page,(char*)tempNode.data+offsetToPrint,sizeof(unsigned));
    	memcpy(&rid_slot,(char*)tempNode.data+offsetToPrint+(sizeof(unsigned)),sizeof(unsigned));
    cout << ":[(" << rid_page << "," << rid_slot << ")]";
    cout << '\"';
    offsetToPrint =offsetToPrint+(sizeof(unsigned)*2);
    return offsetToPrint;
    }
    else {
    	offsetToPrint =offsetToPrint+sizeof(int);
    	return offsetToPrint;
    }
}

int IndexManager::printKeyAndSetOffsetToKeyEnd(int offsetToPrint, const Attribute& attribute, const void* data) const {

    switch (attribute.type) {
        case TypeVarChar:
        {
            int size;
            memcpy(&size, (char*)data+offsetToPrint, sizeof(int));
            string str = string((char*)data+offsetToPrint+sizeof(int),size);
            cout << str;
            offsetToPrint=offsetToPrint+size+sizeof(int);
            return offsetToPrint;
            break;
        }
        case TypeInt:
        {
            int number;
            memcpy(&number, (char*)data+offsetToPrint, sizeof(int));
            cout << number;
            offsetToPrint=offsetToPrint+sizeof(int);
            return offsetToPrint;
            break;
        }
        case TypeReal:
        {
            float number;
            memcpy(&number, (char*)data+offsetToPrint, sizeof(float));
            cout << number;
            offsetToPrint=offsetToPrint+sizeof(float);
            return offsetToPrint;
            break;
        }
        default:
        		return -1;
    }

}


IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{int matchFound = 0;
while(true){
node leafNode;
//cout<<"Inside get next entry!"<<endl;
checkVal=(void*)malloc(PAGE_SIZE);
if(matchingPage==-1)
{
	int rootIndex=ixManager->findRootPage(ixHandle);
	if(rootIndex==-1)
		return -1;
//	cout<<"rootIndex    "<<rootIndex<<endl;
	if(attribute.type==TypeVarChar)
	{
		if(lowKeyStr == NULL && highKeyStr == NULL){
		int len2,len3;
		memcpy(&len2,(char*)lowKey,sizeof(int));
		lowKeyStr=(void*)malloc(len2+1);
		memset(lowKeyStr,'\0',len2+1);
		memcpy((char*)lowKeyStr,(char*)lowKey+sizeof(int),len2);
		memcpy(&len3,(char*)highKey,sizeof(int));
		highKeyStr=(void*)malloc(len3+1);
		memset(highKeyStr,'\0',len3+1);
		memcpy((char*)highKeyStr,(char*)highKey+sizeof(int),len3);
		}
	}
	int leaf=ixManager->findLeafNode(ixHandle,attribute,rootIndex,lowKey);
	matchingPage=leaf;
}
ixManager->readNode(ixHandle,matchingPage,leafNode);
int left_i;
int offset=nextOffset;
int dataIndex=0;
if(originalnextInsertOffset ==0){
originalnextInsertOffset=leafNode.nextInsertOffset;
}
if (leafNode.nextInsertOffset < originalnextInsertOffset){      //if delete happens on the fly
	offset=offset-(originalnextInsertOffset-leafNode.nextInsertOffset);
	originalnextInsertOffset=leafNode.nextInsertOffset;
}
//cout<<"OFFSET   "<<offset<<endl;
//cout<<"matchingPage   "<<matchingPage<<endl;
//cout<<"leafNode.nextInsertOffset   "<<leafNode.nextInsertOffset<<endl;
for(left_i=0;left_i<leafNode.numOfKeys;left_i++)
{
	if(attribute.type==TypeInt)
	{
		int nodeVal,lowVal,highVal;
		memcpy(&nodeVal,(char*)leafNode.data+offset,sizeof(int));
		offset = offset+sizeof(int)+(sizeof(unsigned)*2);
		if(lowKey!=NULL)
			memcpy(&lowVal,(char*)lowKey,sizeof(int));
		if(highKey!=NULL)
			memcpy(&highVal,(char*)highKey,sizeof(int));
		if(highKey!=NULL) { // Neg Case
			if ((nodeVal > highVal) || ((!highKeyInclusive) && (nodeVal == highVal))) {
				free(leafNode.data);
				return -1;
			}}
		if(lowKey!=NULL) {
			if(nodeVal < lowVal || (!lowKeyInclusive && (nodeVal == lowVal)))
				continue;
			else if((lowKeyInclusive && (nodeVal == lowVal)) || nodeVal > lowVal) { //Pos Case
						memset(checkVal,0,PAGE_SIZE);
						memcpy((char*)checkVal,&nodeVal,sizeof(int));
						dataIndex = offset;
						matchFound = 1;
						break;
			}
		}
		else // lowKey ==Null
		{
				memset(checkVal,0,PAGE_SIZE);
				memcpy((char*)checkVal,&nodeVal,sizeof(int));
				dataIndex = offset;
				matchFound = 1;
				break;
		}
} //endif TypeInt
	else if (attribute.type==TypeReal){
		float nodeVal,lowVal,highVal;
					memcpy(&nodeVal,(char*)leafNode.data+offset,sizeof(float));
					offset = offset+sizeof(float)+(sizeof(unsigned)*2);
					if(lowKey!=NULL)
						memcpy(&lowVal,(char*)lowKey,sizeof(float));
					if(highKey!=NULL)
						memcpy(&highVal,(char*)highKey,sizeof(float));
					if(highKey!=NULL) { // Neg Case
						if ((nodeVal > highVal) || ((!highKeyInclusive) && (nodeVal == highVal))) {
							free(leafNode.data);
							return -1;
						}}
					if(lowKey!=NULL) {
						if(nodeVal < lowVal || (!lowKeyInclusive && (nodeVal == lowVal)))
							continue;
						else if((lowKeyInclusive && (nodeVal == lowVal)) || nodeVal > lowVal) { //Pos Case
									memset(checkVal,0.0,PAGE_SIZE);
									memcpy((char*)checkVal,&nodeVal,sizeof(float));
									dataIndex = offset;
									matchFound = 1;
									break;
						}
					}
					else // lowKey ==Null
					{
							memset(checkVal,0.0,PAGE_SIZE);
							memcpy((char*)checkVal,&nodeVal,sizeof(float));
							dataIndex = offset;
							matchFound = 1;
							break;
					}
	}// endif TypeReal
	else {
		int len1,len2,len3;
		memcpy(&len1,(char*)leafNode.data+offset,sizeof(int));
		string nodeVal = string((char*)leafNode.data+offset+sizeof(int),len1);
//		cout<<"len1    "<<len1<<endl;
//		cout<<"nodeVal    "<<nodeVal<<endl;
		offset = offset+sizeof(int)+len1+(sizeof(unsigned)*2);
		int r1,r2;
		if(lowKey!=NULL)
		{
//			memcpy(&len2,(char*)lowKey,sizeof(int));
//		//	string lowVal = string((char*)lowKey+sizeof(int),len2);
//			if(lowKeyStr == NULL){
//			memset(lowKeyStr,'\0',len2);
//			memcpy((char*)lowKeyStr,(char*)lowKey+sizeof(int),len2);
//			}
			string lowStrKey = string((char*)lowKeyStr);
//		cout<<"lowStrKey    "<<lowStrKey<<endl;
			r1=nodeVal.compare(lowStrKey);
		}
		if(highKey!=NULL)
		{
//			memcpy(&len3,(char*)highKey,sizeof(int));
//			//	string highVal = string((char*)highKey+sizeof(int),len3);
//						if(highKeyStr == NULL){
//						memset(highKeyStr,'\0',len2);
//						memcpy((char*)highKeyStr,(char*)highKey+sizeof(int),len3);
//						}
			string highStrKey = string((char*)highKeyStr);
			r2=nodeVal.compare(highStrKey);
		}
		if(highKey!=NULL) { // Neg Case
						if ((r2 > 0) || ((!highKeyInclusive) && (r2 == 0))) {
							free(leafNode.data);
							return -1;
						}}
		if(lowKey!=NULL) {
			if(r1 < 0 || (!lowKeyInclusive && (r1 == 0)))
				continue;
			else if((lowKeyInclusive && (r1 == 0)) || r1 > 0) { //Pos Case
						memset(checkVal,'\0',PAGE_SIZE);
						memcpy((char*)checkVal,&len1,sizeof(int));
						memcpy((char*)checkVal+sizeof(int),&nodeVal,len1);
						dataIndex = offset;
						matchFound = 1;
						break;

			}
		}
		else // lowKey ==Null
		{ //Pos Case
				memset(checkVal,'\0',PAGE_SIZE);
				memcpy((char*)checkVal,&len1,sizeof(int));
				memcpy((char*)checkVal+sizeof(int),&nodeVal,len1);
				dataIndex = offset;
				matchFound = 1;
				break;
		}
	} //endif TypeVarChar
}
if(left_i == leafNode.numOfKeys || dataIndex >leafNode.nextInsertOffset){  //----check if it should go to sibling page or break while loop----//
	if (leafNode.siblingNode == -1){
		free(lowKeyStr);
		free(highKeyStr);
		free(checkVal);
		free(leafNode.data);
		return -1; //match not found
	}
	else {
		matchingPage=leafNode.siblingNode;
		nextOffset=0;
		free(checkVal);
		free(leafNode.data);
		continue;  //Recursive--- // return getNextEntry(rid, key); ---// to search in sibling page
	}
}
if(matchFound == 1){
matchingPage=leafNode.pagenum;
nextOffset=dataIndex;
unsigned rid_page,rid_slot;
memcpy(&rid_page,(char*)leafNode.data+dataIndex-(sizeof(unsigned)*2),sizeof(unsigned));
memcpy(&rid_slot,(char*)leafNode.data+dataIndex-(sizeof(unsigned)),sizeof(unsigned));
rid.pageNum=rid_page;
//cout<<"rid.pageNum "<<rid.pageNum<<endl;
rid.slotNum=rid_slot;
//cout<<"rid.slotNum "<<rid.slotNum<<endl;
if(attribute.type==TypeInt)
{
	int ck;
	memcpy(&ck,(char*)checkVal,sizeof(int));
//	cout<<"intKeyck  "<<ck<<endl;
	memcpy((char*)key,(char*)checkVal,sizeof(int));
//	cout<<"intKey  "<<*(char*)checkVal<<endl;
}
else if(attribute.type==TypeReal)
{
	float fk;
	memcpy(&fk,(char*)checkVal,sizeof(int));
	memcpy((char*)key,(char*)checkVal,sizeof(float));
//	cout<<"floatKey  "<<fk<<endl;
}
else
{
	int len;
	memcpy(&len,(char*)checkVal,sizeof(int));
	memcpy((char*)key,(char*)checkVal,sizeof(int)+len);
//	cout<<"varCharKey  "<<*(char*)key<<endl;
}
free(checkVal);
free(leafNode.data);
return 0;
}
else{ //match not found
	free(lowKeyStr);
	free(highKeyStr);
	free(checkVal);
	free(leafNode.data);
	return -1;
}
}

}

RC IX_ScanIterator::close()
{
    return 0;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
    pfm=PagedFileManager::instance();

}

IXFileHandle::~IXFileHandle()
{

}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	ixReadPageCounter=this->fileHandle.readPageCounter;
	ixWritePageCounter=this->fileHandle.writePageCounter;
	ixAppendPageCounter=this->fileHandle.appendPageCounter;
	readPageCount=ixReadPageCounter;
	writePageCount=ixWritePageCounter;
	appendPageCount=ixAppendPageCounter;
    return 0;
}
RC IndexManager::initialDirectory(IXFileHandle &ixfileHandle)
{
	void *data = malloc(PAGE_SIZE);
	memset(data,0,PAGE_SIZE);
	int index=-1;
	memcpy((char*)data,&index,sizeof(int));
	//ixfileHandle.fileHandle.writePage(0,data);
	ixfileHandle.fileHandle.appendPage(data);
	free(data);
	return 0;
}


int IndexManager::cut(int length)
{
	if(length%2==0)
		return length/2;
	else
		return length/2+1;
}
