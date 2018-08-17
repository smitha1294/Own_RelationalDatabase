#include "qe.h"
#include "string.h"
#include <math.h>

Filter::Filter(Iterator* input, const Condition &condition) {
	this->iterator=input;
	this->condition=condition;
}

RC Filter::getNextTuple(void *data)
{
	while(iterator->getNextTuple(data) != EOF) {
//		cout <<"Inside get next tuple"<<endl;
			if(compareCondition(data) == 0) {
				return 0;
			}
		}
	return QE_EOF;
}
void Filter::getAttributes(vector<Attribute> &attrs) const
{
	iterator->getAttributes(attrs);
}
RC Filter::compareCondition(void *data)
{
int comparisonResult=-1;
vector <Attribute> attrs;
iterator->getAttributes(attrs);
bool isNull;
void *leftAttribute=malloc(PAGE_SIZE);
memset(leftAttribute,0,PAGE_SIZE);
rbfm->readAttributeFromRecord(data,attrs,condition.lhsAttr,leftAttribute); //Receives null bit indicator (1 byte) and attribute value in left attribute
void *leftAttrWithoutNullBits=malloc(PAGE_SIZE);
memset(leftAttrWithoutNullBits,0,PAGE_SIZE);
memcpy((char*)leftAttrWithoutNullBits,(char*)leftAttribute+1,PAGE_SIZE-1);
	for (int i = 0; i < attrs.size(); i++) {
		if (attrs.at(i).name == this->condition.lhsAttr) {
			if (this->condition.bRhsIsAttr) {
				//RHS attribute comparison
				for (int j = 0; j < attrs.size(); j++) {
					if (attrs.at(j).name == this->condition.rhsAttr) {
						void *rightAttribute = malloc(PAGE_SIZE);
						memset(rightAttribute, 0, PAGE_SIZE);
						rbfm->readAttributeFromRecord(data, attrs,
								condition.rhsAttr, rightAttribute);
						void *rightAttWONullBits = malloc(PAGE_SIZE);
						memset(rightAttWONullBits, 0, PAGE_SIZE);
						memcpy((char*)rightAttWONullBits,(char*) rightAttribute + 1,
						PAGE_SIZE - 1);
						comparisonResult = rbfm->compareAttributes(
								leftAttrWithoutNullBits, rightAttWONullBits,
								attrs.at(i), condition.op);
						free(rightAttribute);
						free(rightAttWONullBits);
					}
				}
			} else { //RHS Value comparison
				comparisonResult = rbfm->compareAttributes(
						leftAttrWithoutNullBits, condition.rhsValue.data,
						attrs.at(i), condition.op);
			}
		}
	}
free(leftAttribute);
free(leftAttrWithoutNullBits);
//cout <<"Comparison result: "<< comparisonResult<<endl;
return comparisonResult;
}
Project::Project(Iterator *input,const vector<string> &attrNames)
{

	this->iterator=input;
	this->attrNames=new vector<Attribute>();
	vector<Attribute> *temp=new vector<Attribute>();
	iterator->getAttributes(*temp);
	int j=0;
	for(int i=0;i<(int)temp->size();i++)
	{
		if(temp->at(i).name==attrNames.at(j))
		{
			this->attrNames->push_back(temp->at(i));
			j++;
		}
	}
	temp->clear();
}
void Project::getAttributes(vector<Attribute> &attrs) const
{
	attrs=*(this->attrNames);
}
RC Project::getNextTuple(void *data)
{
		void *tmp=(void *)malloc(PAGE_SIZE);
		RC rc=iterator->getNextTuple(tmp);
		if(rc!=0)
		{
			free(tmp);
			return rc;
		}
		else
		{
			void *dataWNB=malloc(PAGE_SIZE); //DATA without null bits
			memset(dataWNB,0,PAGE_SIZE);
			int offset=0;
			int off=0;
			int j=0;
			vector<Attribute> *a=new vector<Attribute>();
			iterator->getAttributes(*a);
			int vectorSize=a->size();
			int nullBitsIndicatorSize=ceil(vectorSize/8.0); //Size of NBI of data fetched from heap file

			int nbiSizeDesired=ceil(attrNames->size()/8.0);
			bool* finalBitsArray=(bool*)malloc(nbiSizeDesired);
			int k=0;
			//Add null bits to projected attributes
			offset+=nullBitsIndicatorSize;
			for(int i=0;i<(int)a->size();i++)
			{
				if(a->at(i).name==attrNames->at(j).name)
				{
					int len=0;
					if(a->at(i).type==TypeInt)
					{
						len=sizeof(int);
					}
					else if(a->at(i).type==TypeReal)
					{
						len=sizeof(float);
					}
					else if(a->at(i).type==TypeVarChar)
					{
						memcpy(&len,(char*)tmp+offset,sizeof(int));
						len+=sizeof(int);
					}
					memcpy((char*)dataWNB+off,(char*)tmp+offset,len);
					off+=len;
					offset+=len;
					j++;
					finalBitsArray[k]=0;
					k++;
				}

				else
							{
								int len=0;
								if(a->at(i).type==TypeInt)
								{
									len=sizeof(int);
								}
								else if(a->at(i).type==TypeReal)
								{
									len=sizeof(float);
								}
								else if(a->at(i).type==TypeVarChar)
								{
									memcpy(&len,(char*)tmp+offset,sizeof(int));
									len+=sizeof(int);
								}
								offset+=len;
							}
						}

			char* finalByteArray = (char *) malloc(nbiSizeDesired);
			memset(finalByteArray,0,nbiSizeDesired);
			//Bits array to byte data
			int i = 0;
			for (int j = 0; j < nbiSizeDesired * 8; j++)
			{
				if (j >= attrNames->size())
					break;
				if (finalBitsArray[j])
					finalByteArray[i] |= 1 << j;
					if(j%8==0)
					i=i+1;
			}
			memcpy((char*)data,(char*)finalByteArray,nbiSizeDesired);
			memcpy((char*)data+nbiSizeDesired,(char*)dataWNB,off);
			free(finalBitsArray);
			free(finalByteArray);
			free(dataWNB);
			a->clear();
			free(tmp);
			return rc;
					}

}
INLJoin::INLJoin(Iterator *leftIn,           // Iterator of input R
       IndexScan *rightIn,          // IndexScan Iterator of input S
       const Condition &condition   // Join condition
){
	this->itrLeft=leftIn;
	this->itrRight=rightIn;
	this->condition=condition;
	this->flag=true;
	itrLeft->getAttributes(leftAttrs);
	itrRight->getAttributes(rightAttrs);

	leftBuf=(void*)malloc(PAGE_SIZE);
	rightBuf=(void*)malloc(PAGE_SIZE);
	leftKey = (void*)malloc(PAGE_SIZE);

	for(int i=0;i<(int)leftAttrs.size();i++) {
		attrs.push_back(leftAttrs.at(i));}
	for(int i=0;i<(int)rightAttrs.size();i++) {
		attrs.push_back(rightAttrs.at(i));}
}

INLJoin::~INLJoin()
{
	free(leftBuf);
	free(rightBuf);
	free(leftKey);
}
void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
	attrs=this->attrs;
}

RC INLJoin::getNextTuple(void *data) {

	JoinTuples join;
	memset(rightBuf, 0, PAGE_SIZE);
	if((itrRight->getNextTuple(rightBuf) != QE_EOF) && (flag == false))
	{
		join.joinTuples(leftAttrs, leftBuf, rightAttrs, rightBuf, data);
		return 0;
	}
	memset(leftBuf, 0, PAGE_SIZE);
	do{
		if(itrLeft->getNextTuple(leftBuf) != QE_EOF)
		{
			void *leftAttrData = malloc(PAGE_SIZE);
			memset(leftAttrData, 0, PAGE_SIZE);
			memset(leftKey, 0, PAGE_SIZE);
			rbfm->readAttributeFromRecord(leftBuf,leftAttrs,condition.lhsAttr,leftAttrData);
			memcpy((char*)leftKey,(char*)leftAttrData+1,PAGE_SIZE-1);
			itrRight->setIterator(leftKey, leftKey, true, true);
			free(leftAttrData);
		}
		else
			return -1;
	}
	while(itrRight->getNextTuple(rightBuf) == QE_EOF);
	join.joinTuples(leftAttrs, leftBuf, rightAttrs, rightBuf, data);
	flag = false;
	return 0;
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op) {
		this->itr=input;
		this->aggAttr= aggAttr;
		this->op=op;
		endFlag=false;
		itr->getAttributes(allAttr);
		findAttPosition();
		if(aggAttr.type == TypeInt)
			isInt=true;
		else
			isInt=false;
		record = malloc(PAGE_SIZE);
        key = malloc(sizeof(int)+1);
        finalVal = malloc(sizeof(int)+1);
        floatSumVal = malloc(sizeof(int)+1);
		bitArrayNew = (unsigned char *) malloc(1);
		memset(bitArrayNew,0,1);
		bitArrayNew[0] = 0; //00000000
}

Aggregate::~Aggregate()
{
    free(record);
    free(key);
    free(finalVal);
    free(bitArrayNew);
    free(floatSumVal);
}

RC Aggregate::findAttPosition()
{
    for(unsigned i = 0; i < allAttr.size(); i++) {
        if(allAttr.at(i).name == aggAttr.name) {
        		attPos = i;
            return 0;
        }
    }
    return -1;
}

void Aggregate::getAttributes(vector<Attribute> &attrs) const
{
    attrs.clear();
	Attribute input = allAttr.at(attPos);

	    	input.type = TypeReal;

	    attrs.push_back(input);
}

RC Aggregate::getNextTuple(void *data)
{
	if(endFlag){return -1;}
	        float cnt = 0.0;
	        switch (op) {
	            case MAX:
	            case MIN:
	                if(itr->getNextTuple(record) != QE_EOF)
	                {
	                 	rbfm->readAttributeFromRecord(record,allAttr,aggAttr.name,key);
	                 	if(aggAttr.type == TypeInt){
	                 	float floatVal = 0.0;int intVal = 0;
	                 	memcpy(&intVal, (char*) key+1, sizeof(int));
	                 	floatVal = (float)intVal;
	                 	memcpy((char*) finalVal, &bitArrayNew, 1);
	                    memcpy((char*) finalVal+1, &floatVal, sizeof(float));
	                 	}
	                 	else {
	                 		memcpy((char*)finalVal, (char*)key, sizeof(float)+1);
	                 	}
	                }
	                while(itr->getNextTuple(record) != QE_EOF)
	                {
	                		rbfm->readAttributeFromRecord(record,allAttr,aggAttr.name,key);
		                 	if(aggAttr.type == TypeInt){
		                 	float floatVal = 0.0;int intVal = 0;
		                 	memcpy(&intVal, (char*) key+1, sizeof(int));
		                 	floatVal = (float)intVal;
		                 	memcpy((char*) key, &bitArrayNew, 1);
		                    memcpy((char*) key+1, &floatVal, sizeof(float));
		                 	}
	                    if(swap(key, finalVal))
	                    {
	                        memcpy((char*) finalVal, (char*) key, sizeof(float)+1);
	                    }
	                }
	                break;

	           case COUNT:
	                while(itr->getNextTuple(record) != QE_EOF)
	                {
	                    cnt++;
	                }
	            		memcpy((char*) finalVal, &bitArrayNew, 1);
	                memcpy((char*) finalVal+1, &cnt, sizeof(float));
	                break;

	            case SUM:
	            case AVG:
	                if(itr->getNextTuple(record) != QE_EOF)
	                {
	                		rbfm->readAttributeFromRecord(record,allAttr,aggAttr.name,key);
	                    memcpy((char*) finalVal, (char*) key, sizeof(int)+1);
	                    if(op == AVG)
	                    {
	                        cnt++;
	                    }
	                }
	                while(itr->getNextTuple(record) != QE_EOF)
	                {
	                		rbfm->readAttributeFromRecord(record,allAttr,aggAttr.name,key);
	                    add(key, finalVal);
	                    if(op == AVG)
	                    {
	                        cnt++;
	                    }
	                }
	                if(op == AVG)
	                {
	                		avg(cnt, finalVal);
	                }

	                break;

	            default:
	                break;
	        }

	        if((isInt) && op == SUM){
	        		memcpy(data, floatSumVal, sizeof(float)+1);
		        endFlag = true;
		        return 0;
	        }
	        memcpy(data, finalVal, sizeof(float)+1);
	        endFlag = true;
	        return 0;
}

bool Aggregate::swap(void *key, void *finalval)
{
    if(!isInt)
    {
        float keyData = 0.0, finalValData = 0.0;
        memcpy(&keyData, (char*)key+1, sizeof(float));
        memcpy(&finalValData, (char*)finalval+1, sizeof(float));
        if((op == MIN && keyData < finalValData) || (op == MAX && keyData > finalValData))
        {
            return true;
        }
        else
        {
            return false;
        }
    }
    else
    {
        int keyData = 0, finalValData = 0;
        memcpy(&keyData, (char*)key+1, sizeof(int));
        memcpy(&finalValData, (char*)finalval+1, sizeof(int));
        if((op == MIN && keyData < finalValData) || (op == MAX && keyData > finalValData))
        {
            return true;
        }
        else
        {
            return false;
        }
    }
    return false;
}

void Aggregate::add(void *key, void *finalval)
{
    if(!isInt)
    {
        float keyData = 0.0, finalValData = 0.0;
        memcpy(&keyData, (char*)key +1, sizeof(float));
        memcpy(&finalValData, (char*)finalval +1, sizeof(float));
        finalValData = finalValData+keyData;
        memcpy((char*) finalval, &bitArrayNew, 1);
        memcpy((char*) finalval+1, &finalValData, sizeof(float));

    }
    else
    {
        int keyData = 0, finalValData = 0;float finalAddVal = 0.0;
        memcpy(&keyData, (char*)key +1, sizeof(int));
        memcpy(&finalValData, (char*)finalval +1, sizeof(int));
        finalValData = finalValData+keyData;
        memcpy((char*) finalval, &bitArrayNew, 1);
        memcpy((char*) finalval+1, &finalValData, sizeof(int));
        finalAddVal = (float)finalValData;
        memcpy((char*) floatSumVal, &bitArrayNew, 1);
        memcpy((char*) floatSumVal+1, &finalAddVal, sizeof(float));
    }
}

void Aggregate::avg(int count, void *finalval)
{
    float avg = 0.0;

    if(!isInt)
    {
         memcpy(&avg, (char*)finalval+1, sizeof(float));
    }
    else
    {
        int finalValData = 0;
        memcpy(&finalValData, (char*)finalval+1, sizeof(int));
        avg = (float)finalValData;
    }
    avg = avg / count;
    memcpy((char*) finalval, &bitArrayNew, 1);
    memcpy((char*) finalval+1, &avg, sizeof(float));
}

BNLJoin::BNLJoin(Iterator *leftIn,            // Iterator of input R
               TableScan *rightIn,           // TableScan Iterator of input S
               const Condition &condition,   // Join condition
               const unsigned numPages       // # of pages that can be loaded into memory,
			                                 //   i.e., memory block size (decided by the optimizer)
     ){
	this->leftIn=leftIn;
	this->rightIn=rightIn;
	this->condition=condition;
	this->numPages=numPages;
}
void BNLJoin::getAttributes(vector<Attribute> &attrs) const
{
	attrs=this->attrs;
}
RC BNLJoin::getNextTuple(void *data){

		JoinTuples join;
		vector<Attribute> attrsLeft;
		leftIn->getAttributes(attrsLeft);
		vector<Attribute> attrsRight;
		rightIn->getAttributes(attrsRight);
		if(condition.bRhsIsAttr)
		{//RHS attribute being compared
		for(int i=0;i<attrs.size();i++)
		{
			if(attrsLeft[i].name==condition.rhsAttr)
				attrtype=attrs[i].type;
		}
		}
		else
		{//RHS Value being compared
			attrtype=condition.rhsValue.type;
		}
		void *leftData=(void*)malloc(PAGE_SIZE);
		memset(leftData,0,PAGE_SIZE);
		void* rightData=malloc(PAGE_SIZE);
		memset(rightData,0,PAGE_SIZE);
		int recordSize=0;
		int joinDone=0;
		void* remainingTuple=malloc(PAGE_SIZE);
		int length_RemainingTuple=0;
		memset(remainingTuple,0,PAGE_SIZE);
		void *key=malloc(PAGE_SIZE);
		memset(key,0,PAGE_SIZE);
		while(iterator)
		{
		while(leftIn->getNextTuple(leftData)!=-1)
		{
		//In case any tuple remaining to be added in left hash map, add it first
		length_RemainingTuple=rbfm->findRecordLength(attrsLeft,remainingTuple);
		if(length_RemainingTuple>0)
		{
			//Insert in hash map
			memset(key,0,PAGE_SIZE);
			rbfm->readAttributeFromRecord(remainingTuple,attrsLeft,condition.lhsAttr,key);
			pushKeyToHashMap(key,attrtype,remainingTuple);
			memset(remainingTuple,0,PAGE_SIZE);
			summationRecordSize+=length_RemainingTuple;
		}
		joinDone=0;
		recordSize=rbfm->findRecordLength(attrsLeft,leftData);
		summationRecordSize+=recordSize;
//		cout<<"Summation records size:"<<summationRecordSize<<endl;
		if(summationRecordSize<numPages*PAGE_SIZE)
		{
		memset(key,0,PAGE_SIZE);
		rbfm->readAttributeFromRecord(leftData,attrsLeft,condition.lhsAttr,key);
//		cout<<"Putting to hash map: "<<endl;
//		rbfm->printRecord(attrsLeft,leftData);
		pushKeyToHashMap(key,attrtype,leftData);
		}
		else
		{
			//As soon as summation becomes equal to numPages*PAGE_SIZE, read all tuples from right table , compare and return join data.
			//summationRecordSize set to 0;
//			cout<<"Reached else!"<<endl;
			joinDone=1;
			summationRecordSize=0;
			length_RemainingTuple=rbfm->findRecordLength(attrsLeft,leftData);
			if(length_RemainingTuple>0)
			{
				memcpy((char*)remainingTuple,(char*)leftData,length_RemainingTuple);
			}
			while(rightIn->getNextTuple(rightData)!=-1)
			{
				int matchFound=-1;
				matchFound=compareInnerTable(attrtype,attrsLeft,rightData,attrsRight);
					//RHS attribute to be compared
				if(matchFound==0)
				{
					//Join the two tuples
					void *leftDataFromMap=malloc(PAGE_SIZE);
					memset(leftDataFromMap,0,PAGE_SIZE);
					void* keyToSelect=malloc(PAGE_SIZE);
					rbfm->readAttributeFromRecord(rightData,attrsRight,condition.rhsAttr,keyToSelect);
					readKeyValueFromHashMap(keyToSelect,leftDataFromMap);
//					cout<<"Record read from map: ";
//					rbfm->printRecord(attrsLeft,leftDataFromMap);
					join.joinTuples(attrsLeft,leftDataFromMap,attrsRight,rightData,data);
					memset(rightData,0,PAGE_SIZE);
					return 0;
				}

			}


		}
		}
		if(joinDone==0)
		{
			joinDone=1;
//			cout<<"Tuples left in hash map and not compared!"<<endl;
			while (rightIn->getNextTuple(rightData) != -1) {
//				void *leftDataFromMap = malloc(PAGE_SIZE);
				int matchFound = -1;
				matchFound = compareInnerTable(attrtype, attrsLeft,
							rightData, attrsRight);
				if (matchFound == 0) {
//					cout<<"Match found."<<endl;
					void *leftDataFromMap = malloc(PAGE_SIZE);
					memset(leftDataFromMap, 0, PAGE_SIZE);
					void* keyToSelect = malloc(PAGE_SIZE);
					rbfm->readAttributeFromRecord(rightData, attrsRight,
							condition.rhsAttr, keyToSelect);
					readKeyValueFromHashMap(keyToSelect, leftDataFromMap);
					memset(keyToSelect,0,PAGE_SIZE);
//					cout<<"Record read from map: ";
//					rbfm->printRecord(attrsLeft,leftDataFromMap);
					cout<<endl;
					join.joinTuples(attrsLeft, leftDataFromMap, attrsRight,
							rightData, data);

					return 0;
				}


			}

			iterator=false;
		}
		}
		free(leftData);
		free(rightData);
		free(remainingTuple);
	return QE_EOF;
}
RC BNLJoin::readKeyValueFromHashMap(void* key,void* value)
{
	if(attrtype==TypeInt)
	{
		int intValue = 0;
		memcpy(&intValue, (char*) key + 1, sizeof(int));
		auto rc = typeIntMap.find(intValue);
		if(rc != typeIntMap.end()){
			memcpy((char*)value, typeIntMap[intValue], PAGE_SIZE);

		} else {
			return -1;
		}
	}
	if(attrtype==TypeReal)
	{
		float floatValue = 0.0;
		memcpy(&floatValue, (char*) key + 1, sizeof(float));
		auto rc = typeRealMap.find(floatValue);
		if(rc!=typeRealMap.end())
		{
			memcpy((char*)value,typeRealMap[floatValue],PAGE_SIZE);
		}
		else
			return -1;
	}
	if(attrtype==TypeVarChar)
	{
		int lenstr = 0;
		memcpy(&lenstr, (char*) key + 1, sizeof(int));
		string stringValue = string((char*) key + 1 + sizeof(int), lenstr);
		auto rc =typeVarCharMap.find(stringValue);
		if(rc!=typeVarCharMap.end())
		{
			memcpy((char*)value,typeVarCharMap[stringValue],PAGE_SIZE);
		}
		else
			return -1;
	}
	return 0;
}

RC BNLJoin::compareInnerTable(AttrType attrtype, vector<Attribute> attrsLeft,void *rightData, vector<Attribute> attrsRight) //RHS attribute
{
	void *rhsKey = malloc(PAGE_SIZE);
	int res = rbfm->readAttributeFromRecord(rightData, attrsRight,condition.rhsAttr, rhsKey);
	if (attrtype == TypeInt) {
		int intValue = 0;
		memcpy(&intValue, (char*) rhsKey + 1, sizeof(int));
//		cout << "Attribute Value read from RHS tuple:" << intValue << endl;
		//				auto it = typeIntMap.find(intValue);
		auto it = typeIntMap.find(intValue);
		if (it == typeIntMap.end()) {
			//not found
			return -1;
		} else {
			//Join
			return 0;
		}

	}
	if (attrtype == TypeReal) {
		float floatValue = 0.0;
		memcpy(&floatValue, (char*) rhsKey + 1, sizeof(float));
		auto it = typeRealMap.find(floatValue);
		if (it == typeRealMap.end()) {
			return -1;
		} else {
			return 0;
		}
	}
	if (attrtype == TypeVarChar) {
		int lenstr = 0;
		memcpy(&lenstr, (char*) rhsKey + 1, sizeof(int));
		string stringValue = string((char*) rhsKey + 1 + sizeof(int), lenstr);
		auto it = typeVarCharMap.find(stringValue);
		if (it == typeVarCharMap.end()) {
			return -1;
		} else {
			return 0;
		}
	}
	return -1;
}
RC BNLJoin::pushKeyToHashMap(void* key,AttrType attrtype,void* data){

				if (attrtype == TypeInt) {
					int keyValue = 0;
					memcpy(&keyValue, (char*) key + 1, sizeof(int));
//					cout<<"Key inserted:"<<keyValue<<endl;;
//					typeIntMap.insert( { keyValue, data });
					typeIntMap.insert(std::make_pair(keyValue,data));
				}
				if (attrtype == TypeReal) {
					float keyValueFloat=0.0;
					memcpy(&keyValueFloat,(char*)key+1,sizeof(float));
//					typeRealMap.insert({keyValueFloat,data});
					typeRealMap.insert(std::make_pair(keyValueFloat,data));
				}
				if(attrtype==TypeVarChar)
				{
					int len=0;
					memcpy(&len,(char*)key+1,sizeof(int));
					string keyValueString=string((char*)key+1+sizeof(int),len);
//					typeVarCharMap.insert({keyValueString,data});
					typeVarCharMap.insert(std::make_pair(keyValueString,data));
				}
				return 0;
}

RC BNLJoin::iterateAndCompareInnerTable(AttrType attrtype,vector<Attribute> attrsLeft, void *data)
{
	void* rightData = malloc(PAGE_SIZE);
	memset(rightData,0,PAGE_SIZE);
	JoinTuples *join;
	vector<Attribute> attrsRight;
	rightIn->getAttributes(attrsRight);
	while (rightIn->getNextTuple(rightData) != -1) {
		void *leftDataFromMap = malloc(PAGE_SIZE);
		memset(leftDataFromMap,0,PAGE_SIZE);
		if (condition.bRhsIsAttr) { //RHS attribute to be compared
			void *rhsKey = malloc(PAGE_SIZE);
			int res = rbfm->readAttributeFromRecord(rightData, attrsRight,
					condition.rhsAttr, rhsKey);
//			cout << "Read attribute from rbfm:" << res << endl;
			if (attrtype == TypeInt) {
				int intValue = 0;
				memcpy(&intValue, (char*) rhsKey + 1, sizeof(int));
//				cout << "Attribute Value read from RHS tuple:" << intValue
//						<< endl;
//				auto it = typeIntMap.find(intValue);
			auto it=typeIntMap.find(intValue);
				if (it == typeIntMap.end()) {
					//not found
				} else {
					//Join
//					cout << "Match found." << intValue<<endl;
					leftDataFromMap = typeIntMap.at(intValue);
//					cout << "Left data: ";
//					rbfm->printRecord(attrsLeft, leftDataFromMap);
//					cout << "Right data:";
//					rbfm->printRecord(attrsRight, rightData);
//					cout << endl;
					join->joinTuples(attrsLeft, leftDataFromMap, attrsRight,
							rightData, data);

				}

			}
			if (attrtype == TypeReal) {
				float floatValue = 0.0;
				memcpy(&floatValue, (char*) rhsKey + 1, sizeof(float));
				auto it = typeRealMap.find(floatValue);
				if (it == typeRealMap.end()) {
					//Not found
				} else {
					//Join
					leftDataFromMap = typeRealMap.at(floatValue);
					join->joinTuples(attrsLeft, leftDataFromMap, attrsRight,
							rightData, data);
				}
			}
			if (attrtype == TypeVarChar) {
				int lenstr = 0;
				memcpy(&lenstr, (char*) rhsKey + 1, sizeof(int));
				string stringValue = string((char*) rhsKey + 1 + sizeof(int),
						lenstr);
				auto it = typeVarCharMap.find(stringValue);
				if (it == typeVarCharMap.end()) {

				} else {
					//Join
					//-------------TRY!
					leftDataFromMap = typeVarCharMap.at(stringValue);
					join->joinTuples(attrsLeft, leftDataFromMap, attrsRight,
							rightData, data);
				}
			}

		} else { //RHS value to be compared
			if (attrtype == TypeInt) {
				int intValue = 0;
				memcpy(&intValue, (char*) condition.rhsValue.data, sizeof(int));
				auto it = typeIntMap.find(intValue);
				if (it == typeIntMap.end()) {
					//not found
				} else {
					//Join
					leftDataFromMap = typeIntMap.at(intValue);
					join->joinTuples(attrsLeft, leftDataFromMap, attrsRight,
							rightData, data);

				}

			}
			if (attrtype == TypeReal) {
				float floatValue = 0;
				memcpy(&floatValue, (char*) condition.rhsValue.data,
						sizeof(float));
				auto it = typeRealMap.find(floatValue);
				if (it == typeRealMap.end()) {
					//Not found
				} else {
					//Join
					leftDataFromMap = typeRealMap.at(floatValue);
					join->joinTuples(attrsLeft, leftDataFromMap, attrsRight,
							rightData, data);

				}
			}
			if (attrtype == TypeVarChar) {
				int strlen = 0;
				memcpy(&strlen, (char*) condition.rhsValue.data, sizeof(int));
				string stringValue = string(
						(char*) condition.rhsValue.data + sizeof(int), strlen);
				auto it = typeVarCharMap.find(stringValue);
				if (it == typeVarCharMap.end()) {
					//Not found
				} else {
					//Join
					leftDataFromMap = typeVarCharMap.at(stringValue);
					join->joinTuples(attrsLeft, leftDataFromMap, attrsRight,
							rightData, data);

				}
			}

		}}
		free(rightData);
		return 0;

	}

// ... the rest of your implementations go here
