#include "heapfile.h"
#include "error.h"

// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
		// file doesn't exist. First create it and allocate
		// an empty header page and data page.
		status = db.createFile(fileName);
		if (status != OK) return status;

		status = db.openFile(fileName, file);
		if (status != OK) return status;

		// allocate the header page
		status = bufMgr->allocPage(file, hdrPageNo, newPage);
		if (status != OK) return status;
		hdrPage = (FileHdrPage*) newPage;

		// initialize header page fields
		strncpy(hdrPage->fileName, fileName.c_str(), MAXNAMESIZE);
		hdrPage->firstPage = -1;  // will be set after allocating first data page
		hdrPage->lastPage = -1;   // same as above
		hdrPage->pageCnt = 0;
		hdrPage->recCnt = 0;

		// allocate the first data page
		status = bufMgr->allocPage(file, newPageNo, newPage);
		if (status != OK) return status;

		// initialize the first data page
		newPage->init(newPageNo);

		// update header page with first/last page info
		hdrPage->firstPage = newPageNo;
		hdrPage->lastPage = newPageNo;
		hdrPage->pageCnt = 1;

		// unpin both pages and mark as dirty
		status = bufMgr->unPinPage(file, hdrPageNo, true);
		if (status != OK) return status;

		status = bufMgr->unPinPage(file, newPageNo, true);
		if (status != OK) return status;

		status = db.closeFile(file);
		return status;
    }
    return (FILEEXISTS);
}


// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

// constructor opens the underlying file
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
		// get the first page number from the file
		status = filePtr->getFirstPage(headerPageNo);
		if (status != OK) {
			cerr << "error in getFirstPage\n";
			returnStatus = status;
			return;
		}

		// read and pin the header page
		status = bufMgr->readPage(filePtr, headerPageNo, pagePtr);
		if (status != OK) {
			cerr << "error in readPage of header page\n";
			returnStatus = status;
			return;
		}
		headerPage = (FileHdrPage*) pagePtr;
		hdrDirtyFlag = false;

		// read and pin the first data page if it exists
		curPageNo = headerPage->firstPage;
		if (curPageNo != -1) {
			status = bufMgr->readPage(filePtr, curPageNo, curPage);
			if (status != OK) {
				cerr << "error in readPage of first data page\n";
				returnStatus = status;
				return;
			}
			curDirtyFlag = false;
		} else {
			curPage = NULL;
		}

		curRec = NULLRID;
		returnStatus = OK;
    }
    else
    {
    	cerr << "open of heap file failed\n";
		returnStatus = status;
		return;
    }
}



// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
	 // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
	
	// status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
	// if (status != OK) cerr << "error in flushFile call\n";
	// before close the file
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

// Return number of records in heap file
const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter
const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
    if (curPage != NULL) {
        if (rid.pageNo == curPageNo) {
            // record is on the currently pinned page
            status = curPage->getRecord(rid, rec);
            if (status == OK) {
                curRec = rid;
                return OK;
            }
            return status;
        }
        else {
            // unpin current page
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) return status;
            curPage = NULL;
        }
    }

    // read the page containing the record
    status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
    if (status != OK) return status;

    curPageNo = rid.pageNo;
    curDirtyFlag = false;
    curRec = rid;

    // get the record
    return curPage->getRecord(rid, rec);
}



HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}


const Status HeapFileScan::scanNext(RID& outRid)
{
    Status 	status = OK;
    RID		nextRid;
    RID		tmpRid;
    int 	nextPageNo;
    Record  rec;

    if(curPageNo == -1) {   // getNextPage() returns -1 in previous iteration if EOF
        return FILEEOF;
    }

    if (curPage == NULL) {
        // start scan from first page
        if (headerPage->firstPage == -1) {
            return FILEEOF; // empty file
        }
        status = bufMgr->readPage(filePtr, headerPage->firstPage, curPage);
        if (status != OK) return status;
        curPageNo = headerPage->firstPage;
        curDirtyFlag = false;

        // get first record on page
        status = curPage->firstRecord(tmpRid);
        if (status == OK) {
            status = curPage->getRecord(tmpRid, rec);
            if (status != OK) return status;
            
            if (matchRec(rec)) {
                outRid = tmpRid;
                curRec = tmpRid;
                return OK;
            }
            curRec = tmpRid;    // For iteration purposes, won't be returned
            // record doesn't match, continue to next record
        }
        else if (status == NORECORDS) {
            // empty page, go to next page
            // this never happens within testing, bug isn't here
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) return status;

            return scanNext(outRid);
        }
        else {
            return status;
        }
    }

    // loop until we find a matching record or reach end of file
    while (true) {
        // get next record on current page
        status = curPage->nextRecord(curRec, nextRid);
        if (status == OK) {
            status = curPage->getRecord(nextRid, rec);
            if (status != OK) return status;
            
            if (matchRec(rec)) {
                outRid = nextRid;
                curRec = nextRid;
                return OK;
            }
            // record doesn't match, continue to next record
            curRec = nextRid;
            continue;
        }
        else if (status == ENDOFPAGE) {
            // get next page in file
            status = curPage->getNextPage(nextPageNo);
            if (status != OK) { 
                return status;
            }
            
            // unpin current page
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) return status;
            
            if (nextPageNo == -1) {
                // no more pages
                curPage = NULL;
                curPageNo = -1;
                return FILEEOF;
            }
            
            // read next page
            status = bufMgr->readPage(filePtr, nextPageNo, curPage);
            if (status != OK) return status;
            curPageNo = nextPageNo;
            curDirtyFlag = false;
            
            // get first record on new page
            status = curPage->firstRecord(tmpRid);
            if (status == OK) {
                status = curPage->getRecord(tmpRid, rec);
                if (status != OK) return status;
                
                if (matchRec(rec)) {
                    outRid = tmpRid;
                    curRec = tmpRid;
                    return OK;
                }
                // record doesn't match, continue to next record
                curRec = tmpRid;
                continue;
            }
            else if (status == NORECORDS) {
                // empty page, continue to next page
                continue;
            }
            else {
                return status;
            }
        }
        else {
            return status;
        }
    }
}




// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status, unpinstatus;
    RID		rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    if (curPage == NULL) {
        // no current page - start with last page
        if (headerPage->lastPage == -1) {
            // file is empty - need to allocate first page
            status = bufMgr->allocPage(filePtr, newPageNo, newPage);
            if (status != OK) return status;
            
            newPage->init(newPageNo);
            
            // update header page
            headerPage->firstPage = newPageNo;
            headerPage->lastPage = newPageNo;
            headerPage->pageCnt = 1;
            hdrDirtyFlag = true;
            
            curPage = newPage;
            curPageNo = newPageNo;
            curDirtyFlag = false;
        } else {
            // read last page
            status = bufMgr->readPage(filePtr, headerPage->lastPage, curPage);
            if (status != OK) return status;
            curPageNo = headerPage->lastPage;
            curDirtyFlag = false;
        }
    }

    // try to insert record
    status = curPage->insertRecord(rec, outRid);
    if (status == OK) {
        headerPage->recCnt++;
        hdrDirtyFlag = true;
        curDirtyFlag = true;
        return OK;
    }
    else if (status == NOSPACE) {
        // current page is full - allocate new page
        status = bufMgr->allocPage(filePtr, newPageNo, newPage);
        if (status != OK) return status;
        
        newPage->init(newPageNo);
        
        // link new page to file
        status = curPage->setNextPage(newPageNo);
        if (status != OK) {
            bufMgr->unPinPage(filePtr, newPageNo, true);
            return status;
        }
        curDirtyFlag = true;
        
        // update header page
        headerPage->lastPage = newPageNo;
        headerPage->pageCnt++;
        hdrDirtyFlag = true;
        
        // unpin current page
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) return status;
        
        // make new page current
        curPage = newPage;
        curPageNo = newPageNo;
        curDirtyFlag = false;
        
        // try insert again
        status = curPage->insertRecord(rec, outRid);
        if (status != OK) return status;
        
        headerPage->recCnt++;
        hdrDirtyFlag = true;
        curDirtyFlag = true;
        return OK;
    }
    else {
        return status;
    }
}
