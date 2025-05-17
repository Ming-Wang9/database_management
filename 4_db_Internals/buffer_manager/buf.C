#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    // since i get error here, so I initialize each BufDesc explicitly
    // for (int i = 0; i < bufs; i++) {
    //    bufTable[i].Clear(); // Initialize using the Clear() method
    //    bufTable[i].frameNo = i; // Set the frame number
    // }
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
    delete hashTable; // Addition to avoid memory leak
}



/* 
Allocates a free frame using the clock algorithm; if necessary, 
writing a dirty page back to disk.

Returns BUFFEREXCEEDED if all buffer frames are pinned, 
UNIXERR if the call to the I/O layer returned an error when a dirty page was 
being written to disk and OK otherwise.  
*/
const Status BufMgr::allocBuf(int & frame) 
{
    int numFilledFrame = 0;
    while (numFilledFrame < 2 * numBufs) {
        advanceClock(); // move to the next frame
        BufDesc* bufDesc = &bufTable[clockHand];  //using pointer and derefrence to get the info of curr frame

        // 1. frame is not in use
        if (!bufDesc->valid){
            frame = clockHand;
            return OK;
        }

        // 2. refbi is set
        if (bufDesc->refbit) {
            bufDesc->refbit = false;    // clean the refbit 
            numFilledFrame++;
            continue;
        }

        // 3. if the frame is pinned, can't be used
        if (bufDesc->pinCnt > 0) {
            numFilledFrame++;
            continue;
        }

        // 4. victim frame to replace
        if (bufDesc->dirty){
            Status status = bufDesc->file->writePage(
                bufDesc->pageNo, &bufPool[clockHand]);  // have to dereference
            if (status != OK){
                return UNIXERR;     // error in C++, if disk write fails
            }
            bufDesc->dirty = false; 
        }

        // remove page from hashtable 
        hashTable->remove(bufDesc->file, bufDesc->pageNo);
        bufDesc->Clear();

        // allocate this frame
        frame = clockHand;
        return OK;
    }

    return BUFFEREXCEEDED;  // all frame are pinned
}

	
/*

Reads page from buffer pool (or disk if not present in buffer).

Returns OK if no errors occurred, UNIXERR if a Unix error occurred, 
BUFFEREXCEEDED if all buffer frames are pinned, 
HASHTBLERROR if a hash table error occurred.

*/
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    int frameNo = 0;
    Status status;

    // check if page is already in the buffer pool
    status = hashTable->lookup(file, PageNo, frameNo);

    // 1. page is already in the buffer pool
    if (status == OK) {
        bufTable[frameNo].refbit = true;   
        bufTable[frameNo].pinCnt++;       
        page = &bufPool[frameNo];         // set page POINTER!!!
        return OK;
    }

    // 2. page is not in buffer pool
    if (status == HASHNOTFOUND) {
        status = allocBuf(frameNo);     // then allocate a buffer frame
        if (status != OK) {
            return status;     // if no frame is available, it is an error
        }

        // read page from disk into the allocated frame
        status = file->readPage(PageNo, &bufPool[frameNo]);
        if (status != OK) {
            return UNIXERR;     // if disk read fails, it is an error
        }

        // insert the page into the hash table
        status = hashTable->insert(file, PageNo, frameNo);
        if (status != OK) {
            return HASHTBLERROR;    // if hash table insertion fails
        }

        // set up the buffer frame for the new page
        bufTable[frameNo].Set(file, PageNo);
        page = &bufPool[frameNo];   // return POINTER!!!!!!! to the buffer frame
        return OK;
    }

    return HASHTBLERROR;

}

/*
Decrements the pinCnt of the frame containing (file, PageNo) and, if dirty == true, 
sets the dirty bit.  

Returns OK if no errors occurred, 
HASHNOTFOUND if the page is not in the buffer pool hash table, 
PAGENOTPINNED if the pin count is already 0.
*/
const Status BufMgr::unPinPage(File* file, const int PageNo, const bool dirty) 
{
    int frameNo = 0;
    Status status;

    status = hashTable->lookup(file, PageNo, frameNo);
    if (status != OK) {
        return HASHNOTFOUND;
    }

    // if page is already unpinned
    if (bufTable[frameNo].pinCnt == 0) {
        return PAGENOTPINNED;
    }
    
    bufTable[frameNo].pinCnt--;
    
    // set dirty bit, if dirty flag is true
    if (dirty) {
        bufTable[frameNo].dirty = true;
    }
    
    return OK;
}


/*
Allocates a page into the buffer pool. 

Returns OK if no errors occurred, UNIXERR if a Unix error occurred, 
BUFFEREXCEEDED if all buffer frames are pinned and HASHTBLERROR if a hash table error occurred. 
*/
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    int frameNo;

    Status status = file->allocatePage(pageNo);   // allocate empty page in file
    if (status != OK) {
        return status;
    }
    
    // allocate buffer frame for new page
    status = allocBuf(frameNo);  
    if (status != OK) {
        file->disposePage(pageNo);                // if buffer allocation fails, deallocate the page
        return status; 
    }
    
    // Insert entry into hash table
    status = hashTable->insert(file, pageNo, frameNo);
    if (status != OK) {
        file->disposePage(pageNo);               // if hash table insert fails, clean it up
        bufTable[frameNo].Clear();             
        return HASHTBLERROR;
    }
    
  
    bufTable[frameNo].Set(file, pageNo);          // set up buffer frame for new page 
    memset(&bufPool[frameNo], 0, sizeof(Page));   // clear page data
    page = &bufPool[frameNo];                     // return POINTER!!!!! to buffer frame
    
    return OK;
}

/*
Provided method

Disposes page, returning OK if successful and UNIXERR if a Unix error occurred. 
*/
const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}

