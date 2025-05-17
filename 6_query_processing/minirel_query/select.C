#include "catalog.h"
#include "query.h"
#include <iostream>
#include <cstring>

using namespace std;

// Forward declaration
const Status ScanSelect(const string &result,
                        const int projCnt,
                        const AttrDesc projNames[],
                        const AttrDesc *attrDesc,
                        const Operator op,
                        const char *filter,
                        const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 *  OK on success
 *  an error code otherwise
 */
const Status QU_Select(const string & result,
    const int projCnt,
    const attrInfo projNames[],
    const attrInfo *attr,
    const Operator op,
    const char *attrValue)
{
    cout << "Doing QU_Select " << endl;
    Status status;

    if (projCnt <= 0 || projNames == nullptr) return BADCATPARM;

	// Get attribute descriptors for projection
    AttrDesc *projDescs = new AttrDesc[projCnt];
    for (int i = 0; i < projCnt; ++i) {
        status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, projDescs[i]);
        if (status != OK) {
			// free(projDescs)
            delete[] projDescs;
            return status;
        }
    }

    int reclen = 0;
    for (int i = 0; i < projCnt; ++i)
        reclen += projDescs[i].attrLen;


	// Setup filter value and type if attr is provided (else filter null)
    AttrDesc filterDesc;
    char *filterValue = nullptr;
    if (attr != nullptr) {
        status = attrCat->getInfo(attr->relName, attr->attrName, filterDesc);
        if (status != OK) {
            delete[] projDescs;
            return status;
        }

        filterValue = new char[filterDesc.attrLen];
        switch (filterDesc.attrType) {
            case INTEGER: {
                int val = atoi(attrValue);
                memcpy(filterValue, &val, sizeof(int));
                break;
            }
            case FLOAT: {
                float val = atof(attrValue);
                memcpy(filterValue, &val, sizeof(float));
                break;
            }
            case STRING: {
                strncpy(filterValue, attrValue, filterDesc.attrLen);
                filterValue[filterDesc.attrLen - 1] = '\0'; // Null-terminate
                break;
            }
            default:
				// free(filterValue);
				// free(projDescs);
                delete[] filterValue;
                delete[] projDescs;
                return ATTRTYPEMISMATCH;
        }
    }

    // Call ScanSelect
    status = ScanSelect(result, projCnt, projDescs,
                        (attr != nullptr) ? &filterDesc : nullptr,
                        op, filterValue, reclen);
	// free(projDescs);
    delete[] projDescs;
    if (filterValue != nullptr) delete[] filterValue;
    return status;
}

const Status ScanSelect(const string & result,
    const int projCnt,
    const AttrDesc projNames[],
    const AttrDesc *attrDesc,
    const Operator op,
    const char *filter,
    const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
    Status status;

	// Create scanner
    string targetRel = (attrDesc != nullptr) ? attrDesc->relName : projNames[0].relName;
    HeapFileScan scan(targetRel, status);
    if (status != OK) return status;

	// Start scan
    if (attrDesc != nullptr) {
        status = scan.startScan(
            attrDesc->attrOffset,
            attrDesc->attrLen,
            static_cast<Datatype>(attrDesc->attrType),
            filter,
            op
        );
    } else {	// Case where no filter is provided
        status = scan.startScan(0, 0, STRING, nullptr, EQ);
    }
    if (status != OK) return status;

    InsertFileScan resultRel(result, status);
    if (status != OK) {
        scan.endScan();
        return status;
    }

	// Iterate through each element in the scan
    char *outData = new char[reclen];
    RID rid;
    while (scan.scanNext(rid) == OK) {
        Record rec;
        status = scan.getRecord(rec);
        if (status != OK) break;

		// Write out the data to the output record
        int offset = 0;
        for (int i = 0; i < projCnt; i++) {
            memcpy(outData + offset,
                   (char*)rec.data + projNames[i].attrOffset,
                   projNames[i].attrLen);
            offset += projNames[i].attrLen;
        }

        Record outRec;
        outRec.data = outData;
        outRec.length = reclen;
        RID outRid;
        status = resultRel.insertRecord(outRec, outRid);
        if (status != OK) break;
    }

    // free(outData);
    delete[] outData;
    scan.endScan();
    return (status == FILEEOF) ? OK : status;	// Done! :)
}
