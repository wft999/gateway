#include <stdio.h>
#include <stdlib.h>
#include "os.h"

#include "db.h"
#include <tscompression.h>
#include <tchecksum.h>
#include <zlib.h>
#include <zstd.h>

extern int gColTypeSize[];
extern char* gColTypeFormat[];
int (*gTaosCompFunc[])(const char *const input, int inputSize, const int elements, char *const output, int outputSize,
                   char algorithm, char *const buffer, int bufferSize) = {
                                          tsCompressBool,
                                          tsCompressTinyint,
                                          tsCompressSmallint,
                                          tsCompressInt,
                                          tsCompressBigint,
                                          tsCompressFloat,
                                          tsCompressDouble,
                                          tsCompressString,
                                          tsCompressTimestamp,
                                          tsCompressString};

int (*gTaosDecompFunc[])(const char *const input, int compressedSize, const int elements, char *const output,
                     int outputSize, char algorithm, char *const buffer, int bufferSize) = {
                                                            tsDecompressBool,
                                                            tsDecompressTinyint,
                                                            tsDecompressSmallint,
                                                            tsDecompressInt,
                                                            tsDecompressBigint,
                                                            tsDecompressFloat,
                                                            tsDecompressDouble,
                                                            tsDecompressString,
                                                            tsDecompressTimestamp,
                                                            tsDecompressString};

int compressNone(const void* handle,INDEX* const pIndex){
	DB* pDb = (DB*)handle;
	if(pDb == NULL || pDb->signature != handle){
		gErrorCode = 3;
		return -1;
	}

	int inputCount =  pDb->pLogHead->curRowCount;
	int outSize = pDb->pLogHead->maxColSize * inputCount + sizeof(TSCKSUM);
	char* pOutput = malloc(outSize);
	if(pOutput == NULL){
		logError("compressNone","pOutput == NULL");
		return -2;
	}

	int totalSize = 0;
	char* pInput = (char*)pDb->pLogData;
	for(int i = 0; i < pDb->pLogHead->colCount; i++){
		COL_TYPE type = pDb->pLogHead->colType[i];
		int colSize = gColTypeSize[type];
		int size = colSize*inputCount;
		memcpy(pOutput, pInput, size);

		size += sizeof(TSCKSUM);
		taosCalcChecksumAppend(0, (uint8_t*)pOutput, size);

		write(pDb->recordFd, pOutput, size);

		pInput += colSize * pDb->pLogHead->cacheSize;
		totalSize += size;
		pIndex->colLens[i] = size;
	}
	free(pOutput);

	return totalSize;
}

int decompressNone(const void* handle,const char* const pInput,const INDEX* const pIndex,int col,char* pOutput, int outSize){
	DB* pDb = (DB*)handle;
	if(pDb == NULL || pDb->signature != handle){
		return -1;
	}

	COL_TYPE type = pDb->pLogHead->colType[col];
	int colSize = gColTypeSize[type];

	memcpy(pOutput,pInput,colSize * pIndex->rowCount);

	return 0;
}


int compressTaosOneStep(const void* handle,INDEX* const pIndex){
	DB* pDb = (DB*)handle;
	if(pDb == NULL || pDb->signature != handle){
		return -1;
	}

	int inputCount =  pDb->pLogHead->curRowCount;
	int outSize = pDb->pLogHead->maxColSize * inputCount  + sizeof(TSCKSUM);
	char* pOutput = malloc(outSize);
	if(pOutput == NULL){
		logError("compressTaosOneStep","pOutput == NULL");
		return -2;
	}

	int totalSize = 0;
	char* pInput = (char*)pDb->pLogData;
	for(int i = 0; i < pDb->pLogHead->colCount; i++){
		COL_TYPE type = pDb->pLogHead->colType[i];
		int colSize = gColTypeSize[type];
		int size = gTaosCompFunc[type](pInput,colSize*inputCount,inputCount,pOutput,outSize, ONE_STAGE_COMP,NULL, 0);
		size += 4 - size%4;
		size += sizeof(TSCKSUM);
		taosCalcChecksumAppend(0, (uint8_t*)pOutput, size);
		write(pDb->recordFd, pOutput, size);

		pInput += colSize * pDb->pLogHead->cacheSize;
		totalSize += size;
		pIndex->colLens[i] = size;
	}
	free(pOutput);

	return totalSize;
}

int decompressTaosOneStep(const void* handle,const char* const pInput,const INDEX* const pIndex,int col,char* pOutput, int outSize){
	DB* pDb = (DB*)handle;
	if(pDb == NULL || pDb->signature != handle){
		return -1;
	}

	COL_TYPE type = pDb->pLogHead->colType[col];
	int colSize = gColTypeSize[type];

	int size = gTaosDecompFunc[type](pInput,pIndex->colLens[col] - sizeof(TSCKSUM),pIndex->rowCount,pOutput,outSize, ONE_STAGE_COMP,NULL, 0);
	if(size != colSize * pIndex->rowCount){
		logError("compressTaosOneStep","size != colSize * pIndex->count");
		return -3;
	}

	return 0;
}

int compressTaosTwoStep(const void* handle,INDEX* const pIndex){
	DB* pDb = (DB*)handle;
	if(pDb == NULL || pDb->signature != handle){
		gErrorCode = 3;
		return -1;
	}

	return 0;
}

int decompressTaosTwoStep(const void* handle,const char* const pInput,const INDEX* const pIndex,int col,char* pOutput, int outSize){
	return 0;
}

int zipCompressInit(z_stream *pStream) {
  pStream->zalloc = (alloc_func) 0;
  pStream->zfree = (free_func) 0;
  pStream->opaque = (voidpf) 0;
  if (deflateInit2(pStream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, MAX_WBITS + 16, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
    return -1;
  }

  return 0;
}

int zipCompress(z_stream *pStream, char *srcData, int32_t nSrcData, char *destData, int32_t *nDestData) {
  int err = 0;
  pStream->next_in = (Bytef *) srcData;
  pStream->avail_in = (uLong) nSrcData;
  pStream->next_out = (Bytef *) destData;
  pStream->avail_out = (uLong) (*nDestData);

  while (pStream->avail_in != 0 && pStream->total_out < (uLong) (*nDestData)) {
    if (deflate(pStream, Z_FULL_FLUSH) != Z_OK) {
      return -1;
    }
  }

  if (pStream->avail_in != 0) {
    return pStream->avail_in;
  }

  for (;;) {
      if ((err = deflate(pStream, Z_FINISH)) == Z_STREAM_END) {
        break;
      }
      if (err != Z_OK) {
        return -2;
      }
    }

    if (deflateEnd(pStream) != Z_OK) {
      return -3;
    }

  *nDestData = (int32_t) (pStream->total_out);
  return 0;
}

int zipDeCompress(char *srcData, int32_t nSrcData, char *destData, int32_t *nDestData) {
  int err = 0;
  z_stream gzipStream = {0};

  static char dummyHead[2] = {
          0x8 + 0x7 * 0x10,
          (((0x8 + 0x7 * 0x10) * 0x100 + 30) / 31 * 31) & 0xFF,
  };

  gzipStream.zalloc = (alloc_func) 0;
  gzipStream.zfree = (free_func) 0;
  gzipStream.opaque = (voidpf) 0;
  gzipStream.next_in = (Bytef *) srcData;
  gzipStream.avail_in = 0;
  gzipStream.next_out = (Bytef *) destData;
  if (inflateInit2(&gzipStream, 47) != Z_OK) {
    return -1;
  }

  while (gzipStream.total_out < *nDestData && gzipStream.total_in < nSrcData) {
    gzipStream.avail_in = gzipStream.avail_out = nSrcData;  //1
    if ((err = inflate(&gzipStream, Z_NO_FLUSH)) == Z_STREAM_END) {
      break;
    }

    if (err != Z_OK) {
      if (err == Z_DATA_ERROR) {
        gzipStream.next_in = (Bytef *) dummyHead;
        gzipStream.avail_in = sizeof(dummyHead);
        if ((err = inflate(&gzipStream, Z_NO_FLUSH)) != Z_OK) {
          return -2;
        }
      } else return -3;
    }
  }

  if (inflateEnd(&gzipStream) != Z_OK) {
    return -4;
  }
  *nDestData = gzipStream.total_out;

  return 0;
}

int decompressZlib(const void* handle,const char* const pInput,const INDEX* const pIndex,int col,char* pOutput, int outSize){
	DB* pDb = (DB*)handle;
	if(pDb == NULL || pDb->signature != handle){
		gErrorCode = 3;
		return -1;
	}

	COL_TYPE type = pDb->pLogHead->colType[col];
	int32_t colSize = gColTypeSize[type];

	int32_t size = outSize;
	zipDeCompress(pInput, pIndex->colLens[col] - sizeof(TSCKSUM), pOutput, &size);
	if(size != colSize * pIndex->rowCount){
		logError("decompressZlib","size != colSize * pIndex->count");
		return -3;
	}

	return 0;
}


int compressZlib(const void* handle,INDEX* const pIndex){
	DB* pDb = (DB*)handle;
	if(pDb == NULL || pDb->signature != handle){
		gErrorCode = 3;
		return -1;
	}

	int32_t inputCount =  pDb->pLogHead->curRowCount;
	int32_t outSize = pDb->pLogHead->maxColSize * inputCount  + sizeof(TSCKSUM) + 100;
	char* pOutput = malloc(outSize);
	if(pOutput == NULL){
		logError("compressZlib","pOutput == NULL");
		return -2;
	}

	int32_t totalSize = 0;
	char* pInput = (char*)pDb->pLogData;
	for(int i = 0; i < pDb->pLogHead->colCount; i++){
		COL_TYPE type = pDb->pLogHead->colType[i];
		int colSize = gColTypeSize[type];

		z_stream     stream;
		if (zipCompressInit(&stream) != 0) {
			logError("compressZlib","zipCompressInit fail");
			return -1;
		}

		int32_t size = outSize;
		zipCompress(&stream, pInput, colSize*inputCount, pOutput, &size);
		size += 4 - size%4;
		size += sizeof(TSCKSUM);

		taosCalcChecksumAppend(0, (uint8_t*)pOutput, size);
		write(pDb->recordFd, pOutput, size);

		pInput += colSize * pDb->pLogHead->cacheSize;
		totalSize += size;
		pIndex->colLens[i] = size;

	}
	free(pOutput);

	return totalSize;
}

int decompressZstd(const void* handle,const char* const pInput,const INDEX* const pIndex,int col,char* pOutput, int outSize){
	DB* pDb = (DB*)handle;
	if(pDb == NULL || pDb->signature != handle){
		gErrorCode = 3;
		return -1;
	}

	COL_TYPE type = pDb->pLogHead->colType[col];
	int colSize = gColTypeSize[type];

	int size = 0;//ZSTD_decompress(pOutput, outSize, pInput, pIndex->colLens[col] - sizeof(TSCKSUM));
	zipDeCompress(pInput, pIndex->colLens[col] - sizeof(TSCKSUM), pOutput, &size);

	if(size != colSize * pIndex->rowCount){
		logError("decompressZlib","size != colSize * pIndex->count");
		return -3;
	}

	return 0;
}


int compressZstd(const void* handle,INDEX* const pIndex){
	DB* pDb = (DB*)handle;
	if(pDb == NULL || pDb->signature != handle){
		gErrorCode = 3;
		return -1;
	}

	int inputCount =  pDb->pLogHead->curRowCount;
	int outSize = pDb->pLogHead->maxColSize * inputCount  + sizeof(TSCKSUM);
	char* pOutput = malloc(outSize);
	if(pOutput == NULL){
		logError("compressZstd","pOutput == NULL");
		return -2;
	}

	int totalSize = 0;
	char* pInput = (char*)pDb->pLogData;
	for(int i = 0; i < pDb->pLogHead->colCount; i++){
		COL_TYPE type = pDb->pLogHead->colType[i];
		int colSize = gColTypeSize[type];

		//int size = ZSTD_compress(pOutput, outSize, pInput, colSize*inputCount, 1);
		z_stream     stream;
		if (zipCompressInit(&stream) != 0) {
			logError("compressZlib","zipCompressInit fail");
			return -1;
		}
		int size = 0;
		zipCompress(&stream, pInput, colSize*inputCount, pOutput, &size);

		size += sizeof(TSCKSUM);
		taosCalcChecksumAppend(0, (uint8_t*)pOutput, size);
		write(pDb->recordFd, pOutput, size);

		pInput += colSize * pDb->pLogHead->cacheSize;
		totalSize += size;
		pIndex->colLens[i] = size;
	}
	free(pOutput);

	return totalSize;
}

