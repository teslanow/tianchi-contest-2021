package com.aliyun.adb.contest;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CyclicBarrier;

import static com.aliyun.adb.contest.UnsafeUtil.UNSAFE;

public class ProducerThread implements Runnable{
    long[] readStart;
    long[] trueSizeOfMmap;
    long[][] allBufsAddress = new long[Constant.COLNUM_EACHTABLE][Constant.BOUNDARYSIZE];
    long directBufferBase;
    int threadNo;
    FileChannel[] fileChannel;
    ByteBuffer directBuffer;
    Partion[][] allPartion;
    CyclicBarrier[] allBarrier;
    ProducerThread(int threadNo, long[] readStart , long[] trueSizeOfMmap, FileChannel[] fileChannel, Partion[][] allPartion, CyclicBarrier[] allBarrier)
    {
        this.threadNo = threadNo;
        this.readStart = readStart;
        this.trueSizeOfMmap = trueSizeOfMmap;
        this.fileChannel = fileChannel;
        this.allPartion = allPartion;
        this.allBarrier = allBarrier;
    }
    @Override
    public void run() {
        this.directBuffer = ByteBuffer.allocateDirect(Constant.EACHREADSIZE);
        this.directBufferBase = ((DirectBuffer)directBuffer).address();
        for(int i = 0; i < Constant.COLNUM_EACHTABLE; i++)
        {
            for(int j = 0; j < Constant.BOUNDARYSIZE; j++)
            {
                allBufsAddress[i][j] = UNSAFE.allocateMemory(Constant.BYTEBUFFERSIZE);
            }
        }
        long[][] allBufSize = new long[Constant.COLNUM_EACHTABLE][Constant.BOUNDARYSIZE];
        try{
            for(int k = 0; k < Constant.TABLENUM; k++) {
                long nowRead = 0, realRead;
                long leftSize = trueSizeOfMmap[k];
                while (leftSize > 0) {
                    realRead = Math.min(leftSize, Constant.EACHREADSIZE);
                    if(leftSize < Constant.EACHREADSIZE)
                        System.out.println("hello");
                    directBuffer.clear();
                    fileChannel[k].read(directBuffer, realRead + nowRead);
                    for (int i = (int) realRead - 1; i >= 0; i--) {
                        if (UNSAFE.getByte(directBufferBase + i) != 10) {
                            realRead--;
                        } else {
                            break;
                        }
                    }
                    System.out.println("realRead " + realRead);
                    leftSize -= realRead;
                    nowRead += realRead;
                    long val = 0;
                    byte t;
                    long curPos = directBufferBase;
                    long endPos = directBufferBase + realRead;
                    int col = 0;
                    for (; curPos < endPos; curPos++) {
                        t = UNSAFE.getByte(curPos);
                        if ((t & 16) == 0) {
                            int index = (int)(val >> Constant.SHIFTBITNUM);
                            long address = allBufsAddress[col][index];
                            long size = allBufSize[col][index];
                            UNSAFE.putLong(null, address, val);
                            size += 8;
                            val = 0;
                            if(size != Constant.BYTEBUFFERSIZE)
                            {
                                allBufSize[col][index] = size;
                                col ^= 0x1;
                                continue;
                            }
                            else
                            {
                                Partion partion = allPartion[col][index];
                                partion.acquire();
                                partion.writeSeq(address, Constant.BYTEBUFFERSIZE, k);
                                allBufSize[col][index] = 0;
                                col ^= 0x1;
                                continue;
                            }
                        } else {
                            val = (val << 1) + (val << 3) + (t - 48);
                        }
                    }

                }
                for(int i = 0; i < Constant.COLNUM_EACHTABLE; i++)
                {
                    for(int j = 0; j < Constant.BOUNDARYSIZE; j++)
                    {
                        Partion partion = allPartion[i][j];
                        long address = allBufsAddress[i][j];
                        partion.acquire();
                        partion.writeSeq(address, Constant.BYTEBUFFERSIZE, k);
                        allBufSize[i][j] = 0;
                    }
                }
                allBarrier[k].await();
            }
        }catch (Exception e){
            e.printStackTrace();
        }


    }
}