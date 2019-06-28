package com.nebo.fseditlog;

import java.util.LinkedList;

public class FSEditLog {
    /**
     * 爬虫
     *
     * @param args
     */
    public static void main(String[] args) {
        FSEditLog fseditLog = new FSEditLog();
        for (int i = 0; i < 100; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < 100; j++) {
                        fseditLog.writeLog(Thread.currentThread().getName());
                    }
                }
            }).start();
        }
    }

    public long taxid = 0;
    public DoubleBuffer doubleBuffer = new DoubleBuffer();
    //当前是否有线程在刷写磁盘
    public boolean isRunning = false;
    public long maxTaxid = 0;
    public ThreadLocal<Long> threadLocalTaxid = new ThreadLocal<Long>();
    public boolean isWait = false;

    //分段加锁代码
    public void writeLog(String log) { //写内存加锁可以达到ns 级别
        //currentBuffer: 1 2 3 4 5多条数据
        synchronized (this) {
            taxid++;
            //构建日志对象
            EditLog editLog = new EditLog(taxid, log);
            //写内存
            doubleBuffer.writeLog(editLog);
            threadLocalTaxid.set(taxid);
        }//分段 释放
        flushLog();
    }

    private void flushLog() {
        //taxid 1  2  6
        synchronized (this) {
            //true
            if (isRunning) {//代表当前有线程在同步数据到磁盘
                // 6
                long localTaxid = threadLocalTaxid.get();

                if (localTaxid <= maxTaxid) {
                    return;
                }
                if (isWait) {//true
                    return;
                }
                isWait = true;
                while (isRunning) {
                    try {
                        //释放锁  线程6等着
                        this.wait(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                isWait = false;
            }
            //交互内存
            doubleBuffer.exchange();
            isRunning = true;
            if (doubleBuffer.syncBuffer.size() != 0) {
                //5
                maxTaxid = doubleBuffer.getMaxTaxid();
            }
        }//释放锁
        //把内存里面的数据写到磁盘
        //要把当前内存里面的[ 1 2 3 4 5]  taxid = 5 -》 磁盘
        doubleBuffer.flush();
        synchronized (this) {
            //释放锁
            this.notifyAll();
            isRunning = false;
        }

    }


    public class DoubleBuffer {
        //消息队列
        //用来写日志的
        public LinkedList<EditLog> currentBuffer = new LinkedList<EditLog>();
        //用来把内存里面的元数据同步到磁盘上面
        public LinkedList<EditLog> syncBuffer = new LinkedList<EditLog>();

        /**
         * 写日志
         *
         * @param editLog
         */
        public void writeLog(EditLog editLog) {
            currentBuffer.add(editLog);
        }

        /**
         * 把数据写到磁盘上面
         * 比较慢的
         * 经常几十毫秒 几百毫毛
         */
        public void flush() {
            for (EditLog editLog : syncBuffer) {
                //把数据写到磁盘上面了
                System.out.println(editLog);
            }
            syncBuffer.clear();
        }

        /**
         * 交互内存
         */
        public void exchange() {
            LinkedList<EditLog> tmp = currentBuffer;
            currentBuffer = syncBuffer;
            syncBuffer = tmp;
        }

        /**
         * 获取到当前同步到磁盘的这个内存，里面最大的一个事务id
         *
         * @return
         */
        public long getMaxTaxid() {
            return syncBuffer.getLast().taxid;
        }


    }


    //面向对象的思想，把每一条数据信息，都看成是一个对象
    public class EditLog {
        public long taxid;
        public String log;

        public EditLog(long taxid, String log) {
            super();
            this.taxid = taxid;
            this.log = log;
        }

        @Override
        public String toString() {
            return "EditLog [taxid=" + taxid + ", log=" + log + "]";
        }


    }

}
