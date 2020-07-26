package com.sym.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.gson.Gson;
import com.sym.canal.handler.ICanalMessageHandler;
import com.sym.canal.message.CanalMessage;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * @author shenyanming
 * @date 2020/7/26 11:53.
 */
@Slf4j
public class CanalClient implements Runnable {
    /**
     * 连接 canal server的配置信息
     */
    private static String CANAL_SERVER_HOST = "127.0.0.1";
    private static int CANAL_SERVER_PORT = 11111;
    private static String CANAL_SERVER_USERNAME = "";
    private static String CANAL_SERVER_PASSWORD = "";
    private static String DESTINATION = "example";


    /**
     * 从 canal server 拉取记录的最大条数
     */
    private static int MAX_BATCH_SIZE = 1000;

    /**
     * 运行状态
     */
    private final static int NONE = -1;
    private final static int RUNNING = 1 << 1;
    private final static int SUSPEND = 1 << 2;
    private final static int STOP = 1 << 3;

    /**
     * 原子操作类
     */
    private static AtomicIntegerFieldUpdater<CanalClient> STATUS_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(CanalClient.class, "status");

    /**
     * canal server连接器
     */
    private CanalConnector connector;

    /**
     * 工作线程
     */
    private Thread thread;

    /**
     * 消息处理
     */
    private ICanalMessageHandler messageHandler;

    /**
     * 表示 canal client 是否处于启动状态
     */
    volatile int status;


    /**
     * 表示订阅哪些信息
     */
    private String filter = ".*\\..*";

    public CanalClient() {
        this(list -> {
            Gson gson = new Gson();
            log.info("获取数据：{}", gson.toJson(list));
        });
    }

    public CanalClient(ICanalMessageHandler handler) {
        connector = CanalConnectors.newSingleConnector(new InetSocketAddress(CANAL_SERVER_HOST, CANAL_SERVER_PORT),
                DESTINATION, CANAL_SERVER_USERNAME, CANAL_SERVER_PASSWORD);
        this.messageHandler = Objects.requireNonNull(handler);
        this.status = NONE;
        this.thread = new Thread(this);
    }

    /**
     * 启动
     */
    public void start() {
        if (isRunning()) {
            log.info("canal client is already running");
            return;
        }
        if (STATUS_UPDATER.compareAndSet(this, NONE, RUNNING)) {
            log.info("canal client start..");
            thread.start();
        }
    }

    public void stop() {
        if (isStop()) {
            log.info("canal client is already stop");
            return;
        }
        if (STATUS_UPDATER.compareAndSet(this, RUNNING, STOP)) {
            log.info("canal client stop..");
            thread.interrupt();
        }
    }


    private void process(List<CanalEntry.Entry> entryList, long batchId) {
        List<CanalMessage> messageList = new ArrayList<>(entryList.size());
        CanalMessage canalMessage;
        try {
            for (CanalEntry.Entry entry : entryList) {
                // 只对行数据变化有兴趣
                if (entry.getEntryType() != CanalEntry.EntryType.ROWDATA) {
                    continue;
                }
                // 解析具体数据内容
                CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                CanalEntry.EventType eventType = rowChange.getEventType();
                // 获取表名
                String tableName = entry.getHeader().getTableName();
                // 组装数据
                canalMessage = new CanalMessage();
                canalMessage.setTableName(tableName);
                canalMessage.parseCanalEventType(eventType);
                // 针对增删改
                if (eventType != CanalEntry.EventType.INSERT && eventType != CanalEntry.EventType.UPDATE
                        && eventType != CanalEntry.EventType.DELETE) {
                    continue;
                }
                List<Map<String, Object>> rowDataBeforeList = new ArrayList<>();
                List<Map<String, Object>> rowDataAfterList = new ArrayList<>();
                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    if (eventType == CanalEntry.EventType.INSERT) {
                        // insert sql 只有修改后的数据
                        rowDataAfterList.add(convertRowData(rowData.getAfterColumnsList()));
                    } else if (eventType == CanalEntry.EventType.DELETE) {
                        // delete sql 只有修改前的数据
                        rowDataBeforeList.add(convertRowData(rowData.getBeforeColumnsList()));
                    } else {
                        // update sql, 既有修改前的数据, 也有修改后的数据
                        rowDataBeforeList.add(convertRowData(rowData.getBeforeColumnsList()));
                        rowDataAfterList.add(convertRowData(rowData.getAfterColumnsList()));
                    }
                }
                canalMessage.setRowDataBeforeList(rowDataBeforeList);
                canalMessage.setRowDataAfterList(rowDataAfterList);
                messageList.add(canalMessage);
            }
            // 处理解析好的数据
            if (!messageList.isEmpty()) {
                messageHandler.resolve(messageList);
            }
            // 提交这一批次的数据
            connector.ack(batchId);
        } catch (Exception e) {
            log.error("canal message process failure, ", e);
        }
    }

    private boolean isRunning() {
        return status == RUNNING;
    }

    private boolean isStop() {
        return status == STOP;
    }

    private Map<String, Object> convertRowData(List<CanalEntry.Column> columnList) {
        Map<String, Object> map = new HashMap<>(columnList.size());
        for (CanalEntry.Column column : columnList) {
            map.put(column.getName(), column.getValue());
        }
        return map;
    }

    @Override
    public void run() {
        connector.connect();
        connector.subscribe(filter);
        connector.rollback();
        try {
            while (isRunning() && !thread.isInterrupted()) {
                // 获取指定数量的数据
                Message message = connector.getWithoutAck(MAX_BATCH_SIZE);
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    // 没有任何数据
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                } else {
                    // 获取到数据
                    process(message.getEntries(), batchId);
                }
            }
        } finally {
            if(thread.isInterrupted()){
                log.info("thread is interrupt, program exit");
            }else{
                log.info("client is closed, program exit");
            }
            connector.disconnect();
        }
    }
}
