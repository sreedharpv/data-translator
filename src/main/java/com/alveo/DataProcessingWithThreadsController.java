package com.alveo;

import java.io.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataProcessingWithThreadsController {

    private static final String VENDOR_DATA_FILE = "src/main/resources/vendor-data.txt";
    private static final String COLUMN_CONFIG_DATA_FILE = "src/main/resources/column-config-data.txt";
    private static final String ROW_VENDOR_DATA_FILE = "src/main/resources/id-config-data.txt";
    private static final String TRANSLATOR_DATA_FILE = "src/main/resources/translator-data.txt";

    private BlockingQueue<String> dataQueue = new ArrayBlockingQueue<>(10);
    private Map<String, String> rowDataMap = new LinkedHashMap<>();
    private String[] userColumns;
    private static final String SPLIT_CHAR = "\\s+";

    public static void main(String[] args) throws IOException {
        DataProcessingWithThreadsController dataProcessingController = new DataProcessingWithThreadsController();
        dataProcessingController.processAndTranslateData();
    }
    public boolean processAndTranslateData() throws IOException {

        //Read column config file
        Map<String, String> columnDataMap = new LinkedHashMap<>();
        BufferedReader brColumnData = readFileData(COLUMN_CONFIG_DATA_FILE);
        columnDataMap = buildDataMap(brColumnData);
        brColumnData.close();

        //Read row id config file
        BufferedReader brRowIdData = readFileData(ROW_VENDOR_DATA_FILE);
        rowDataMap = buildDataMap(brRowIdData);
        brRowIdData.close();

        FileWriter fileWriter;
        try {
            fileWriter = new FileWriter(TRANSLATOR_DATA_FILE);
        } catch (IOException e1) {
            e1.printStackTrace();
            return false;
        }

        //Read vendor data file
        BufferedReader brVendorData = readFileData(VENDOR_DATA_FILE);
        if(brVendorData != null ) {
            try {
                this.userColumns = brVendorData.readLine().split(SPLIT_CHAR);

                String header = columnDataMap.get(userColumns[0]);
                for (int i = 1; i < userColumns.length; i++) {
                    userColumns[i] = columnDataMap.get(userColumns[i]);
                    if (userColumns[i] != null) {
                        header += "\t" + userColumns[i];
                    }
                }
                fileWriter.write(header);

                //write the data
                Runnable producer = () -> {
                    try {
                        producer(brVendorData);
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                };
                Thread t1 = new Thread(producer);
                t1.start();

                ExecutorService executor = Executors.newFixedThreadPool(2);
                Runnable consumer = () -> {
                    try {
                        consumer(fileWriter);
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                };
                for (int i = 0; i < 5; i++) {
                    executor.execute(consumer);
                }
                executor.shutdown();
                while (!executor.isTerminated()) {

                }
                executor.shutdown();
                fileWriter.flush();
            }catch (IOException e) {
                e.printStackTrace();
                try {
                    fileWriter.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
                return false;
            }
        }
        brVendorData.close();
        fileWriter.close();
        return true;
    }

    private Map<String, String> buildDataMap(BufferedReader brData) throws IOException {
        String currentLine;
        Map<String, String> rowDataMap = new LinkedHashMap<>();
        try {
            while ((currentLine = brData.readLine()) != null) {
                String[] rowData = currentLine.split(SPLIT_CHAR);
                rowDataMap.put(rowData[0], rowData[1]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return rowDataMap;
    }

    private BufferedReader readFileData(String fileName) {
        BufferedReader bufferedReader = null;
        FileReader fileReader = null;

        try {
            fileReader = new FileReader(fileName);
            bufferedReader = new BufferedReader(fileReader);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            bufferedReader = null;
        }

        return bufferedReader;
    }

    private void producer(BufferedReader bufferedReader) throws IOException, InterruptedException {
        String currentLine;
        while ((currentLine = bufferedReader.readLine()) != null) {
            dataQueue.put(currentLine);

        }
        bufferedReader.close();
    }

    private void consumer(FileWriter fw) throws InterruptedException, IOException {
        String separator = System.getProperty("line.separator");
        while (dataQueue.size() > 0) {
            String[] data = dataQueue.take().split(SPLIT_CHAR);
            if (rowDataMap.get(data[0]) != null) {
                String values = rowDataMap.get(data[0]);
                for (int i = 1; i < data.length; i++) {
                    if (userColumns[i] != null) {
                        values += "\t" + data[i];
                    }
                }
                fw.write(separator + values);
            }
        }
    }
}
