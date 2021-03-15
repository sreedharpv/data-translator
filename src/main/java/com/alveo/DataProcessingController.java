package com.alveo;

import java.io.*;
import java.util.LinkedHashMap;
import java.util.Map;

public class DataProcessingController {

    private static final String VENDOR_DATA_FILE = "vendor-data.txt";
    private static final String COLUMN_CONFIG_DATA_FILE = "column-config-data.txt";
    private static final String ROW_VENDOR_DATA_FILE = "id-config-data.txt";
    private static final String TRANSLATOR_DATA_FILE = "translator-data.txt";
    private static final String SPLIT_CHAR = "\\s+";

    private Map<String, String> rowDataMap = new LinkedHashMap<>();
    private String[] userColumns;

    public static void main(String[] args) throws IOException {
        DataProcessingController dataProcessingController = new DataProcessingController();
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
        String separator = System.getProperty( "line.separator" );
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
                String currentLine;
                while ((currentLine = brVendorData.readLine()) != null) {
                    String[] data = currentLine.split(SPLIT_CHAR);
                    if (rowDataMap.get(data[0]) != null) {
                        String values = rowDataMap.get(data[0]);
                        for (int i = 1; i < data.length; i++) {
                            if (userColumns[i] != null) {
                                values += "\t" + data[i];
                            }

                        }
                        fileWriter.write(separator + values);
                    }

                }
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
}
