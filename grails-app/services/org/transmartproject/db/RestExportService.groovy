package org.transmartproject.db

import grails.transaction.Transactional
import org.springframework.beans.factory.annotation.Autowired
import org.transmartproject.export.Tasks.DataFetchTask
import org.transmartproject.export.Tasks.DataFetchTaskFactory

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

@Transactional
class RestExportService {

    @Autowired
    DataFetchTaskFactory dataFetchTaskFactory

    private final ZIP_FILE_NAME = "Data_Export.zip"

    List<File> export(arguments) {
        DataFetchTask task = dataFetchTaskFactory.createTask(arguments)
        task.getTsv()
    }


    File createZip(ArrayList<File> files) {
        //TODO: Add naming of files based on content of file, instead of a number.
        Path tmpPath = createTmpDir()
        def ant = new AntBuilder()
        int number = 1
        files.each { file ->
            String nameFile = number as String
            ant.copy(file: file,
                    tofile: "$tmpPath/$nameFile .txt")
            number++
        }
        ant.zip(destfile: "$tmpPath/$ZIP_FILE_NAME",
                basedir: tmpPath)
        File zipFile = new File("$tmpPath/$ZIP_FILE_NAME")
        zipFile
    }

    Path createTmpDir() {
        String uuid = UUID.randomUUID().toString()
        Path defaultTmp = Paths.get(System.getProperty("java.io.tmpdir"))
        try {
            def tmp_2 = Files.createTempDirectory(defaultTmp, uuid)
            return tmp_2

        } catch (IOException e) {
            "No Temporary directory created. Java.io.tmpdir not set correctly. $e"
        }
    }

    List parseFiles (files, outputFormats) {
        //TODO: Beautify the code, maybe change the location of this code to DataFetchTask?
        List headerNames = []
        List currentHeaders = []
        List outFiles = []
        List removableHeaders = []
        List usedHeaders = []
        Map fileInfo = new HashMap()
        files.each { file ->
            String fileContents = file.text
            boolean isBiomarker = false
            int lineNumber = 1
            fileContents.eachLine { line ->
                List lineList = line.split('\t')
                if (lineNumber == 1) {
                    lineList.each { rowHeader ->
                        currentHeaders.add(rowHeader)
                    }
                    isBiomarker = '"Bio marker"' in currentHeaders ? true : isBiomarker
                } else if (isBiomarker){
                        file in outFiles ? null : outFiles.add(file)
                        currentHeaders.clear()
                } else if (!(isBiomarker) && lineNumber > 1) {
                        String columnName = lineList[0]
                        if (!fileInfo.containsKey(columnName)) {
                            fileInfo[columnName] = [:]
                        }
                        lineList.each { infoPiece ->
                            if (infoPiece != "") {
                                int index = lineList.indexOf(infoPiece)
                                String currentRowHeader = currentHeaders[index]
                                usedHeaders.add(currentRowHeader)
                                try{
                                    def categoricalHeader = currentRowHeader.tokenize("\\")
                                    def categoricalValue = '"'+categoricalHeader[-2]+'"'
                                    if (infoPiece == categoricalValue){
                                        if (!(categoricalHeader[-3] in currentHeaders)) {
                                            currentHeaders[index] = categoricalHeader[-3]
                                        }
                                        removableHeaders.add(currentRowHeader)
                                        currentRowHeader = categoricalHeader[-3]
                                    }
                                } catch (Exception e){

                                }
                                def currentInfoMap = fileInfo.get(columnName)
                                if (currentRowHeader in currentInfoMap) {
                                    try {
                                        assert fileInfo[columnName][currentRowHeader] == infoPiece
                                    } catch(AssertionError e) {
                                        log.error(e)
                                    }
                                } else {
                                    fileInfo[columnName][currentRowHeader] = infoPiece
                                }
                            }
                        }
                    }
                lineNumber++
            }
            removableHeaders += currentHeaders-usedHeaders
            currentHeaders.removeAll(removableHeaders)
            currentHeaders.each { header ->
                if (!headerNames.contains(header)) {
                    headerNames[(headerNames.size())] = header
                }
            }
            currentHeaders = []
        }
        outputFormats.each { outputFormat ->
            switch (outputFormat) {
                case 'tsv':
                    def file = WriteToTsv(headerNames, fileInfo)
                    outFiles.add(file)
            }
        }
        outFiles
    }


    def WriteToTsv(List<String> headerNames, HashMap fileInfo){
        String uuid = UUID.randomUUID().toString()
        File outFile = new File(System.getProperty("java.io.tmpdir") + uuid + '.txt')
        headerNames.each { header ->
            outFile << header+'\t'
        }
        ArrayList headerNamesList = headerNames.toArray()
        outFile << '\n'
        fileInfo.each { key, value ->
            def outList = createTabList(headerNamesList)
            def infoMap = value
            infoMap.each {keyInfo, valueInfo ->
                int headerIndex = headerNamesList.findIndexOf {it == keyInfo}
                outList[headerIndex] = valueInfo
            }
            outList.each { finalInfo ->
                if ('\t' in finalInfo) {
                    outFile << finalInfo
                } else{
                    outFile << finalInfo+'\t'
                }
            }
            outFile << '\n'
            outList.clear()
        }
        print(fileInfo)
        outFile
    }

    List createTabList(headerList){
        def outList = new ArrayList<>(headerList.size())
        headerList.each {
            outList.add('\t')
        }
        return outList
    }
}
