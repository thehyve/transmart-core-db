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

    List<File> export(arguments) {
        DataFetchTask task = dataFetchTaskFactory.createTask(arguments)
        task.getTsv()
    }


    File createZip(ArrayList<File> files) {
        Path tmpPath = createTmpDir()
        def ant = new AntBuilder()
        int number = 1
        files.each { file ->
            String nameFile = number as String
            ant.copy(file: file,
                    tofile: "$tmpPath/$nameFile .txt")
            number++
        }
        ant.zip(destfile: "$tmpPath/file.zip",
                basedir: tmpPath)
        File zipFile = new File("$tmpPath/file.zip")
        zipFile
    }

    Path createTmpDir() {
        String uuid = UUID.randomUUID().toString()
        Path defaultTmp = Paths.get(System.getProperty("java.io.tmpdir"))

        try {
            def tmp_2 = Files.createTempDirectory(defaultTmp, uuid)
            return tmp_2

        } catch (IOException e) {
            System.err.println(e);
        }
    }

    File parseFiles (files) {
        String uuid = UUID.randomUUID().toString()
        List headerNames = []
        List currentHeaders = []
        HashMap fileInfo = new HashMap()
        File outFile = new File(System.getProperty("java.io.tmpdir")+uuid+'.txt')
        files.each { file ->
            String fileContents = file.text
            int lineNumber = 1
            fileContents.eachLine { line ->
                List lineList = line.split('\t')
                if (lineNumber == 1) {
                    lineList.each { rowHeader ->
                        currentHeaders.add(rowHeader)
                    }
                } else {
                    String columnName = lineList[0]
                    if (!fileInfo.containsKey(columnName)) {
                        fileInfo[columnName] = [:]
                    }
                    lineList.each { infoPiece ->
                        if (infoPiece != "") {
                            int index = lineList.indexOf(infoPiece)
                            String currentRowHeader = currentHeaders[index].toString()
                            def currentInfoMap = fileInfo.get(columnName)
                            if (currentRowHeader in currentInfoMap) {
                                assert fileInfo[columnName][currentRowHeader] == infoPiece
                            } else {
                                fileInfo[columnName][currentRowHeader] = infoPiece
                            }
                        }
                }
                }
                lineNumber++
            }
            currentHeaders.each { header ->
                if (!headerNames.contains(header)) {
                    headerNames[(headerNames.size())] = header
                }
            }
            currentHeaders = []
        }
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
