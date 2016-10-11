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
}
