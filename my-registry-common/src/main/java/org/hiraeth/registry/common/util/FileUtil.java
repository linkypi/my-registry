package org.hiraeth.registry.common.util;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 12:27
 */
@Slf4j
public class FileUtil {
    public static boolean persist(String dataDir, String fileName, byte[] bytes) {
        try {
            File dataFile = new File(dataDir);
            if (!dataFile.exists()) {
                dataFile.mkdirs();
            }
            // write check sum
            Checksum checksum = new Adler32();
            checksum.update(bytes, 0, bytes.length);
            long crc32 = checksum.getValue();


            File file = new File(dataDir, fileName);
            try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
                try (BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream)) {
                    try (DataOutputStream dataOutputStream = new DataOutputStream(bufferedOutputStream)) {

                        // write check sum
                        dataOutputStream.writeLong(crc32);

                        // write length
                        dataOutputStream.writeInt(bytes.length);

                        // write bytes
                        dataOutputStream.write(bytes);

                        // 多次flush 保证数据落地到磁盘
                        // bufferedOutputStream 的flush仅仅保证数据落入 fileOutputStream
                        bufferedOutputStream.flush();
                        // 对fileOutputStream执行 flush 保证数据进入 os cache
                        fileOutputStream.flush();
                        // 强制刷盘
                        fileOutputStream.getChannel().force(false);
                    }
                }
            }
            log.info("persist success: {}.", dataDir + "/" + fileName);
            return true;
        } catch (Exception ex) {
            log.error("persist occur error, data dir: {}, file name: {}.", dataDir, fileName, ex);
        }
        return false;
    }
}
