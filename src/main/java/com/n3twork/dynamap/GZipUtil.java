package com.n3twork.dynamap;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GZipUtil {

    private static final Charset UTF8 = StandardCharsets.UTF_8;

    public static byte[] serialize(Object data, ObjectMapper objectMapper) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); GZIPOutputStream os = new GZIPOutputStream(baos)) {
            String json = objectMapper.writeValueAsString(data);
            os.write(json.getBytes(UTF8));
            os.finish();
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> Object deSerialize(byte[] data, ObjectMapper objectMapper, Class<T> resultClass) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data); GZIPInputStream is = new GZIPInputStream(bais)) {
            byte[] json = IOUtils.toByteArray(is);
            return objectMapper.readValue(new String(json, UTF8), resultClass);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
