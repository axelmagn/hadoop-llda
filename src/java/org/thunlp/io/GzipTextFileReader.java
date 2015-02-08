package org.thunlp.io;

import java.io.*;
import java.util.zip.GZIPInputStream;

public class GzipTextFileReader extends TextFileReader {
    public GzipTextFileReader(File file) throws IOException {
        super(file);
    }

    public GzipTextFileReader(String file) throws IOException {
        super(file);
    }

    public GzipTextFileReader(String file, String charset) throws IOException {
        super(file, charset);
    }

    public GzipTextFileReader(File file, String charset) throws IOException {
        super(file, charset);
    }

    @Override
    public BufferedReader constructReader(File file, String encode)
            throws IOException {
        return new BufferedReader(
                new InputStreamReader(
                        new GZIPInputStream(new FileInputStream(file)), encode));
    }
}
