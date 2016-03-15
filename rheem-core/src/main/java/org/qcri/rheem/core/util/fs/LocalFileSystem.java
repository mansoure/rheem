package org.qcri.rheem.core.util.fs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * {@link FileSystem} implementation for the local file system.
 */
public class LocalFileSystem implements FileSystem {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private static File toFile(String fileUrl) throws URISyntaxException, MalformedURLException {
        return new File(new URL(fileUrl).toURI());
    }

    @Override
    public long getFileSize(String fileUrl) throws FileNotFoundException {
        try {
            File file = toFile(fileUrl);
            return file.length();
        } catch (MalformedURLException | URISyntaxException e) {
            this.logger.error("Illegal URL: \"{}\"", fileUrl);
            throw new FileNotFoundException("File not found, because the URL is not correct.");
        }
    }

    @Override
    public boolean canHandle(String urlAsString) {
        try {
            URL url = new URL(urlAsString);
            return url.getProtocol().equals("file") &&
                    (url.getHost().equals("") || url.getHost().equals("localhost"));
        } catch (MalformedURLException e) {
            this.logger.error("Illegal URL: \"{}\"", urlAsString);
            return false;
        }
    }

    @Override
    public InputStream open(String url) throws IOException {
        try {
            File file = toFile(url);
            return new FileInputStream(file);
        } catch (URISyntaxException e) {
            throw new IOException("Could not process the given URL.", e);
        }
    }

    @Override
    public OutputStream create(String url) throws IOException {
        File file = null;
        try {
            file = toFile(url);
            return new FileOutputStream(file, false);
        } catch (URISyntaxException e) {
            throw new IOException("Could not process the given URL.", e);
        } catch (IOException e) {
            if (file != null) {
                file.delete();
            }
            throw e;
        }
    }

    @Override
    public boolean isDirectory(String url) {
        try {
            return toFile(url).isDirectory();
        } catch (URISyntaxException | MalformedURLException e) {
            this.logger.warn("Could not inspect directory.", e);
            return false;
        }
    }

    @Override
    public Collection<String> listChildren(String url) {
        try {
            final File[] files = toFile(url).listFiles();
            if (files == null) {
                return Collections.emptyList();
            }
            return Arrays.stream(files).map(File::toURI).map(Object::toString).collect(Collectors.toList());
        } catch (URISyntaxException | MalformedURLException e) {
            this.logger.warn("Could not inspect directory.", e);
            return Collections.emptyList();
        }
    }
}