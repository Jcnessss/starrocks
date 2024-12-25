// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.hive;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.common.DdlException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.starrocks.connector.hive.HiveMetastoreOperations.EXTERNAL_LOCATION_PROPERTY;
import static java.lang.String.join;
import static java.util.Locale.ENGLISH;

public class HiveWriteUtils {
    private static final Logger LOG = LogManager.getLogger(HiveWriteUtils.class);
    public static boolean isS3Url(String prefix) {
        return prefix.startsWith("oss://") || prefix.startsWith("s3n://") || prefix.startsWith("s3a://") ||
                prefix.startsWith("s3://") || prefix.startsWith("cos://") || prefix.startsWith("cosn://") ||
                prefix.startsWith("obs://") || prefix.startsWith("ks3://") || prefix.startsWith("tos://");
    }

    public static void checkLocationProperties(Map<String, String> properties) throws DdlException {
        if (properties.containsKey(EXTERNAL_LOCATION_PROPERTY)) {
            throw new DdlException("Can't create non-managed Hive table. " +
                    "Only supports creating hive table under Database location. " +
                    "You could execute command without external_location properties");
        }
    }

    public static void checkExternalLocationProperties(Map<String, String> properties) throws DdlException {
        if (!properties.containsKey(EXTERNAL_LOCATION_PROPERTY)) {
            throw new DdlException("Can't create external Hive table without external_location property.");
        }
    }

    public static boolean pathExists(Path path, Configuration conf) {
        try {
            FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
            return fileSystem.exists(path);
        } catch (Exception e) {
            LOG.error("Failed to check path {}", path, e);
            throw new StarRocksConnectorException("Failed to check path: " + path + ". msg: " + e.getMessage());
        }
    }

    public static boolean isDirectory(Path path, Configuration conf) {
        try {
            FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
            return fileSystem.getFileStatus(path).isDirectory();
        } catch (IOException e) {
            LOG.error("Failed checking path {}", path, e);
            throw new StarRocksConnectorException("Failed checking path: " + path);
        }
    }

    public static boolean isEmpty(Path path, Configuration conf) {
        try {
            FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
            return !fileSystem.listFiles(path, false).hasNext();
        } catch (IOException e) {
            LOG.error("Failed checking path {}", path, e);
            throw new StarRocksConnectorException("Failed checking path: " + path);
        }
    }

    public static void createDirectory(Path path, Configuration conf) {
        try {
            FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
            if (!fileSystem.mkdirs(path)) {
                LOG.error("Mkdir {} returned false", path);
                throw new IOException("mkdirs returned false");
            }
        } catch (IOException e) {
            LOG.error("Failed to create directory: {}", path);
            throw new StarRocksConnectorException("Failed to create directory: " + path, e);
        }
    }

    public static String getStagingDir(HiveTable table, String tempStagingDir) {
        String stagingDir;
        String location = table.getTableLocation();
        if (isS3Url(location)) {
            stagingDir = location;
        } else {
            Path tempRoot = new Path(location, tempStagingDir);
            Path tempStagingPath = new Path(tempRoot, UUID.randomUUID().toString());
            stagingDir = tempStagingPath.toString();
        }
        return stagingDir.endsWith("/") ? stagingDir : stagingDir + "/";
    }

    public static boolean fileCreatedByQuery(String fileName, String queryId) {
        Preconditions.checkState(queryId.length() > 8, "file name or query id is invalid");
        if (fileName.length() <= queryId.length()) {
            // file is created by other engine like hive
            return false;
        }
        String checkQueryId = queryId.substring(0, queryId.length() - 8);
        return fileName.startsWith(checkQueryId) || fileName.endsWith(checkQueryId);
    }

    public static void checkedDelete(FileSystem fileSystem, Path file, boolean recursive) throws IOException {
        try {
            if (!fileSystem.delete(file, recursive)) {
                if (fileSystem.exists(file)) {
                    throw new IOException("Failed to delete " + file);
                }
            }
        } catch (FileNotFoundException ignored) {
            // ignore
        }
    }

    public static boolean deleteIfExists(Path path, boolean recursive, Configuration conf) {
        try {
            FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
            if (fileSystem.delete(path, recursive)) {
                return true;
            }

            return !fileSystem.exists(path);
        } catch (FileNotFoundException ignored) {
            return true;
        } catch (IOException ignored) {
            LOG.error("Failed to delete remote path {}", path);
        }

        return false;
    }

    /**/
    public static Set<Location> listDirectories(Location directory, Configuration conf) throws IOException {
        Path path = new Path(directory.toString());
        try {
            FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
            FileStatus[] files = fileSystem.listStatus(path);
            if (files.length == 0) {
                return ImmutableSet.of();
            }
            if (files[0].getPath().equals(directory)) {
                throw new IOException("Location is a file, not a directory: " + path);
            }
            return Stream.of(files)
                    .filter(FileStatus::isDirectory)
                    .map(file -> listedLocation(directory, path, file.getPath()))
                    .map(file -> file.appendSuffix("/"))
                    .collect(toImmutableSet());
        } catch (FileNotFoundException exception) {
            return ImmutableSet.of();
        } catch (IOException ioException) {
            throw new IOException(String.format("List directories for %s failed: %s", path, ioException.getMessage()),
                    ioException);
        }
    }

    private static Location listedLocation(Location listingLocation, Path listingPath, Path listedPath) {
        String root = listingPath.toUri().getPath();
        String path = listedPath.toUri().getPath();

        verify(path.startsWith(root), "iterator path [%s] not a child of listing path [%s] " +
                "for location [%s]", path, root, listingLocation);

        int index = root.endsWith("/") ? root.length() : root.length() + 1;
        return listingLocation.appendPath(path.substring(index));
    }

    private static String listedDirectoryName(Location directory, Location location) {
        String prefix = directory.path();
        if (!prefix.endsWith("/")) {
            prefix += "/";
        }
        String path = location.path();
        verify(path.endsWith("/"), "path does not end with slash: %s", location);
        verify(path.startsWith(prefix), "path [%s] is not a child of directory [%s]", location, directory);
        return path.substring(prefix.length(), path.length() - 1);
    }

    public static Set<String> listPartitions(Location directory, List<Column> partitionColumns, Configuration conf)
            throws IOException {
        return doListPartitions(directory, partitionColumns, partitionColumns.size(), ImmutableList.of(), conf);
    }

    private static Set<String> doListPartitions(Location directory, List<Column> partitionColumns, int depth,
                                                List<String> partitions, Configuration conf) throws IOException {
        if (depth == 0) {
            return ImmutableSet.of(join("/", partitions));
        }

        ImmutableSet.Builder<String> result = ImmutableSet.builder();
        for (Location location : listDirectories(directory, conf)) {
            String path = listedDirectoryName(directory, location);
            Column column = partitionColumns.get(partitionColumns.size() - depth);
            if (!isValidPartitionPath(path, column, true)) {
                continue;
            }
            List<String> current = ImmutableList.<String>builder().addAll(partitions).add(path).build();
            result.addAll(doListPartitions(location, partitionColumns, depth - 1, current, conf));
        }
        return result.build();
    }

    private static boolean isValidPartitionPath(String path, Column column, boolean caseSensitive) {
        if (!caseSensitive) {
            path = path.toLowerCase(ENGLISH);
        }
        return path.startsWith(column.getName() + '=');
    }

    public static Optional<String> relativeLocation(Location tableLocation, Location partLocation) {
        String prefix = tableLocation.path();
        if (!prefix.endsWith("/")) {
            prefix += "/";
        }
        String path = stripTrailingSlash(partLocation.path());
        if (path.startsWith(prefix)) {
            return Optional.of(path.substring(prefix.length()));
        }
        return Optional.empty();
    }

    private static String stripTrailingSlash(String path) {
        String result = path;
        while (result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }

}
