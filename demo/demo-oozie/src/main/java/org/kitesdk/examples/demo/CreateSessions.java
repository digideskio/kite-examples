/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.examples.demo;

import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetRepositories;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.PartitionKey;
import org.kitesdk.data.crunch.CrunchDatasets;
import org.kitesdk.data.event.StandardEvent;
import org.kitesdk.data.filesystem.FileSystemDatasetRepository;
import org.kitesdk.examples.demo.event.Session;
import com.google.common.collect.Iterables;
import java.io.Serializable;
import java.net.URI;
import org.apache.crunch.CombineFn;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.Target;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.util.CrunchTool;
import org.apache.hadoop.util.ToolRunner;

public class CreateSessions extends CrunchTool implements Serializable {

  @Override
  public int run(String[] args) throws Exception {

    // Construct an HCatalog dataset repository using external Hive tables
    DatasetRepository repo = DatasetRepositories.open("repo:hive:/tmp/data");

    // Turn debug on while in development.
    getPipeline().enableDebug();
    getPipeline().getConfiguration().set("crunch.log.job.progress", "true");

    // Load the events dataset and get the correct partition to sessionize
    Dataset<StandardEvent> eventsDataset = repo.load("events");
    Dataset<StandardEvent> partition;
    if (args.length == 0 || (args.length == 1 && args[0].equals("LATEST"))) {
      partition = getLatestPartition(eventsDataset);
    } else {
      partition = getPartitionForURI(eventsDataset, args[0]);
    }

    // Create a parallel collection from the working partition
    PCollection<StandardEvent> events = read(
        CrunchDatasets.asSource(partition, StandardEvent.class));

    // Process the events into sessions, using a combiner
    PCollection<Session> sessions = events
      .parallelDo(new DoFn<StandardEvent, Session>() {
        @Override
        public void process(StandardEvent event, Emitter<Session> emitter) {
          emitter.emit(Session.newBuilder()
              .setUserId(event.getUserId())
              .setSessionId(event.getSessionId())
              .setIp(event.getIp())
              .setStartTimestamp(event.getTimestamp())
              .setDuration(0)
              .setSessionEventCount(1)
              .build());
        }
      }, Avros.specifics(Session.class))
      .by(new MapFn<Session, Pair<Long, String>>() {
        @Override
        public Pair<Long, String> map(Session session) {
          return Pair.of(session.getUserId(), session.getSessionId());
        }
      }, Avros.pairs(Avros.longs(), Avros.strings()))
      .groupByKey()
      .combineValues(new CombineFn<Pair<Long, String>, Session>() {
        @Override
        public void process(Pair<Pair<Long, String>, Iterable<Session>> pairIterable,
            Emitter<Pair<Pair<Long, String>, Session>> emitter) {
          String ip = null;
          long startTimestamp = Long.MAX_VALUE;
          long endTimestamp = Long.MIN_VALUE;
          int sessionEventCount = 0;
          for (Session s : pairIterable.second()) {
            ip = s.getIp();
            startTimestamp = Math.min(startTimestamp, s.getStartTimestamp());
            endTimestamp = Math.max(endTimestamp, s.getStartTimestamp() + s.getDuration());
            sessionEventCount += s.getSessionEventCount();
          }
          emitter.emit(Pair.of(pairIterable.first(), Session.newBuilder()
              .setUserId(pairIterable.first().first())
              .setSessionId(pairIterable.first().second())
              .setIp(ip)
              .setStartTimestamp(startTimestamp)
              .setDuration(endTimestamp - startTimestamp)
              .setSessionEventCount(sessionEventCount)
              .build()));
        }
      })
      .parallelDo(new DoFn<Pair<Pair<Long, String>, Session>, Session>() {
        @Override
        public void process(Pair<Pair<Long, String>, Session> pairSession,
            Emitter<Session> emitter) {
          emitter.emit(pairSession.second());
        }
      }, Avros.specifics(Session.class));

    // Write the sessions to the "sessions" Dataset
    getPipeline().write(sessions, CrunchDatasets.asTarget(repo.load("sessions")),
        Target.WriteMode.APPEND);

    return run().succeeded() ? 0 : 1;
  }

  private <E> Dataset<E> getLatestPartition(Dataset<E> eventsDataset) {
    Dataset<E> ds = eventsDataset;
    while (ds.getDescriptor().isPartitioned()) {
      ds = Iterables.getLast(ds.getPartitions());
    }
    return ds;
  }

  private <E> Dataset<E> getPartitionForURI(Dataset<E> eventsDataset, String uri) {
    PartitionKey partitionKey = FileSystemDatasetRepository.partitionKeyForPath(
        eventsDataset, URI.create(uri));
    Dataset<E> partition = eventsDataset.getPartition(partitionKey, false);
    if (partition == null) {
      throw new IllegalArgumentException("Partition not found: " + uri);
    }
    return partition;
  }

  public static void main(String... args) throws Exception {
    int rc = ToolRunner.run(new CreateSessions(), args);
    System.exit(rc);
  }

}
